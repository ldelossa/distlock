package distlock

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"time"

	"github.com/jackc/pgx/v4"
)

const (
	trySessionLock   = `SELECT pg_try_advisory_lock($1);`
	trySessionUnlock = `SELECT pg_advisory_unlock($1)`
)

func keyify(key string) int64 {
	h := fnv.New64a()
	io.WriteString(h, key)
	return int64(h.Sum64())
}

type RequestType int

const (
	Invalid RequestType = iota
	Lock
	Unlock
	// this forces a close of the underlying conn.
	// this should only be used for testing.
	Close
)

var ErrMutualExclusion = fmt.Errorf("another process is holding the requested lock")

// response is an internal response for a
// lock request
type response struct {
	ok  bool
	ctx context.Context
	err error
}

// request is an internal request for a lock
type request struct {
	t        RequestType
	key      string
	respChan chan response
}

type Manager struct {
	root    context.Context
	cancel  context.CancelFunc
	m       map[string]context.CancelFunc
	conn    *pgx.Conn
	reqChan chan request
}

func NewManager(ctx context.Context, conn *pgx.Conn) *Manager {
	reqChan := make(chan request, 1024)
	m := make(map[string]context.CancelFunc)
	root, cancel := context.WithCancel(ctx)

	mgr := &Manager{
		root:    root,
		cancel:  cancel,
		m:       m,
		conn:    conn,
		reqChan: reqChan,
	}

	go mgr.Guard(ctx)
	return mgr
}

// Guard provides a channel-based memory guard
// that serializes access to internal data structures
// not concurrency safe.
//
// this guard performs database health checks on an interval.
// if database becomes unavailable all lock ctx(s) are canceled.
//
// if the original ctx controlling the manager's lifetime is canceled
// the manager sets reqChan to nil and is no longer useable.
func (m *Manager) Guard(ctx context.Context) {
	t := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-t.C:
			m.checkDB(ctx)
		case req := <-m.reqChan:
			switch req.t {
			case Invalid:
				continue
			case Lock:
				resp := m.lock(ctx, req.key)
				req.respChan <- resp
			case Unlock:
				resp := m.unlock(ctx, req.key)
				req.respChan <- resp
			case Close:
				var resp response
				resp.err = m.conn.Close(ctx)
				t.Stop()
				m.cancel()
				m.reqChan = nil
			}
		case <-ctx.Done():
			log.Printf("manager's ctx canceled. canceling all locks")
			for key, f := range m.m {
				log.Printf("canceling lock for key %s", key)
				f()
			}
			t.Stop()
			m.cancel()
			m.reqChan = nil
			return
		}
	}
}

// TryLock will block on acquiring a lock until either success or the provided ctx
// is canceled.
func (m *Manager) TryLock(ctx context.Context, key string) (context.Context, error) {
	for {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("provided ctx has been canceled")
		}

		c := make(chan response)
		req := request{
			t:        Lock,
			key:      key,
			respChan: c,
		}
		select {
		case m.reqChan <- req:
		default:
			return nil, fmt.Errorf("manager could not take request. construct a new one")
		}

		// block until response or ctx canceled
		var resp response
		select {
		case resp = <-c:
		case <-m.root.Done():
			return nil, fmt.Errorf("manager's context canceled while waiting for lock")
		}

		// lock acquired
		if resp.ok {
			return resp.ctx, nil
		}

		// if ErrMutualExclusion just retry
		if resp.err == ErrMutualExclusion {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return nil, resp.err
	}
}

func (m *Manager) Lock(ctx context.Context, key string) (context.Context, error) {
	c := make(chan response)
	req := request{
		t:        Lock,
		key:      key,
		respChan: c,
	}
	select {
	case m.reqChan <- req:
	default:
		return nil, fmt.Errorf("manager could not take request. construct a new one")
	}

	// block until response
	var resp response
	select {
	case <-m.root.Done():
		return nil, fmt.Errorf("manager's ctx canceled while waiting for lock")
	case resp = <-c:
	}

	if !resp.ok {
		return nil, resp.err
	}
	return resp.ctx, nil
}

func (m *Manager) Unlock(ctx context.Context, key string) error {
	c := make(chan response)
	req := request{
		t:        Unlock,
		key:      key,
		respChan: c,
	}
	select {
	case m.reqChan <- req:
	default:
		return fmt.Errorf("manager could not take request. construct a new one")
	}

	// block until response
	var resp response
	select {
	case <-m.root.Done():
		return fmt.Errorf("manager's ctx canceled while waiting for lock")
	case resp = <-c:
	}

	if !resp.ok {
		return resp.err
	}
	return nil
}

func (m *Manager) close(ctx context.Context) error {
	c := make(chan response)
	req := request{
		t:        Close,
		respChan: c,
	}
	select {
	case m.reqChan <- req:
	default:
		return fmt.Errorf("manager could not take request. construct a new one")
	}

	// block until response
	var resp response
	select {
	case <-m.root.Done():
		return fmt.Errorf("manager's ctx canceled while waiting for lock")
	case resp = <-c:
	}
	return resp.err
}

func (m *Manager) lock(ctx context.Context, key string) response {
	if _, ok := m.m[key]; ok {
		return response{false, nil, ErrMutualExclusion}
	}

	var ok bool
	row := m.conn.QueryRow(ctx, trySessionLock, keyify(key))
	err := row.Scan(&ok)
	if err != nil {
		return response{false, nil, err}
	}
	if !ok {
		return response{false, nil, ErrMutualExclusion}
	}

	// dervice ctx from root
	ctx, cancel := context.WithCancel(m.root)
	m.m[key] = cancel
	return response{true, ctx, nil}
}

func (m *Manager) unlock(ctx context.Context, key string) response {
	var f context.CancelFunc
	var ok bool
	if f, ok = m.m[key]; !ok {
		return response{false, nil, fmt.Errorf("no existing lock for key %s found", key)}
	}

	row := m.conn.QueryRow(ctx, trySessionUnlock, keyify(key))
	err := row.Scan(&ok)
	if err != nil {
		return response{false, nil, err}
	}
	if !ok {
		return response{false, nil, fmt.Errorf("unlock of key %s returned false", key)}
	}

	f()
	delete(m.m, key)
	return response{true, nil, nil}
}

func (m *Manager) checkDB(ctx context.Context) {
	if err := m.conn.Ping(ctx); err != nil {
		for key, f := range m.m {
			log.Printf("database unhealthy: %v. canceling lock for %s", err, key)
			f()
			delete(m.m, key)
		}
	}
}
