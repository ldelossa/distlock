package distlock

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
)

const (
	trySessionLock   = `SELECT pg_try_advisory_lock($1);`
	trySessionUnlock = `SELECT pg_advisory_unlock($1)`
)

// Signifies mutual exclusion could not be obtained. In other words another
// process owns a requested lock.
var ErrMutualExclusion = fmt.Errorf("another process is holding the requested lock")

// Signifies the dabatase is unavialable. Locks cannot be provided. Unlock
// requests can simply ignore this, their locks are gone.
var ErrDatabaseUnavailable = fmt.Errorf("database currently unavailable.")

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
)

// request is an internal request for a lock
type request struct {
	t        RequestType
	key      string
	respChan chan response
}

// response is an internal response for a
// lock request
type response struct {
	ok  bool
	ctx context.Context
	err error
}

// guard provides a concurrency and deadlock safe api for requesting
// distributed locks.
//
// guard should only be created via a Manager's constructor.
type guard struct {
	root    context.Context
	cancel  context.CancelFunc
	chanMu  sync.Mutex
	reqChan chan request
	dsn     string
	m       map[string]context.CancelFunc
	conn    *pgx.Conn
	// atomic bool. true if the guard is not connected to the db.
	online atomic.Value
	// atomic bool. true if reconnection loop is running
	reconnecting atomic.Value
}

// ioLoop is a serialized event loop.
// the loop provides mutually exclusive access to m.conn and m.m
//
// in the event a reconnect loop is running in the background this
// loop is guarenteed not to access m.conn and m.m.
func (m *guard) ioLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// only ping if we are online.
			if m.online.Load().(bool) {
				if err := m.conn.Ping(ctx); err != nil {
					m.reconnect(ctx)
				}
			}
		case req := <-m.reqChan:
			m.handleRequest(req, ctx)
		case <-ctx.Done():
			m.quit()
			return
		}
	}
}

// reconnect occurs when the database connection is lost.
//
// reconnect is guarenteed to have exclusive access to m.conn and m.m
//
// the ioLoop will continue processing requests, returning errors, while
// the database is not available.
func (m *guard) reconnect(ctx context.Context) {
	log.Printf("manager detected database disconnect... entering reconnection loop")

	m.online.Store(false)
	m.reconnecting.Store(true)

	for key, f := range m.m {
		log.Printf("database disconnected. canceling lock for %s", key)
		f()
	}

	go func(ctx context.Context) {
		defer m.reconnecting.Store(false)
		for {
			if ctx.Err() != nil {
				log.Printf("ctx canceled during reconnect loop.")
				return
			}

			tctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			conn, err := pgx.Connect(tctx, m.dsn)
			cancel()
			if err != nil {
				continue
			}

			// re-initialize connection
			m.conn = conn

			// re-initialize map
			m.m = make(map[string]context.CancelFunc)

			// set status back to online
			m.online.Store(true)
			log.Printf("database connection restored...")
			return
		}
	}(ctx)
}

// request will provide the caller with the results of a lock request.
//
// request is guaranteed to return a response.
func (m *guard) request(r request) response {
	m.chanMu.Lock()
	defer m.chanMu.Unlock()
	select {
	case m.reqChan <- r:
		// if a request is pushed, a response is guarenteed
		resp := <-r.respChan
		return resp
	default:
		// this will only happen if guard is shutting down
		// and reqChan is nil
		return response{ok: false, err: fmt.Errorf("managers ctx has been canceled")}
	}
}

// quit ensures graceful termination of the guard.
func (m *guard) quit() {
	for m.reconnecting.Load().(bool) {
		// waiting for reconnect loop to quit.
		// should be a short spin
	}

	// set offline. no new requests will enter the guard
	m.online.Store(false)

	// replace channel with nil causing any in-flight lock requests to fail.
	m.chanMu.Lock()
	rc := m.reqChan
	m.reqChan = nil
	m.chanMu.Unlock()

	// safe to close channel now
	close(rc)

	// drain any requests which made it to the channel before nil swap
	for req := range rc {
		var resp response
		resp.err = fmt.Errorf("managers ctx canceled. construct a new one")
		resp.ok = false
		req.respChan <- resp
	}

	// cancel all lock holder's ctx(s)
	for key, f := range m.m {
		log.Printf("database disconnected. canceling lock for %s", key)
		f()
	}

	// if conn is present close it. releases all locks
	if m.conn != nil {
		m.conn.Close(m.root)
	}
	// for good measure, this will already be canceled since its derived
	// from the ctx that got us here.
	m.cancel()
}

// handleRequest multiplexes guard requests to the appropriate postgres
// methods.
//
// handleRequest is guarenteed to have exclusive access to m.conn and m.m
func (m *guard) handleRequest(req request, ctx context.Context) {
	var resp response

	if !m.online.Load().(bool) {
		resp.err = ErrDatabaseUnavailable
		resp.ok = false
		req.respChan <- resp
	}

	switch req.t {
	case Lock:
		resp := m.lock(m.root, req.key)
		req.respChan <- resp
	case Unlock:
		resp := m.unlock(m.root, req.key)
		req.respChan <- resp
	default:
	}

	return
}

func (m *guard) lock(ctx context.Context, key string) response {
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

func (m *guard) unlock(ctx context.Context, key string) response {
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
