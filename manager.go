package distlock

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
)

// Manager provides a client facing api for obtaining and returning
// distributed locks.
//
// Manager must not be copied after construction.
type Manager struct {
	// Guard provides a concurrency safe request/response api with
	// guarentees against deadlocks and races.
	g *guard
}

func NewManager(ctx context.Context, dsn string) (*Manager, error) {
	reqChan := make(chan request, 1024)
	m := make(map[string]context.CancelFunc)

	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	root, cancel := context.WithCancel(ctx)

	g := &guard{
		root:    root,
		cancel:  cancel,
		reqChan: reqChan,
		dsn:     dsn,
		m:       m,
		conn:    conn,
	}
	g.online.Store(true)
	g.reconnecting.Store(false)
	go g.ioLoop(g.root)

	return &Manager{
		g: g,
	}, nil
}

func (m *Manager) Lock(key string) (context.Context, error) {
	c := make(chan response)
	req := request{
		t:        Lock,
		key:      key,
		respChan: c,
	}

	if !m.g.online.Load().(bool) {
		return nil, ErrDatabaseUnavailable
	}

	resp := m.g.request(req)

	if !resp.ok {
		return nil, resp.err
	}
	return resp.ctx, nil
}

func (m *Manager) Unlock(key string) error {
	c := make(chan response)
	req := request{
		t:        Unlock,
		key:      key,
		respChan: c,
	}

	if !m.g.online.Load().(bool) {
		return ErrDatabaseUnavailable
	}

	resp := m.g.request(req)

	if !resp.ok {
		return resp.err
	}
	return nil
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

		if !m.g.online.Load().(bool) {
			return nil, ErrDatabaseUnavailable
		}

		resp := m.g.request(req)
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
