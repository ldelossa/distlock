package distlock

import (
	"context"
	"log"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
)

// pg_advisory session locks are nestable over the same conn.
// this means if you issue a lock for a specific key over the same conn
// the lock *will* be granted.
//
// this is not the behavior we want. instead the mananger will check if a session
// lock exists in it's own business logic, and reject the lock request if so.
// this functionality is checked in Test_SingleSessionMutualExclusion
//
// two distinct processes will generate two distinct connections. making a lock
// request across two distinct connections *will* work the way we expect.
// this functionality is checked in Test_MultiSessionMutualExclusion

const (
	dsn = "host=localhost port=5434 user=distlock dbname=distlock sslmode=disable"
)

func Test_Manager(t *testing.T) {
	// a command to start local postgres instance
	cmd := exec.Command(
		"docker-compose",
		"up",
		"-d",
	)
	err := cmd.Run()
	if err != nil {
		t.Fatalf("failed to start the local postgres instance: %v", err)
	}

	// spin in db becoming available
	up := false
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	for !up {
		tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
		conn, err := pgx.ConnectConfig(tctx, conf)
		if err != nil {
			log.Printf("database not available yet: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		up = true
		conn.Close(tctx)
	}

	test_SingleSessionMutualExclusion(t)
	test_MultiSessionMutualExclusion(t)
	test_TryLockSingleSession(t)
	test_TryLockMultiSession(t)
	test_ProcessDeath(t)
	// should be last test, it rips down the local db
	test_DBFailure(t)
}

// this is a hack of a test, but you can run this, then ctrl-c on the command line
// and confirm when the process dies the lock is removed from the database.
//
// do not inclue in the automated tests.
func test_ProcessDeathManual(t *testing.T) {
	const (
		query = `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';`
	)

	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	ctx := context.Background()
	manager0 := NewManager(ctx, conn0)

	key := "test-key"
	_, err = manager0.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Hour)
}

func test_ProcessDeath(t *testing.T) {
	const (
		query = `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';`
	)

	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	ctx := context.Background()
	manager0 := NewManager(ctx, conn0)

	key := "test-key"
	_, err = manager0.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// forceable close the connection simulating process
	// death
	err = manager0.close(ctx)
	if err == nil {
		t.Fatal("expected error when force closing channel")
	}

	conn1, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	var i int
	row := conn1.QueryRow(ctx, query)
	err = row.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i > 0 {
		t.Fatal("lock still exists in db")
	}
}

func test_TryLockMultiSession(t *testing.T) {
	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	ctx := context.Background()
	manager0 := NewManager(ctx, conn0)

	conn1, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	ctx = context.Background()
	manager1 := NewManager(ctx, conn1)

	key := "test-key0"

	var acquired struct {
		sync.Mutex
		b bool
	}
	// launch goroutine that will acquire lock and unlock
	// in some time
	go func() {
		_, err := manager0.Lock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		acquired.Lock()
		acquired.b = true
		acquired.Unlock()
		log.Printf("goroutine 0 acquired lock. sleeping for 5 seconds before unlock")

		time.Sleep(5 * time.Second)
		err = manager0.Unlock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// lock dance to confirm goroutine 0 acquired dist lock
	for {
		acquired.Lock()
		if acquired.b {
			break
		}
		acquired.Unlock()
	}
	acquired.Unlock()

	tctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	ctx, err = manager1.TryLock(tctx, key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	tctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	err = manager1.Unlock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

}

func test_TryLockSingleSession(t *testing.T) {
	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	ctx := context.Background()
	manager := NewManager(ctx, conn0)

	key := "test-key0"

	var acquired struct {
		sync.Mutex
		b bool
	}
	// launch goroutine that will acquire lock and unlock
	// in some time
	go func() {
		_, err := manager.Lock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		acquired.Lock()
		acquired.b = true
		acquired.Unlock()
		log.Printf("goroutine 0 acquired lock. sleeping for 5 seconds before unlock")

		time.Sleep(5 * time.Second)
		err = manager.Unlock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// lock dance to confirm goroutine 0 acquired dist lock
	for {
		acquired.Lock()
		if acquired.b {
			break
		}
		acquired.Unlock()
	}
	acquired.Unlock()

	tctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	ctx, err = manager.TryLock(tctx, key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	tctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	err = manager.Unlock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

}

func test_DBFailure(t *testing.T) {
	// a command to rip down postgres unexpectedly
	cmd := exec.Command(
		"docker-compose",
		"down",
	)

	// create some locks
	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	locks := []string{"test-key0", "test-key1", "test-key2", "test-key3"}
	ctx, _ := context.WithCancel(context.Background())
	manager := NewManager(ctx, conn0)

	var wg sync.WaitGroup
	for _, lock := range locks {
		ctx, err := manager.Lock(ctx, lock)
		if err != nil {
			t.Fatal(err)
		}
		// launch routines waiting on ctx, they should unblock when
		// we rip the database down.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
		}()
	}

	// rip database down
	err = cmd.Run()
	if err != nil {
		t.Fatalf("could not rip database down: %v", err)
	}

	wg.Wait()
}

func test_MultiSessionMutualExclusion(t *testing.T) {
	tctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	conn0, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	if conn0 == nil {
		t.Fatalf("conn is nil")
	}
	conn1, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	if conn1 == nil {
		t.Fatalf("conn is nil")
	}

	ctx, _ := context.WithCancel(context.Background())
	manager0 := NewManager(ctx, conn0)
	manager1 := NewManager(ctx, conn1)

	// test mutual exclusion
	key := "test-key"
	var wg sync.WaitGroup

	acquire := make([]bool, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := manager0.Lock(ctx, key)
		if err != nil {
			if err == ErrMutualExclusion {
				log.Printf("go routine %d lost the race", 0)
				return
			}
			log.Printf("ERR: go routine %d encountered error: %v", 0, err)
			return
		}
		log.Printf("go routine %d acquired lock", 0)
		acquire[0] = true
		return
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := manager1.Lock(ctx, key)
		if err != nil {
			if err == ErrMutualExclusion {
				log.Printf("go routine %d lost the race", 1)
				return
			}
			log.Printf("ERR: go routine %d encountered error: %v", 1, err)
			return
		}
		log.Printf("go routine %d acquired lock", 1)
		acquire[1] = true
		return
	}()
	wg.Wait()

	count := 0
	if acquire[0] {
		count++
		// go routine 0 informed us it acquired the lock.
		// attempt unlock and check error
		err = manager0.Unlock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		// a subsequent call to unlock should fail.
		err = manager0.Unlock(ctx, key)
		if err == nil {
			t.Fatal(err)
		}
	}
	if acquire[1] {
		count++
		// go routine 1 informed us it acquired the lock.
		// attempt unlock and check error
		err = manager1.Unlock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		// a subsequent call to unlock should fail.
		err = manager1.Unlock(ctx, key)
		if err == nil {
			t.Fatal(err)
		}
	}

	if count == 2 {
		t.Fatal("both go routines reported acquiring the lock")
	}
}

func test_SingleSessionMutualExclusion(t *testing.T) {
	tctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	if conn == nil {
		t.Fatalf("conn is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewManager(ctx, conn)

	// test mutual exclusion
	key := "test-key"
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		ii := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := manager.Lock(ctx, key)
			if err != nil {
				if err == ErrMutualExclusion {
					log.Printf("go routine %d lost the race", ii)
					return
				}
				log.Printf("ERR: go routine %d encountered error: %v", ii, err)
				return
			}
			log.Printf("go routine %d acquired lock", ii)
			return
		}()
	}
	wg.Wait()
	err = manager.Unlock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
}
