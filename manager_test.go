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

func stopDB(t *testing.T) {
	// a command to rip down postgres unexpectedly
	cmd := exec.Command(
		"docker-compose",
		"down",
	)
	err := cmd.Run()
	if err != nil {
		t.Fatalf("could not rip database down: %v", err)
	}
	log.Printf("db down")
}

func startDB(t *testing.T) {
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
	log.Printf("db started")
}

func waitDB(t *testing.T) {
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
	log.Printf("db up")
}

func Test_Manager(t *testing.T) {
	startDB(t)
	waitDB(t)

	// concurrency bugs are intermittent. lets run
	// these tests a bunch of times.

	for i := 0; i < 9; i++ {
		test_MultiSessionMutualExclusion(t)
	}
	for i := 0; i < 9; i++ {
		test_TryLockSingleSession(t)
	}
	for i := 0; i < 9; i++ {
		test_TryLockMultiSession(t)
	}
	for i := 0; i < 9; i++ {
		test_ProcessDeath(t)
	}
	for i := 0; i < 9; i++ {
		test_DBFlap(t)
	}
	// should be last test, it rips down the local db
	test_DBFailure(t)
}

func test_DBFlap(t *testing.T) {
	// get a lock
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, dsn)

	key := "test-key"
	_, err = manager.Lock(key)
	if err != nil {
		t.Fatal(err)
	}

	stopDB(t)

	// make sure we get an error trying to get a lock
	_, err = manager.Lock(key)
	if err == nil {
		t.Fatal("expected error")
	}

	startDB(t)
	waitDB(t)

	log.Printf("sleeping...")
	time.Sleep(1 * time.Second)
	log.Printf("done sleeping...")

	// make sure we dont get an error getting a lock
	_, err = manager.Lock(key)
	if err != nil {
		t.Fatal(err)
	}
}

// this is a hack of a test, but you can run this, then ctrl-c on the command line
// and confirm when the process dies the lock is removed from the database.
//
// do not inclue in the automated tests.
func test_ProcessDeathManual(t *testing.T) {
	const (
		query = `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';`
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, dsn)

	key := "test-key"
	_, err = manager.Lock(key)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Hour)
}

func test_ProcessDeath(t *testing.T) {
	const (
		query = `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';`
	)

	tctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	mctx, mCancel := context.WithCancel(context.Background())
	manager, err := NewManager(mctx, dsn)

	// take some locks
	keys := []string{"test-key0", "test-key1", "test-key2", "test-key3"}
	for _, key := range keys {
		_, err = manager.Lock(key)
		if err != nil {
			t.Fatal(err)
		}
	}

	// confirm locks in db
	tctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn1, err := pgx.ConnectConfig(tctx, conf)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	var i int

	row := conn1.QueryRow(context.Background(), query)
	err = row.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != len(keys) {
		t.Fatalf("got: %v want: %v", i, len(keys))
	}

	// cancel manager ctx, should drop all locks
	mCancel()
	time.Sleep(100 * time.Millisecond)

	row = conn1.QueryRow(context.Background(), query)
	err = row.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 0 {
		t.Fatalf("got: %v want: %v", i, 0)
	}
}

func test_TryLockMultiSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager0, err := NewManager(ctx, dsn)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	manager1, err := NewManager(ctx, dsn)

	key := "test-key0"

	var acquired struct {
		sync.Mutex
		b bool
	}
	// launch goroutine that will acquire lock and unlock
	// in some time
	go func() {
		_, err := manager0.Lock(key)
		if err != nil {
			t.Fatal(err)
		}

		acquired.Lock()
		acquired.b = true
		acquired.Unlock()
		log.Printf("goroutine 0 acquired lock. sleeping for 5 seconds before unlock")

		time.Sleep(5 * time.Second)
		err = manager0.Unlock(key)
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
	err = manager1.Unlock(key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

}

func test_TryLockSingleSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, dsn)

	key := "test-key0"

	var acquired struct {
		sync.Mutex
		b bool
	}
	// launch goroutine that will acquire lock and unlock
	// in some time
	go func() {
		_, err := manager.Lock(key)
		if err != nil {
			t.Fatal(err)
		}

		acquired.Lock()
		acquired.b = true
		acquired.Unlock()
		log.Printf("goroutine 0 acquired lock. sleeping for 5 seconds before unlock")

		time.Sleep(5 * time.Second)
		err = manager.Unlock(key)
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
	err = manager.Unlock(key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

}

func test_DBFailure(t *testing.T) {
	// create some locks
	keys := []string{"test-key0", "test-key1", "test-key2", "test-key3"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := NewManager(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, key := range keys {
		ctx, err := manager.Lock(key)
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
	stopDB(t)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager0, err := NewManager(ctx, dsn)
	manager1, err := NewManager(ctx, dsn)

	// test mutual exclusion
	key := "test-key"
	var wg sync.WaitGroup

	acquire := make([]bool, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := manager0.Lock(key)
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
		_, err := manager1.Lock(key)
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
		err = manager0.Unlock(key)
		if err != nil {
			t.Fatal(err)
		}
		// a subsequent call to unlock should fail.
		err = manager0.Unlock(key)
		if err == nil {
			t.Fatal(err)
		}
	}
	if acquire[1] {
		count++
		// go routine 1 informed us it acquired the lock.
		// attempt unlock and check error
		err = manager1.Unlock(key)
		if err != nil {
			t.Fatal(err)
		}
		// a subsequent call to unlock should fail.
		err = manager1.Unlock(key)
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
	manager, err := NewManager(ctx, dsn)

	// test mutual exclusion
	key := "test-key"
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		ii := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := manager.Lock(key)
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
	err = manager.Unlock(key)
	if err != nil {
		t.Fatal(err)
	}
}
