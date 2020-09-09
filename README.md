# DistLock
Dist lock implements a distributed lock via postgres.

## Goals

* No Transcations.
* A single manager uses a single DB conn.
* Cleanup databse locks on process death.
* Cleanup application locks on database death.
* A ctx based api where ctx is canceled when lock is lost
