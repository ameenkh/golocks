package golocks

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"time"
)

var (
	// ErrLockAlreadyAcquired occurs when trying to acquire an already acquired lock
	ErrLockAlreadyAcquired = errors.New("lock already acquired")
	// ErrReleaseLock occurs when the lock is already released or if the lock is acquired by another session
	ErrReleaseLock = errors.New("lock is already released or its acquired by another session")
	// ErrUnexpected occurs when multiple rows were affected by lock/release query
	ErrUnexpected   = errors.New("unexpected error: multiple rows were affected by query")
	ErrNotConnected = errors.New("not connected to database")
	ErrNil          = errors.New("lock is nil")
)

type Lock struct {
	Id          string    `db:"id"`
	LockedUntil time.Time `db:"locked_until"`
	SessionId   string    `db:"session_id"`
}

func (l *Lock) Remaining() time.Duration {
	return max(l.LockedUntil.Sub(time.Now()), 0)
}

func (l *Lock) IsLocked() bool {
	return l.LockedUntil.Compare(time.Now()) > 0
}

func (l *Lock) IsReleased() bool {
	return l.LockedUntil.Compare(time.Now()) <= 0
}

type DBLocker interface {
	Connect(ctx context.Context, connectionURI string) error
	Ping(ctx context.Context) error
	Migrate(ctx context.Context, locksTable string) error
	Lock(ctx context.Context, locksTable string, lockId string, lockTimeSeconds int, sessionId string) (*Lock, error)
	Unlock(ctx context.Context, locksTable string, lockId string, sessionId string, deleteOnRelease bool) error
	Close(ctx context.Context)
}

type LockManager struct {
	locksTable      string
	lockTimeSeconds int
	deleteOnRelease bool
	dbLocker        DBLocker
}

func NewLockManager(ctx context.Context, options ...LockManagerOption) (*LockManager, error) {
	opts := &LockManagerOptions{
		locksTable:          "locks",
		lockTimeSeconds:     60,
		deleteOnRelease:     false,
		dbStore:             DBStore{},
		pingAfterConnection: false,
	}
	for _, option := range options {
		option(opts)
	}

	if opts.lockTimeSeconds <= 0 {
		return nil, errors.New("lockTimeSeconds must be greater than zero")
	}

	var dbLocker DBLocker
	switch opts.dbStore.DBType {
	case Postgres:
		dbLocker = &PostgresLockProxy{}
	case MongoDB:
		dbLocker = &MongoDBLockProxy{}
	default:
		return nil, errors.New("database type is undefined")
	}

	// TODO decide where to put the connect function
	err := dbLocker.Connect(ctx, opts.dbStore.ConnectionURI)
	if err != nil {
		return nil, err
	}

	err = dbLocker.Migrate(ctx, opts.locksTable)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	if opts.pingAfterConnection {
		if err = dbLocker.Ping(ctx); err != nil {
			return nil, err
		}
	}

	return &LockManager{
		locksTable:      opts.locksTable,
		lockTimeSeconds: opts.lockTimeSeconds,
		deleteOnRelease: opts.deleteOnRelease,
		dbLocker:        dbLocker,
	}, nil
}

func (lm *LockManager) Lock(ctx context.Context, lockId string, sessionId string) (*Lock, error) {
	if sessionId == "" {
		sessionId = uuid.NewString()
	}
	return lm.dbLocker.Lock(ctx, lm.locksTable, lockId, lm.lockTimeSeconds, sessionId)
}

func (lm *LockManager) Unlock(ctx context.Context, lock *Lock) error {
	if lock == nil {
		return ErrNil
	}
	err := lm.dbLocker.Unlock(ctx, lm.locksTable, lock.Id, lock.SessionId, lm.deleteOnRelease)
	if err == nil {
		lock.LockedUntil = time.Now()
		lock.SessionId = ""
	}
	return err
}

func (lm *LockManager) Close(ctx context.Context) {
	lm.dbLocker.Close(ctx)
}
