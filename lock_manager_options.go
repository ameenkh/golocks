package golocks

type LockManagerOptions struct {
	locksTable          string
	lockTimeSeconds     int
	deleteOnRelease     bool
	dbStore             DBStore
	pingAfterConnection bool
}

type DBType int

const (
	Undefined DBType = iota
	Postgres
	MongoDB
)

type DBStore struct {
	DBType        DBType
	ConnectionURI string
}

type LockManagerOption func(*LockManagerOptions)

func WithLocksTable(locksTable string) LockManagerOption {
	return func(o *LockManagerOptions) {
		o.locksTable = locksTable
	}
}

func WithLockTimeSeconds(lockTimeSeconds int) LockManagerOption {
	return func(o *LockManagerOptions) {
		o.lockTimeSeconds = lockTimeSeconds
	}
}

func WithDeleteOnRelease(deleteOnRelease bool) LockManagerOption {
	return func(o *LockManagerOptions) {
		o.deleteOnRelease = deleteOnRelease
	}
}

func WithDBStore(dbStore DBStore) LockManagerOption {
	return func(o *LockManagerOptions) {
		o.dbStore = dbStore
	}
}

func WithPingAfterConnection(pingAfterConnection bool) LockManagerOption {
	return func(o *LockManagerOptions) {
		o.pingAfterConnection = pingAfterConnection
	}
}
