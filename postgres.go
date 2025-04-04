package golocks

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
)

var (
	acquireLockTemplate = `
		INSERT INTO %s AS t (id, locked_until, session_id) VALUES ('%s', CURRENT_TIMESTAMP + INTERVAL '%d second', '%s')
		ON CONFLICT (id) DO UPDATE SET locked_until = EXCLUDED.locked_until, session_id = EXCLUDED.session_id
		WHERE t.locked_until <= CURRENT_TIMESTAMP
		RETURNING id, locked_until, session_id`
	releaseLockTemplate = `
		UPDATE %s SET locked_until = CURRENT_TIMESTAMP, session_id = '' WHERE id = '%s' AND session_id = '%s' AND locked_until > CURRENT_TIMESTAMP`
	releaseAndDeleteLockTemplate = `
		DELETE FROM %s WHERE id = '%s' AND session_id = '%s'
		RETURNING locked_until > CURRENT_TIMESTAMP`
	createTableTemplate = `
		CREATE TABLE IF NOT EXISTS %s (
		    id varchar not null,
		    locked_until timestamp not null,
		    session_id varchar
		)`
	createIndexTemplate = `
		CREATE UNIQUE INDEX IF NOT EXISTS %s_id_uindex ON %s (id)`
)

type PostgresLockProxy struct {
	con         *pgxpool.Pool
	conLock     sync.Mutex
	isConnected bool
}

func (p *PostgresLockProxy) Connect(ctx context.Context, connectionURI string) error {
	p.conLock.Lock()
	defer p.conLock.Unlock()
	if !p.isConnected {
		var err error
		p.con, err = pgxpool.New(ctx, connectionURI)
		if err != nil {
			return err
		}
		p.isConnected = true
	}
	return nil
}

func (p *PostgresLockProxy) Ping(ctx context.Context) error {
	if !p.isConnected {
		return ErrNotConnected
	}
	return p.con.Ping(ctx)
}

func (p *PostgresLockProxy) Migrate(ctx context.Context, locksTable string) error {
	if !p.isConnected {
		return ErrNotConnected
	}
	createTableQuery := fmt.Sprintf(createTableTemplate, locksTable)
	_, err := p.con.Exec(ctx, createTableQuery)
	if err != nil {
		return err
	}
	createIndexQuery := fmt.Sprintf(createIndexTemplate, locksTable, locksTable)
	_, err = p.con.Exec(ctx, createIndexQuery)
	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresLockProxy) Lock(ctx context.Context, locksTable string, lockId string, lockTimeSeconds int, sessionId string) (*Lock, error) {
	lockQuery := fmt.Sprintf(acquireLockTemplate, locksTable, lockId, lockTimeSeconds, sessionId)
	rows, err := p.con.Query(ctx, lockQuery)
	if err != nil {
		return nil, err
	}

	lock, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[Lock])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrLockAlreadyAcquired
		}
		if errors.Is(err, pgx.ErrTooManyRows) {
			return nil, ErrUnexpected
		}
		return nil, err
	}

	return &lock, nil
}

func (p *PostgresLockProxy) Unlock(ctx context.Context, locksTable string, lockId string, sessionId string, deleteOnRelease bool) error {
	var releaseQuery string
	if deleteOnRelease {
		releaseQuery = fmt.Sprintf(releaseAndDeleteLockTemplate, locksTable, lockId, sessionId)
		rows, err := p.con.Query(ctx, releaseQuery)
		if err != nil {
			return err
		}

		released, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrReleaseLock
			}
			if errors.Is(err, pgx.ErrTooManyRows) {
				return ErrUnexpected
			}
			return err
		}
		if !released {
			return ErrReleaseLock
		}
		return nil
	} else {
		releaseQuery = fmt.Sprintf(releaseLockTemplate, locksTable, lockId, sessionId)
		commandTag, err := p.con.Exec(ctx, releaseQuery)
		if err != nil {
			return err
		}
		affectedRows := commandTag.RowsAffected()
		if affectedRows == 1 {
			return nil
		}
		if affectedRows == 0 {
			return ErrReleaseLock
		}
		return ErrUnexpected
	}
}

func (p *PostgresLockProxy) Close(ctx context.Context) {
	p.conLock.Lock()
	defer p.conLock.Unlock()
	if p.isConnected {
		p.con.Close()
		p.isConnected = false
	}
}
