package golocks

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
)

type MongoDBLockProxy struct {
	con         *mongo.Client
	conLock     sync.Mutex
	isConnected bool
}

func (p *MongoDBLockProxy) Connect(ctx context.Context, connectionURI string) error {
	p.conLock.Lock()
	defer p.conLock.Unlock()
	if !p.isConnected {
		var err error
		p.con, err = mongo.Connect(ctx, mongoOptions.Client().ApplyURI(connectionURI))
		if err != nil {
			return err
		}
		p.isConnected = true
	}
	return nil
}

func (p *MongoDBLockProxy) Ping(ctx context.Context) error {
	if !p.isConnected {
		return ErrNotConnected
	}
	return p.con.Ping(ctx, readpref.Primary())
}

func (p *MongoDBLockProxy) Migrate(ctx context.Context, locksTable string) error {
	if !p.isConnected {
		return ErrNotConnected
	}
	err := p.con.Database("").CreateCollection(ctx, locksTable)
	return err
}

func (p *MongoDBLockProxy) Lock(ctx context.Context, locksTable string, lockId string, lockTimeSeconds int, sessionId string) (*Lock, error) {
	return nil, ErrUnexpected
}

func (p *MongoDBLockProxy) Unlock(ctx context.Context, locksTable string, lockId string, sessionId string, deleteOnRelease bool) error {
	return ErrUnexpected
}

func (p *MongoDBLockProxy) Close(ctx context.Context) {
	p.conLock.Lock()
	defer p.conLock.Unlock()
	if p.isConnected {
		_ = p.con.Disconnect(ctx) // TODO handle error ?
		p.isConnected = false
	}
}
