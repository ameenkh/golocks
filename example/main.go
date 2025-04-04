package main

import (
	"context"
	"fmt"
	"github.com/ameenkh/golocks"
	"time"
)

func main() {
	ctx := context.Background()
	var err error
	lm, err := golocks.NewLockManager(ctx,
		golocks.WithDBStore(golocks.DBStore{DBType: golocks.Postgres, ConnectionURI: "postgres://postgres:postgres@localhost:5432?sslmode=disable"}),
		//golocks.WithDBStore(golocks.DBStore{DBType: golocks.MongoDB, ConnectionURI: "mongodb://localhost:27017"}),
		golocks.WithDeleteOnRelease(false),
		golocks.WithLockTimeSeconds(1),
		golocks.WithPingAfterConnection(true),
	)
	if err != nil {
		fmt.Printf("failed to create locks manager: %v\n", err)
	}
	lock, err := lm.Lock(ctx, "test-lock", "")
	if lock != nil {
		fmt.Printf("lock: %v, remaining: %v\n\n", lock, lock.Remaining())
		fmt.Printf("isLocked: %v, isReleased: %v\n\n", lock.IsLocked(), lock.IsReleased())
	}
	fmt.Printf("err: %v\n\n", err)

	time.Sleep(1 * time.Second)
	err = lm.Unlock(ctx, lock)
	fmt.Printf("err 2: %v\n\n", err)
	fmt.Printf("isLocked 2: %v, isReleased 2: %v\n\n", lock.IsLocked(), lock.IsReleased())
}
