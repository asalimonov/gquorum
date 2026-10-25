package disk

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	bolt "go.etcd.io/bbolt"
)

// OpenReadOnlyDB never initializes a missing database or creates buckets. Its
// bound covers decoded key/value input, excluding unused bbolt pages.
func OpenReadOnlyDB(ctx context.Context, path string, maxBytes uint64) (*bolt.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxBytes == 0 {
		return nil, fault.ErrInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fault.ErrUnreadable
	}
	timeout := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	db, err := bolt.Open(path, fileMode, &bolt.Options{ReadOnly: true, Timeout: timeout})
	if err != nil {
		return nil, fault.New(fault.Unreadable, "open retained database read-only", err)
	}
	err = db.View(func(tx *bolt.Tx) error { return CheckReadBudget(ctx, tx, maxBytes) })
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

// CheckReadBudget visits key/value sizes before any typed record decoder allocates.
func CheckReadBudget(ctx context.Context, tx *bolt.Tx, maxBytes uint64) error {
	budget := dbReadBudget{ctx: ctx, remaining: maxBytes}
	return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
		if err := budget.take(name); err != nil {
			return err
		}
		return budget.bucket(bucket, 0)
	})
}

type dbReadBudget struct {
	ctx       context.Context
	remaining uint64
}

func (b *dbReadBudget) take(data []byte) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if uint64(len(data)) > b.remaining {
		return fault.New(fault.Unreadable, "retained metadata exceeds byte budget", model.ErrResourceBound)
	}
	b.remaining -= uint64(len(data))
	return nil
}
func (b *dbReadBudget) bucket(bucket *bolt.Bucket, depth int) error {
	const maxBucketDepth = 128
	if depth > maxBucketDepth {
		return fault.ErrUnreadable
	}
	return bucket.ForEach(func(key, value []byte) error {
		if err := b.take(key); err != nil {
			return err
		}
		if value != nil {
			return b.take(value)
		}
		child := bucket.Bucket(key)
		if child == nil {
			return fault.ErrUnreadable
		}
		return b.bucket(child, depth+1)
	})
}
