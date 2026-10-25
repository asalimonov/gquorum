package recoverydisk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

// DigestDB binds every ordered logical bucket/key/value and sequence while
// excluding unused pages, allocation layout and other physical bbolt padding.
func DigestDB(ctx context.Context, db *bolt.DB, maxBytes uint64) (string, error) {
	sum := sha256.New()
	err := db.View(func(tx *bolt.Tx) error {
		if err := CheckReadBudget(ctx, tx, maxBytes); err != nil {
			return err
		}
		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			digestDBBytes(sum, name)
			return digestBucket(ctx, sum, bucket, 0)
		})
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func digestBucket(ctx context.Context, sum hash.Hash, bucket *bolt.Bucket, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	const maxBucketDepth = 128
	if depth > maxBucketDepth {
		return fault.ErrUnreadable
	}
	var sequence [8]byte
	binary.BigEndian.PutUint64(sequence[:], bucket.Sequence())
	_, _ = sum.Write(sequence[:])
	err := bucket.ForEach(func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		digestDBBytes(sum, key)
		if value == nil {
			_, _ = sum.Write([]byte{1})
			return digestBucket(ctx, sum, bucket.Bucket(key), depth+1)
		}
		_, _ = sum.Write([]byte{0})
		digestDBBytes(sum, value)
		return nil
	})
	_, _ = sum.Write([]byte{2})
	return err
}

func digestDBBytes(sum hash.Hash, data []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	_, _ = sum.Write(length[:])
	_, _ = sum.Write(data)
}
