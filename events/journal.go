package events

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

type JournalOptions struct {
	Path         string
	Retention    time.Duration
	MaxLiveBytes int64
	Logger       *slog.Logger
}
type Journal struct {
	db                                *bolt.DB
	opts                              JournalOptions
	now                               func() time.Time
	liveSize                          func() int64
	sinceRotate                       int
	lastLiveBytes, appendedSinceStats int64
	closeOnce                         sync.Once
	closeErr                          error
}

var bucketEvents = []byte("events")
var bucketMeta = []byte("meta")

func OpenJournal(opts JournalOptions) (*Journal, error) {
	if opts.Path == "" || opts.Retention < 0 || opts.MaxLiveBytes < 0 {
		return nil, fault.New(fault.Invalid, "explicit journal path and nonnegative retention/size required", nil)
	}
	if opts.Retention == 0 {
		opts.Retention = 30 * 24 * time.Hour
	}
	if opts.MaxLiveBytes == 0 {
		opts.MaxLiveBytes = 64 << 20
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(opts.Path, 0o600, &bolt.Options{Timeout: 5 * time.Second, NoSync: true})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if v := b.Get([]byte("schema_version")); v != nil && string(v) != "1" {
			return fault.New(fault.Unsupported, "unsupported event journal schema", nil)
		}
		if err := b.Put([]byte("schema_version"), []byte("1")); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketEvents)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Journal{db: db, opts: opts, now: time.Now}, nil
}

func (j *Journal) Append(e Event) (Event, error) {
	e = clone(e)
	e.Seq = 0
	if j == nil {
		if e.At.IsZero() {
			e.At = time.Now().UTC()
		}
		return e, nil
	}
	if e.At.IsZero() {
		e.At = j.now().UTC()
	} else {
		e.At = e.At.UTC()
	}
	err := j.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEvents)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		e.Seq = seq
		data, err := encodeEvent(e)
		if err != nil {
			return err
		}
		if err := b.Put(be(seq), data); err != nil {
			return err
		}
		j.appendedSinceStats += int64(len(data))
		j.sinceRotate++
		if j.sinceRotate >= 64 {
			j.sinceRotate = 0
			return j.rotate(tx, false)
		}
		return nil
	})
	if err != nil {
		e.Seq = 0
		return e, err
	}
	return e, nil
}

func (j *Journal) rotate(tx *bolt.Tx, fullAgeScan bool) error {
	b := tx.Bucket(bucketEvents)
	cutoff := j.now().Add(-j.opts.Retention)
	var doomed [][]byte
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		e, err := decodeEvent(v)
		// Periodic rotation prunes the head with bounded amortized work. Explicit
		// Prune also finds expired entries after an observer clock moved backwards.
		if err != nil || !e.At.After(cutoff) {
			doomed = append(doomed, bytes.Clone(k))
		} else if !fullAgeScan {
			break
		}
	}
	if j.liveSize != nil || j.lastLiveBytes > j.opts.MaxLiveBytes-j.opts.MaxLiveBytes/16 || j.appendedSinceStats > j.opts.MaxLiveBytes/16 {
		stats := b.Stats()
		measured := int64(stats.BranchInuse + stats.LeafInuse)
		if j.liveSize != nil {
			measured = j.liveSize()
		}
		j.lastLiveBytes = measured
		j.appendedSinceStats = 0
		if measured > j.opts.MaxLiveBytes {
			n := max(1, stats.KeyN/10)
			seen := make(map[string]bool, len(doomed))
			for _, k := range doomed {
				seen[string(k)] = true
			}
			for k, _ := c.First(); k != nil && n > 0; k, _ = c.Next() {
				if !seen[string(k)] {
					doomed = append(doomed, bytes.Clone(k))
					n--
				}
			}
		}
	}
	for _, k := range doomed {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (j *Journal) List(ctx context.Context, q Query) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if j == nil {
		return nil, nil
	}
	if q.Limit == 0 {
		q.Limit = 100
	}
	if q.Limit < 0 {
		return nil, fault.ErrInvalid
	}
	q.Limit = min(q.Limit, 500)
	var result []Event
	err := j.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketEvents).Cursor()
		for k, v := c.Seek(be(q.AfterSeq)); k != nil && len(result) < q.Limit; k, v = c.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if binary.BigEndian.Uint64(k) <= q.AfterSeq {
				continue
			}
			e, err := decodeEvent(v)
			if err != nil {
				return fault.New(fault.Unreadable, "decode journal event", err)
			}
			if q.Filter.matches(e) {
				result = append(result, e)
			}
		}
		return nil
	})
	return result, err
}
func (j *Journal) Prune(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j == nil {
		return nil
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return j.rotate(tx, true)
	})
}
func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	j.closeOnce.Do(func() { j.closeErr = j.db.Close() })
	return j.closeErr
}
func be(n uint64) []byte { data := make([]byte, 8); binary.BigEndian.PutUint64(data, n); return data }
