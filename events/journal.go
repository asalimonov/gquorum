package events

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultRetention        = 30 * 24 * time.Hour
	defaultMaxLiveBytes     = 64 << 20
	directoryMode           = 0o700
	fileMode                = 0o600
	liveStatsRefreshDivisor = 16
	defaultQueryLimit       = 100
	maximumQueryLimit       = 500
	uint64ByteCount         = 8
)

// JournalOptions configures the journal path, retention, size limit and logger.
type JournalOptions struct {
	Path         string
	Retention    time.Duration
	MaxLiveBytes int64
	Logger       *slog.Logger
}

type rotationMode uint8

const (
	periodicRotation rotationMode = iota
	fullAgeRotation
)

// Journal stores node-local events in a bounded bbolt database.
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

// OpenJournal opens or creates a journal and validates its schema.
func OpenJournal(opts JournalOptions) (*Journal, error) {
	if opts.Path == "" || opts.Retention < 0 || opts.MaxLiveBytes < 0 {
		return nil, fault.New(fault.Invalid, "explicit journal path and nonnegative retention/size required", nil)
	}
	if opts.Retention == 0 {
		opts.Retention = defaultRetention
	}
	if opts.MaxLiveBytes == 0 {
		opts.MaxLiveBytes = defaultMaxLiveBytes
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), directoryMode); err != nil {
		return nil, err
	}
	db, err := bolt.Open(opts.Path, fileMode, &bolt.Options{Timeout: 5 * time.Second, NoSync: true})
	if err != nil {
		return nil, err
	}
	updateErr := db.Update(func(tx *bolt.Tx) error {
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
	})
	if updateErr != nil {
		_ = db.Close()
		return nil, updateErr
	}
	return &Journal{db: db, opts: opts, now: time.Now}, nil
}

// Append stores an event with a journal-assigned sequence and applies retention
// limits.
func (j *Journal) Append(e Event) (Event, error) {
	event := clone(e)
	event.Seq = 0
	if j == nil {
		if event.At.IsZero() {
			event.At = time.Now().UTC()
		}
		return event, nil
	}
	if event.At.IsZero() {
		event.At = j.now().UTC()
	} else {
		event.At = event.At.UTC()
	}
	err := j.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEvents)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		event.Seq = seq
		data, err := encodeEvent(event)
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
			return j.rotate(tx, periodicRotation)
		}
		return nil
	})
	if err != nil {
		event.Seq = 0
		return event, err
	}
	return event, nil
}

func (j *Journal) rotate(tx *bolt.Tx, mode rotationMode) error {
	b := tx.Bucket(bucketEvents)
	cutoff := j.now().Add(-j.opts.Retention)
	c := b.Cursor()
	doomed := expiredKeys(c, cutoff, mode)
	if j.refreshSizeStats() {
		stats := b.Stats()
		measured := int64(stats.BranchInuse + stats.LeafInuse)
		if j.liveSize != nil {
			measured = j.liveSize()
		}
		j.lastLiveBytes = measured
		j.appendedSinceStats = 0
		if measured > j.opts.MaxLiveBytes {
			doomed = appendOversizedKeys(c, doomed, stats.KeyN)
		}
	}
	for _, k := range doomed {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func expiredKeys(c *bolt.Cursor, cutoff time.Time, mode rotationMode) [][]byte {
	var doomed [][]byte
	for k, v := c.First(); k != nil; k, v = c.Next() {
		e, err := decodeEvent(v)
		// Periodic rotation prunes the head with bounded amortized work.
		// Explicit Prune also finds expired entries after an observer clock
		// moved backwards.
		if err != nil || !e.At.After(cutoff) {
			doomed = append(doomed, bytes.Clone(k))
		} else if mode == periodicRotation {
			break
		}
	}
	return doomed
}

func (j *Journal) refreshSizeStats() bool {
	return j.liveSize != nil || j.lastLiveBytes > j.opts.MaxLiveBytes-j.opts.MaxLiveBytes/liveStatsRefreshDivisor ||
		j.appendedSinceStats > j.opts.MaxLiveBytes/liveStatsRefreshDivisor
}

func appendOversizedKeys(c *bolt.Cursor, doomed [][]byte, keyCount int) [][]byte {
	keys := slices.Clone(doomed)
	n := max(1, keyCount/10)
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		seen[string(k)] = true
	}
	for k, _ := c.First(); k != nil && n > 0; k, _ = c.Next() {
		if !seen[string(k)] {
			keys = append(keys, bytes.Clone(k))
			n--
		}
	}
	return keys
}

// List returns matching events in sequence order after the requested cursor.
func (j *Journal) List(ctx context.Context, q Query) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if j == nil {
		return nil, nil
	}
	if q.Limit == 0 {
		q.Limit = defaultQueryLimit
	}
	if q.Limit < 0 {
		return nil, fault.ErrInvalid
	}
	q.Limit = min(q.Limit, maximumQueryLimit)
	var result []Event
	err := j.db.View(func(tx *bolt.Tx) error {
		return listEvents(ctx, tx.Bucket(bucketEvents).Cursor(), q, &result)
	})
	return result, err
}

func listEvents(ctx context.Context, c *bolt.Cursor, q Query, result *[]Event) error {
	for k, v := c.Seek(be(q.AfterSeq)); k != nil && len(*result) < q.Limit; k, v = c.Next() {
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
			*result = append(*result, e)
		}
	}
	return nil
}

// Prune applies the journal retention and size limits.
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
		return j.rotate(tx, fullAgeRotation)
	})
}

// Close releases the database once and preserves its close result.
func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	j.closeOnce.Do(func() { j.closeErr = j.db.Close() })
	return j.closeErr
}

func be(n uint64) []byte {
	data := make([]byte, uint64ByteCount)
	binary.BigEndian.PutUint64(data, n)
	return data
}
