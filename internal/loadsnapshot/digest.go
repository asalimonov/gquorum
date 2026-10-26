package loadsnapshot

import (
	"context"
	"crypto/sha256"
)

const snapshotHashBlock = 1 << 20

type snapshotHashWork struct {
	waiters int
	done    chan struct{}
	sum     [sha256.Size]byte
	err     error
}

func (c *Coordinator) snapshotDigest(ctx context.Context, id SourceSnapshotID, data []byte) ([sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	c.mu.Lock()
	if sum, ok := c.digests[id]; ok {
		c.mu.Unlock()
		return sum, nil
	}
	if work := c.hashWork[id]; work != nil {
		work.waiters++
		c.mu.Unlock()
		defer func() { c.mu.Lock(); work.waiters--; c.mu.Unlock() }()
		select {
		case <-work.done:
			return work.sum, work.err
		case <-ctx.Done():
			return [sha256.Size]byte{}, ctx.Err()
		}
	}
	work := &snapshotHashWork{done: make(chan struct{})}
	c.hashWork[id] = work
	c.mu.Unlock()
	// The calling admitted job owns this work and its immutable data until actual
	// return. Singleflight waiters own independent reservations, never copies.
	sum, err := hashSnapshotBlocks(ctx, data)
	c.mu.Lock()
	work.sum, work.err = sum, err
	delete(c.hashWork, id)
	owner, live := c.digestOwners[id.RunID]
	_, fenced := c.fencedOwners[owner]
	if err == nil && live && !fenced && !c.closed {
		c.putDigestLocked(id, sum)
	}
	close(work.done)
	c.mu.Unlock()
	return sum, err
}
func hashSnapshotBlocks(ctx context.Context, data []byte) ([sha256.Size]byte, error) {
	digest := sha256.New()
	for off := 0; off < len(data); {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, err
		}
		n := min(snapshotHashBlock, len(data)-off)
		if _, err := digest.Write(data[off : off+n]); err != nil {
			return [sha256.Size]byte{}, err
		}
		off += n
	}
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(sum[:0]))
	return sum, nil
}
