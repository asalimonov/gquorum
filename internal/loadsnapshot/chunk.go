package loadsnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
)

// WriteSnapshot verifies one bounded contiguous chunk or an exact duplicate.
func (c *Coordinator) WriteSnapshot(ctx context.Context, chunk snapshot.Chunk) (snapshot.Ack, error) {
	op, err := c.beginOperation(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer op.close()
	tr, _ := c.findTransfer(chunk.Session.Key)
	if tr == nil {
		return snapshot.Ack{}, transferError(snapshot.ReasonRestartRequired)
	}
	attachment, err := tr.pin(chunk.Session)
	if err != nil {
		return snapshot.Ack{}, err
	}
	defer tr.unpin()
	work, release := bindAttachment(op.ctx, attachment)
	defer release()
	if err := c.revalidateSnapshotPeer(work, tr); err != nil {
		return snapshot.Ack{}, err
	}
	if err := tr.obsolete(); err != nil {
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	ack, err := c.writeChunk(work, tr, chunk)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.failReceive(tr, err)
	}
	return ack, err
}
func (c *Coordinator) writeChunk(ctx context.Context, tr *snapshotReceive, chunk snapshot.Chunk) (snapshot.Ack, error) {
	tr.mu.Lock()
	err := tr.checkChunkLocked(ctx, chunk)
	duplicate := chunk.Offset < tr.next
	tr.mu.Unlock()
	if err != nil {
		return snapshot.Ack{}, err
	}
	// pin serializes storage/hash access, while the metadata mutex stays free
	// so detach/resume/abort can cancel and join this operation.
	if sha256.Sum256(chunk.Data) != chunk.SHA256 {
		return snapshot.Ack{}, transferError(snapshot.ReasonChecksum)
	}
	if duplicate {
		previous := make([]byte, len(chunk.Data))
		if _, err := tr.staging.ReadAt(ctx, previous, chunk.Offset); err != nil {
			return snapshot.Ack{}, err
		}
		if !bytes.Equal(previous, chunk.Data) {
			return snapshot.Ack{}, transferError(snapshot.ReasonTransferConflict)
		}
		tr.mu.Lock()
		defer tr.mu.Unlock()
		if tr.failed != nil {
			return snapshot.Ack{}, tr.failed
		}
		return tr.ackLocked(time.Now()), nil
	}
	if err := ctx.Err(); err != nil {
		return snapshot.Ack{}, err
	}
	if err := tr.staging.Append(ctx, chunk.Offset, chunk.Data); err != nil {
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	if _, err := tr.digest.Write(chunk.Data); err != nil {
		c.failReceive(tr, err)
		return snapshot.Ack{}, err
	}
	tr.mu.Lock()
	// A successful append remains verified after attachment cancellation. Resume
	// waits for unpin and sees this exact offset, unless the transfer expired.
	now := time.Now()
	expired := tr.expiredLocked(now)
	tr.next += uint64(len(chunk.Data))
	if expired == nil {
		tr.progress = now
	}
	ack, failed := tr.ackLocked(now), tr.failed
	tr.mu.Unlock()
	if expired != nil {
		c.failReceive(tr, expired)
		return snapshot.Ack{}, expired
	}
	if failed != nil {
		return snapshot.Ack{}, failed
	}
	return ack, nil
}

func (tr *snapshotReceive) checkChunkLocked(ctx context.Context, chunk snapshot.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tr.failed != nil {
		return tr.failed
	}
	if tr.state != snapshot.DeliveryReceiving {
		return transferError(snapshot.ReasonGroupBusy)
	}
	if err := tr.expiredLocked(time.Now()); err != nil {
		return err
	}
	return validateChunk(tr, chunk)
}
func validateChunk(tr *snapshotReceive, chunk snapshot.Chunk) error {
	total, off, n, size := tr.manifest.TotalBytes, chunk.Offset, uint64(len(chunk.Data)), uint64(tr.chunkBytes)
	if size == 0 || n == 0 || off > total || n > total-off || off > tr.next ||
		off%size != 0 || chunk.Ordinal != off/size {
		return transferError(snapshot.ReasonMalformed)
	}
	if off < tr.next && n > tr.next-off || n != min(size, total-off) {
		return transferError(snapshot.ReasonMalformed)
	}
	return nil
}
