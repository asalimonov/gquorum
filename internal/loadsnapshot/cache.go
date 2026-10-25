//nolint:unused // Bounded caches are connected by finalization/digest tasks 8–9.
package loadsnapshot

import (
	"bytes"
	"crypto/sha256"
	"math"

	"github.com/asalimonov/gquorum/load/snapshot"
)

// putTerminalLocked is best-effort replay metadata, never an admission permit.
func (c *Coordinator) putTerminalLocked(key snapshot.Key, record snapshotTerminal) {
	if record.ack.State != snapshot.DeliveryAccepted && record.ack.State != snapshot.DeliveryNotNeeded {
		return
	}
	if len(record.manifest.NativeHeader) > snapshot.MaxNativeHeaderBytes || c.terminalSequence == math.MaxUint64 {
		return
	}
	limit := max(snapshotTerminalMinimum, snapshotTerminalScale*c.options.MaxIncoming)
	if _, exists := c.terminal[key]; !exists && len(c.terminal) >= limit {
		var oldest snapshot.Key
		sequence := uint64(math.MaxUint64)
		for k, v := range c.terminal {
			if v.sequence < sequence {
				oldest, sequence = k, v.sequence
			}
		}
		delete(c.terminal, oldest)
	}
	c.terminalSequence++
	record.sequence = c.terminalSequence
	record.manifest.NativeHeader = bytes.Clone(record.manifest.NativeHeader)
	c.terminal[key] = &record
}

func (c *Coordinator) appendHistoryLocked(status snapshot.Status) {
	if c.historySequence == math.MaxUint64 {
		return
	}
	c.historySequence++
	id := c.historySequence
	c.appendHistoryAtLocked(id, status)
}
func (c *Coordinator) appendHistoryAtLocked(id uint64, status snapshot.Status) {
	status.ErrorReason = status.ErrorReason[:min(len(status.ErrorReason), snapshotTextLimit)]
	if len(c.history) == snapshotHistoryLimit {
		copy(c.history, c.history[1:])
		copy(c.historyIDs, c.historyIDs[1:])
		c.historyIDs[len(c.historyIDs)-1] = id
		c.history[len(c.history)-1] = status
	} else {
		c.history = append(c.history, status)
		c.historyIDs = append(c.historyIDs, id)
	}
}

func (c *Coordinator) putDigestLocked(id SourceSnapshotID, digest [sha256.Size]byte) {
	order := c.digestOrder[id.RunID]
	if _, exists := c.digests[id]; exists {
		c.digests[id] = digest
		return
	}
	if len(order) == snapshotDigestsPerRun {
		delete(c.digests, order[0])
		order = order[1:]
	}
	c.digests[id] = digest
	c.digestOrder[id.RunID] = append(order, id)
}
func (c *Coordinator) clearDigestsLocked(runID string) {
	for _, id := range c.digestOrder[runID] {
		delete(c.digests, id)
	}
	delete(c.digestOrder, runID)
}

func (c *Coordinator) nextObservationLocked() (uint64, error) {
	if c.historySequence == math.MaxUint64 {
		return 0, transferError(snapshot.ReasonTransferConflict)
	}
	c.historySequence++
	return c.historySequence, nil
}
