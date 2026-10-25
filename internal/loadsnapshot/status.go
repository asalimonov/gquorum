package loadsnapshot

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/load/snapshot"
)

type snapshotStatusRow struct {
	id     uint64
	status snapshot.Status
}

// List returns bounded local observations without waiting for native owners.
func (c *Coordinator) List(q snapshot.Query) snapshot.Page {
	limit := q.Limit
	if limit <= 0 || limit > snapshotPageLimit {
		limit = snapshotPageLimit
	}
	c.mu.Lock()
	page := snapshot.Page{Incoming: c.incomingCount, Outgoing: c.outgoingCount,
		ReceiveBytes: c.receiveBytes, SendBytes: c.sendBytes, NextCursor: q.Cursor}
	rows := make([]snapshotStatusRow, 0, len(c.history)+len(c.incoming))
	for i, status := range c.history {
		rows = append(rows, snapshotStatusRow{c.historyIDs[i], status})
	}
	active := make([]*snapshotReceive, 0, len(c.incoming))
	for _, tr := range c.incoming {
		active = append(active, tr)
	}
	c.mu.Unlock()
	for _, tr := range active {
		tr.mu.Lock()
		rows = append(rows, snapshotStatusRow{tr.statusID, tr.statusLocked()})
		tr.mu.Unlock()
	}
	slices.SortFunc(rows, func(a, b snapshotStatusRow) int { return cmp.Compare(a.id, b.id) })
	for _, row := range rows {
		if row.id <= q.Cursor {
			continue
		}
		if q.GroupID != "" && row.status.Key.To.Replica.GroupID != q.GroupID {
			page.NextCursor = row.id
			continue
		}
		if len(page.Items) == limit {
			page.Truncated = true
			break
		}
		page.Items = append(page.Items, row.status)
		page.NextCursor = row.id
	}
	return page
}
func (tr *snapshotReceive) statusLocked() snapshot.Status {
	status := snapshot.Status{Key: tr.key, Direction: "incoming", SnapshotIndex: tr.header.Snapshot.Metadata.Index,
		SnapshotTerm: tr.header.Snapshot.Metadata.Term, TotalBytes: tr.manifest.TotalBytes, VerifiedBytes: tr.next,
		StartedAt: tr.started, LastProgressAt: tr.progress, Deadline: tr.deadline, Delivery: tr.state,
		ReceiverOutcome: tr.receiverOutcome, ReservedBytes: tr.manifest.TotalBytes}
	err := tr.failed
	if tr.completion != nil && tr.completion.err != nil {
		err = tr.completion.err
	}
	if tr.cleanupErr != nil {
		err = tr.cleanupErr
	}
	if err != nil {
		if detail, ok := err.(*snapshot.TransferError); ok {
			status.ErrorReason = detail.Reason
		} else {
			status.ErrorReason = snapshot.ReasonIngressFailed
		}
	}
	return status
}
func (c *Coordinator) observe(status snapshot.Status) {
	c.mu.Lock()
	c.appendHistoryLocked(status)
	c.mu.Unlock()
}

func (c *Coordinator) observeTransfer(id uint64, status snapshot.Status) {
	c.mu.Lock()
	c.appendHistoryAtLocked(id, status)
	c.mu.Unlock()
}
