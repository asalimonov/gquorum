package loadsnapshot

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func validSnapshotPeer(peer load.Peer) bool {
	return peer.Node.MemberID != 0 && peer.Replica.ReplicaID != 0 &&
		boundedSnapshotText(peer.Node.NodeName) && boundedSnapshotText(peer.Node.ClusterID) &&
		boundedSnapshotText(peer.Node.InstanceID) && topology.ValidateLoadID(peer.Replica.GroupID) == nil
}
func boundedSnapshotText(value string) bool {
	return len(value) > 0 && len(value) <= snapshotTextLimit &&
		utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

type validatedManifest struct {
	manifest snapshot.Manifest
	header   raftpb.Message
}

func (c *Coordinator) validateManifest(b snapshot.BeginRequest) (validatedManifest, error) {
	if err := c.validateManifestPolicy(b); err != nil {
		return validatedManifest{}, err
	}
	m := b.Manifest
	m.NativeHeader = bytes.Clone(m.NativeHeader)
	h, err := decodeNativeSnapshotHeader(m)
	return validatedManifest{manifest: m, header: h}, err
}
func (c *Coordinator) validateManifestPolicy(b snapshot.BeginRequest) error {
	m := b.Manifest
	if m.Key.DeliveryID == 0 || m.TotalBytes == 0 || m.ChunkBytes == 0 ||
		b.RemainingTotal <= 0 || b.ProgressTimeout <= 0 {
		return transferError(snapshot.ReasonMalformed)
	}
	if m.Key.From.Node.ClusterID != m.Key.To.Node.ClusterID ||
		m.Key.From.Replica.GroupID != m.Key.To.Replica.GroupID ||
		m.Key.From.Replica.ReplicaID == m.Key.To.Replica.ReplicaID ||
		!validSnapshotPeer(m.Key.From) || !validSnapshotPeer(m.Key.To) {
		return transferError(snapshot.ReasonIdentity)
	}
	if len(m.NativeHeader) == 0 || len(m.NativeHeader) > snapshot.MaxNativeHeaderBytes {
		return transferError(snapshot.ReasonFrameLimit)
	}
	if m.TotalBytes > c.options.MaxSnapshotBytes {
		return &snapshot.TransferError{Reason: snapshot.ReasonSizeLimit,
			Size: m.TotalBytes, Limit: c.options.MaxSnapshotBytes}
	}
	return nil
}
func decodeNativeSnapshotHeader(m snapshot.Manifest) (raftpb.Message, error) {
	var h raftpb.Message
	if err := h.Unmarshal(m.NativeHeader); err != nil {
		return h, &snapshot.TransferError{Reason: snapshot.ReasonMalformed, Cause: err}
	}
	if h.Type != raftpb.MsgSnap || h.Snapshot == nil || h.Term == 0 || len(h.Entries) != 0 || len(h.Responses) != 0 {
		return h, transferError(snapshot.ReasonMalformed)
	}
	if h.From != m.Key.From.Replica.ReplicaID || h.To != m.Key.To.Replica.ReplicaID {
		return h, transferError(snapshot.ReasonIdentity)
	}
	ss := h.Snapshot
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || ss.Metadata.Term > h.Term || len(ss.Data) != 0 {
		return h, transferError(snapshot.ReasonMalformed)
	}
	return h, validateSnapshotMembership(ss.Metadata.ConfState)
}
func validateSnapshotMembership(cs raftpb.ConfState) error {
	if cs.AutoLeave || len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 {
		return transferError(snapshot.ReasonUnsupported)
	}
	if len(cs.Voters) == 0 {
		return transferError(snapshot.ReasonMalformed)
	}
	seen := make(map[uint64]bool)
	for _, ids := range [][]uint64{cs.Voters, cs.Learners} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return transferError(snapshot.ReasonMalformed)
			}
			seen[id] = true
		}
	}
	return nil
}
func (c *Coordinator) validateSnapshotPeer(ctx context.Context, key snapshot.Key) (snapshotOwner, error) {
	if err := ctx.Err(); err != nil {
		return snapshotOwner{}, err
	}
	if c.ports.Authorize == nil {
		return snapshotOwner{}, transferError(snapshot.ReasonUnsupported)
	}
	owner, err := c.ports.Authorize(ctx, key)
	if err != nil {
		return snapshotOwner{}, err
	}
	if owner == nil {
		return snapshotOwner{}, transferError(snapshot.ReasonIdentity)
	}
	exact := owner.Key()
	if exact.Peer != key.To || !validOwnerKey(exact) {
		return snapshotOwner{}, transferError(snapshot.ReasonIdentity)
	}
	if owner.Context() == nil || owner.Context().Err() != nil {
		return snapshotOwner{}, transferError(snapshot.ReasonStopped)
	}
	if err := ctx.Err(); err != nil {
		return snapshotOwner{}, err
	}
	c.mu.Lock()
	_, fenced := c.fencedOwners[exact]
	c.mu.Unlock()
	if fenced {
		return snapshotOwner{}, transferError(snapshot.ReasonStopped)
	}
	return snapshotOwner{key: exact, handle: owner}, nil
}
func (c *Coordinator) revalidateSnapshotPeer(ctx context.Context, tr *snapshotReceive) error {
	owner, err := c.validateSnapshotPeer(ctx, tr.key)
	if err != nil {
		return err
	}
	if owner.key != tr.owner.key {
		return transferError(snapshot.ReasonRestartRequired)
	}
	return nil
}
func sameManifest(a, b snapshot.Manifest) bool {
	return a.Key == b.Key && a.TotalBytes == b.TotalBytes && a.SHA256 == b.SHA256 && a.ChunkBytes == b.ChunkBytes &&
		bytes.Equal(a.NativeHeader, b.NativeHeader)
}
func snapshotPreflightError(err error) error {
	if errors.Is(err, fault.ErrOverloaded) {
		return &snapshot.TransferError{Reason: snapshot.ReasonGroupBusy, Cause: err}
	}
	return err
}

func (tr *snapshotReceive) expiredLocked(now time.Time) error {
	if !now.Before(tr.deadline) {
		return transferError(snapshot.ReasonTotalTimeout)
	}
	if tr.state == snapshot.DeliveryReceiving && tr.next < tr.manifest.TotalBytes &&
		now.Sub(tr.progress) >= tr.progressTimeout {
		return transferError(snapshot.ReasonProgressTimeout)
	}
	return nil
}
func (tr *snapshotReceive) obsolete() error {
	if tr.owner.handle.Context().Err() != nil {
		return transferError(snapshot.ReasonStopped)
	}
	bounds := tr.owner.handle.SnapshotBounds()
	if bounds.RunID != tr.owner.key.RunID {
		return transferError(snapshot.ReasonRestartRequired)
	}
	if tr.header.Term < bounds.Term || tr.header.Snapshot.Metadata.Index <= bounds.Commit {
		return transferError(snapshot.ReasonObsolete)
	}
	return nil
}

func snapshotTimedOut(err error) bool {
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) {
		return false
	}
	return detail.Reason == snapshot.ReasonProgressTimeout || detail.Reason == snapshot.ReasonTotalTimeout
}
