//nolint:unused // The additive codecs are connected by plan tasks 10 and 11.
package loadgrpc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotWireVersion = 1
	snapshotTextLimit   = 512
	maxWireVarintBytes  = 10
	varintPayloadBits   = 7
	varintBoundary      = 1 << varintPayloadBits
	snapshotScalarCount = 3 // attachment, ordinal, offset
)

func encodeSnapshotOpen(
	send snapshot.Send, remaining, progress time.Duration, resume bool,
) (*loadpb.SnapshotOpen, error) {
	m := send.Manifest
	if remaining <= 0 || progress <= 0 || uint64(len(send.Data)) != m.TotalBytes {
		return nil, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if err := validateSnapshotManifest(m); err != nil {
		return nil, err
	}
	return &loadpb.SnapshotOpen{
		Version: snapshotWireVersion, From: encodeRaftPeer(m.Key.From), To: encodeRaftPeer(m.Key.To),
		DeliveryId: m.Key.DeliveryID, NativeHeader: bytes.Clone(m.NativeHeader),
		TotalBytes: m.TotalBytes, Sha256: bytes.Clone(m.SHA256[:]), ChunkBytes: m.ChunkBytes,
		RemainingTotalNanos: uint64(remaining), ProgressTimeoutNanos: uint64(progress), Resume: resume,
	}, nil
}

func decodeSnapshotOpen(open *loadpb.SnapshotOpen, limit int) (snapshot.BeginRequest, error) {
	if open == nil || limit <= 0 {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if open.Version != snapshotWireVersion {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonUnsupported, nil)
	}
	frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: open}}
	if size := proto.Size(frame); size > 0 && size > limit {
		return snapshot.BeginRequest{}, snapshotWireLimit(snapshot.ReasonFrameLimit, uint64(size), uint64(limit))
	}
	from, err := decodeRaftPeer(open.From)
	if err != nil {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonMalformed, err)
	}
	to, err := decodeRaftPeer(open.To)
	if err != nil {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonMalformed, err)
	}
	if len(open.Sha256) != sha256.Size || !validSnapshotDuration(open.RemainingTotalNanos) ||
		!validSnapshotDuration(open.ProgressTimeoutNanos) {
		return snapshot.BeginRequest{}, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	manifest := snapshot.Manifest{Key: snapshot.Key{From: from, To: to, DeliveryID: open.DeliveryId},
		NativeHeader: open.NativeHeader, TotalBytes: open.TotalBytes, ChunkBytes: open.ChunkBytes}
	copy(manifest.SHA256[:], open.Sha256)
	if err := validateSnapshotManifest(manifest); err != nil {
		return snapshot.BeginRequest{}, err
	}
	manifest.NativeHeader = bytes.Clone(manifest.NativeHeader)
	return snapshot.BeginRequest{Manifest: manifest, RemainingTotal: snapshotDuration(open.RemainingTotalNanos),
		ProgressTimeout: snapshotDuration(open.ProgressTimeoutNanos), Resume: open.Resume}, nil
}

func validateSnapshotManifest(m snapshot.Manifest) error {
	if m.Key.DeliveryID == 0 || m.TotalBytes == 0 || m.ChunkBytes == 0 ||
		!validRaftPeer(m.Key.From) || !validRaftPeer(m.Key.To) {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if m.TotalBytes > uint64(math.MaxInt) {
		return snapshotWireLimit(snapshot.ReasonSizeLimit, m.TotalBytes, uint64(math.MaxInt))
	}
	if m.Key.From.Node.ClusterID != m.Key.To.Node.ClusterID ||
		m.Key.From.Replica.GroupID != m.Key.To.Replica.GroupID ||
		m.Key.From.Replica.ReplicaID == m.Key.To.Replica.ReplicaID {
		return snapshotWireError(snapshot.ReasonIdentity, nil)
	}
	if len(m.NativeHeader) > snapshot.MaxNativeHeaderBytes {
		return snapshotWireLimit(snapshot.ReasonFrameLimit, uint64(len(m.NativeHeader)), snapshot.MaxNativeHeaderBytes)
	}
	if len(m.NativeHeader) == 0 {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	var header raftpb.Message
	if err := header.Unmarshal(m.NativeHeader); err != nil {
		return snapshotWireError(snapshot.ReasonMalformed, err)
	}
	if header.Type != raftpb.MsgSnap || header.Snapshot == nil || header.Term == 0 ||
		header.Snapshot.Metadata.Index == 0 || header.Snapshot.Metadata.Term == 0 ||
		header.Snapshot.Metadata.Term > header.Term || len(header.Snapshot.Data) != 0 ||
		len(header.Entries) != 0 || len(header.Responses) != 0 {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if header.From != m.Key.From.Replica.ReplicaID || header.To != m.Key.To.Replica.ReplicaID {
		return snapshotWireError(snapshot.ReasonIdentity, nil)
	}
	return validateSnapshotConfState(header.Snapshot.Metadata.ConfState)
}

func validateSnapshotConfState(cs raftpb.ConfState) error {
	if cs.AutoLeave || len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 {
		return snapshotWireError(snapshot.ReasonUnsupported, nil)
	}
	if len(cs.Voters) == 0 {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	seen := make(map[uint64]bool)
	for _, ids := range [][]uint64{cs.Voters, cs.Learners} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return snapshotWireError(snapshot.ReasonMalformed, nil)
			}
			seen[id] = true
		}
	}
	return nil
}

func encodeSnapshotAck(ack snapshot.Ack, err error) (*loadpb.SnapshotAck, error) {
	if ack.Session.Key.DeliveryID == 0 || ack.RemainingTotal < 0 || ack.ProgressTimeout < 0 {
		return nil, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	reply := &loadpb.SnapshotAck{DeliveryId: ack.Session.Key.DeliveryID, Attachment: ack.Session.Attachment,
		NextOffset: ack.NextOffset, ChunkBytes: ack.ChunkBytes, State: string(ack.State),
		RestartNeeded: ack.RestartNeeded, RemainingTotalNanos: uint64(ack.RemainingTotal),
		ProgressTimeoutNanos: uint64(ack.ProgressTimeout), ReceiverRunId: ack.ReceiverRunID}
	if err != nil {
		reply.State = string(snapshot.DeliveryFailed)
		reply.Failure = encodeSnapshotFailure(err)
		reply.RestartNeeded = reply.Failure.Reason == snapshot.ReasonRestartRequired
	}
	if !validSnapshotAckShape(reply) {
		return nil, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	return reply, nil
}

// decodeSnapshotAck recovers only the wire delivery ID and attachment. The
// stream owner must verify that ID and bind From/To to its exact request key;
// the wire acknowledgement does not carry or independently authorize peers.
func decodeSnapshotAck(reply *loadpb.SnapshotAck) (snapshot.Ack, error) {
	if !validSnapshotAckShape(reply) {
		return snapshot.Ack{}, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	ack := snapshot.Ack{Session: snapshot.Session{Key: snapshot.Key{DeliveryID: reply.DeliveryId},
		Attachment: reply.Attachment}, NextOffset: reply.NextOffset, ChunkBytes: reply.ChunkBytes,
		State: snapshot.DeliveryState(reply.State), RestartNeeded: reply.RestartNeeded,
		RemainingTotal:  snapshotDuration(reply.RemainingTotalNanos),
		ProgressTimeout: snapshotDuration(reply.ProgressTimeoutNanos),
		ReceiverRunID:   reply.ReceiverRunId}
	if f := reply.Failure; f != nil {
		return ack, &snapshot.TransferError{Reason: f.Reason, Size: f.Size, Limit: f.Limit,
			Cause: fault.New(fault.Code(f.Code), f.Message, nil)}
	}
	return ack, nil
}

func validSnapshotAckShape(reply *loadpb.SnapshotAck) bool {
	if reply == nil || reply.DeliveryId == 0 || reply.RemainingTotalNanos > math.MaxInt64 ||
		reply.ProgressTimeoutNanos > math.MaxInt64 || !snapshotText(reply.ReceiverRunId, true) {
		return false
	}
	if failure := reply.Failure; failure != nil {
		return reply.RestartNeeded == (failure.Reason == snapshot.ReasonRestartRequired) &&
			reply.State == string(snapshot.DeliveryFailed) && snapshotText(failure.Code, false) &&
			snapshotText(failure.Reason, false) && snapshotText(failure.Message, true)
	}
	if reply.RestartNeeded || !snapshotText(reply.ReceiverRunId, false) ||
		!validSnapshotDuration(reply.RemainingTotalNanos) || !validSnapshotDuration(reply.ProgressTimeoutNanos) {
		return false
	}
	switch snapshot.DeliveryState(reply.State) {
	case snapshot.DeliveryNotNeeded:
		return reply.NextOffset == 0
	case snapshot.DeliveryReceiving, snapshot.DeliveryVerifying, snapshot.DeliveryHandingOff, snapshot.DeliveryAccepted:
		return reply.Attachment != 0 && reply.ChunkBytes != 0
	default:
		return false
	}
}

func encodeSnapshotFailure(err error) *loadpb.SnapshotFailure {
	failure := &loadpb.SnapshotFailure{Code: string(fault.Unavailable), Reason: snapshot.ReasonIngressFailed,
		Message: snapshotBoundedText(err.Error())}
	var classified *fault.Error
	if errors.As(err, &classified) {
		if classified.Code != "" {
			failure.Code = snapshotBoundedText(string(classified.Code))
		}
	}
	var detail *snapshot.TransferError
	if errors.As(err, &detail) {
		if detail.Reason != "" {
			failure.Reason = snapshotBoundedText(detail.Reason)
		}
		failure.Size, failure.Limit = detail.Size, detail.Limit
	}
	return failure
}

// snapshotDuration is used only after wire shape validation; invalid values stay zero.
func snapshotDuration(nanos uint64) time.Duration {
	if nanos > math.MaxInt64 {
		return 0
	}
	return time.Duration(nanos)
}

func validSnapshotDuration(nanos uint64) bool { return nanos > 0 && nanos <= math.MaxInt64 }

func snapshotText(text string, empty bool) bool {
	return len(text) <= snapshotTextLimit && utf8.ValidString(text) && !strings.ContainsRune(text, 0) &&
		(empty || strings.TrimSpace(text) != "")
}

func snapshotBoundedText(text string) string {
	bounded := text[:min(len(text), snapshotTextLimit)]
	return strings.ReplaceAll(strings.ToValidUTF8(bounded, "?"), "\x00", "?")
}

func snapshotWireError(reason string, cause error) error {
	return &snapshot.TransferError{Reason: reason, Cause: cause}
}

func snapshotWireLimit(reason string, size, limit uint64) error {
	return &snapshot.TransferError{Reason: reason, Size: size, Limit: limit}
}

func snapshotChunkCapacity(frameLimit, requested int) (int, error) {
	if frameLimit <= 0 || requested <= 0 {
		return 0, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	limit := uint64(frameLimit)
	low, high := uint64(0), min(uint64(requested), limit, uint64(math.MaxUint32))
	for low < high {
		mid := low + (high-low+1)/2
		if snapshotChunkFrameBytes(mid) <= limit {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if low == 0 {
		return 0, snapshotWireLimit(snapshot.ReasonFrameLimit, snapshotChunkFrameBytes(1), limit)
	}
	if low > uint64(math.MaxInt) {
		return 0, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	return int(low), nil
}

func snapshotChunkFrameBytes(payload uint64) uint64 {
	// All field tags currently fit one byte. Account for three maximal uint64
	// scalars, the data length, SHA-256 and the enclosing oneof message length.
	const scalars = snapshotScalarCount * (1 + maxWireVarintBytes)
	const digest = 1 + 1 + sha256.Size
	inner := scalars + 1 + snapshotVarintBytes(payload) + payload + digest
	return 1 + snapshotVarintBytes(inner) + inner
}

func snapshotVarintBytes(value uint64) uint64 {
	n, remaining := uint64(1), value
	for remaining >= varintBoundary {
		remaining >>= varintPayloadBits
		n++
	}
	return n
}
