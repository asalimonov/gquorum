//go:build !functional

package loadgrpc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func snapshotWireSend(t *testing.T) snapshot.Send {
	t.Helper()
	batch := binaryTestBatch(0)
	header := raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 3,
		Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
			Index: 50, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}}}}
	raw, err := header.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x5a, 0xff}, 256)
	return snapshot.Send{Manifest: snapshot.Manifest{
		Key:          snapshot.Key{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID},
		NativeHeader: raw, TotalBytes: uint64(len(data)), SHA256: sha256.Sum256(data), ChunkBytes: 128}, Data: data}
}

func snapshotWireOpen(t *testing.T) *loadpb.SnapshotOpen {
	t.Helper()
	open, err := encodeSnapshotOpen(snapshotWireSend(t), time.Minute, time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	return open
}

func TestSnapshotWireOpenOwnershipAndFrameBoundary(t *testing.T) {
	t.Parallel()
	send := snapshotWireSend(t)
	wantHeader := bytes.Clone(send.Manifest.NativeHeader)
	open, err := encodeSnapshotOpen(send, time.Second, time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	// Remaining total may be shorter than the original idle policy on resume.
	frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Open{Open: open}}
	size := proto.Size(frame)
	got, err := decodeSnapshotOpen(open, size)
	if err != nil || !reflect.DeepEqual(got.Manifest, send.Manifest) || !got.Resume ||
		got.RemainingTotal != time.Second || got.ProgressTimeout != time.Minute {
		t.Fatal("wire identity/policy changed", got, err)
	}
	if _, err := decodeSnapshotOpen(open, size-1); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("enclosing frame overhead was omitted", err)
	}
	if size >= len(send.Data) {
		t.Fatal("Open unexpectedly materialized the payload")
	}
	send.Manifest.NativeHeader[0] ^= 0xff
	open.NativeHeader[0] ^= 0xff
	open.Sha256[0] ^= 0xff
	if !bytes.Equal(got.Manifest.NativeHeader, wantHeader) || got.Manifest.SHA256 != sha256.Sum256(send.Data) {
		t.Fatal("decoded header/digest alias mutable transport buffers")
	}
}

func TestSnapshotWireRejectsBadOpen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*loadpb.SnapshotOpen)
		want error
	}{
		{"version", func(o *loadpb.SnapshotOpen) { o.Version++ }, fault.ErrUnsupported},
		{"zero delivery", func(o *loadpb.SnapshotOpen) { o.DeliveryId = 0 }, fault.ErrInvalid},
		{"missing peer", func(o *loadpb.SnapshotOpen) { o.From = nil }, fault.ErrInvalid},
		{"foreign group", func(o *loadpb.SnapshotOpen) { o.To.GroupId = "other" }, fault.ErrIdentityMismatch},
		{"foreign cluster", func(o *loadpb.SnapshotOpen) { o.To.ClusterId = "other" }, fault.ErrIdentityMismatch},
		{"zero payload", func(o *loadpb.SnapshotOpen) { o.TotalBytes = 0 }, fault.ErrInvalid},
		{"overflow payload", func(o *loadpb.SnapshotOpen) { o.TotalBytes = math.MaxUint64 }, fault.ErrInvalid},
		{"digest", func(o *loadpb.SnapshotOpen) { o.Sha256 = []byte{1} }, fault.ErrInvalid},
		{"zero chunk", func(o *loadpb.SnapshotOpen) { o.ChunkBytes = 0 }, fault.ErrInvalid},
		{"zero duration", func(o *loadpb.SnapshotOpen) { o.RemainingTotalNanos = 0 }, fault.ErrInvalid},
		{"duration overflow", func(o *loadpb.SnapshotOpen) { o.ProgressTimeoutNanos = math.MaxUint64 }, fault.ErrInvalid},
		{"native garbage", func(o *loadpb.SnapshotOpen) { o.NativeHeader = []byte{0xff} }, fault.ErrInvalid},
		{"header too large", func(o *loadpb.SnapshotOpen) { o.NativeHeader = make([]byte, snapshot.MaxNativeHeaderBytes+1) }, fault.ErrInvalid},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := snapshotWireOpen(t)
			tt.edit(o)
			if _, err := decodeSnapshotOpen(o, 1<<20); !errors.Is(err, tt.want) {
				t.Fatal("wrong refusal", err)
			}
		})
	}
	if _, err := decodeSnapshotOpen(nil, 1024); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("nil Open accepted", err)
	}
}

func TestSnapshotWireNativeHeaderSafety(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*raftpb.Message)
		want error
	}{
		{"zero message term", func(m *raftpb.Message) { m.Term = 0 }, fault.ErrInvalid},
		{"zero index", func(m *raftpb.Message) { m.Snapshot.Metadata.Index = 0 }, fault.ErrInvalid},
		{"zero snapshot term", func(m *raftpb.Message) { m.Snapshot.Metadata.Term = 0 }, fault.ErrInvalid},
		{"future snapshot term", func(m *raftpb.Message) { m.Snapshot.Metadata.Term = m.Term + 1 }, fault.ErrInvalid},
		{"not snapshot", func(m *raftpb.Message) { m.Type = raftpb.MsgApp }, fault.ErrInvalid},
		{"missing metadata", func(m *raftpb.Message) { m.Snapshot = nil }, fault.ErrInvalid},
		{"embedded payload", func(m *raftpb.Message) { m.Snapshot.Data = []byte{1} }, fault.ErrInvalid},
		{"entry", func(m *raftpb.Message) { m.Entries = []raftpb.Entry{{Index: 1}} }, fault.ErrInvalid},
		{"response", func(m *raftpb.Message) { m.Responses = []raftpb.Message{{Type: raftpb.MsgAppResp}} }, fault.ErrInvalid},
		{"wrong recipient", func(m *raftpb.Message) { m.To = 3 }, fault.ErrIdentityMismatch},
		{"zero member", func(m *raftpb.Message) { m.Snapshot.Metadata.ConfState.Voters[0] = 0 }, fault.ErrInvalid},
		{"duplicate member", func(m *raftpb.Message) { m.Snapshot.Metadata.ConfState.Learners = []uint64{1} }, fault.ErrInvalid},
		{"no voters", func(m *raftpb.Message) { m.Snapshot.Metadata.ConfState.Voters = nil }, fault.ErrInvalid},
		{"joint membership", func(m *raftpb.Message) { m.Snapshot.Metadata.ConfState.AutoLeave = true }, fault.ErrUnsupported},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := snapshotWireOpen(t)
			var m raftpb.Message
			if err := m.Unmarshal(o.NativeHeader); err != nil {
				t.Fatal(err)
			}
			tt.edit(&m)
			var err error
			o.NativeHeader, err = m.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeSnapshotOpen(o, 1<<20); !errors.Is(err, tt.want) {
				t.Fatal("unsafe native header accepted", err)
			}
		})
	}
}

func TestSnapshotWireLargeDeclarationWithoutAllocation(t *testing.T) {
	t.Parallel()
	o := snapshotWireOpen(t)
	o.TotalBytes = 64 << 30
	got, err := decodeSnapshotOpen(o, 1024)
	if strconv.IntSize == 64 {
		if err != nil || got.Manifest.TotalBytes != o.TotalBytes {
			t.Fatal("qualification ceiling leaked into wire policy", got, err)
		}
	} else if !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("unrepresentable slice size accepted", err)
	}
}

func TestSnapshotWireChunkCapacity(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{72, 127, 128, 255, 1024, 16383, 16384, 1 << 20} {
		n, err := snapshotChunkCapacity(limit, limit)
		if err != nil {
			t.Fatal(limit, err)
		}
		frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{
			Attachment: math.MaxUint64, Ordinal: math.MaxUint64, Offset: math.MaxUint64,
			Data: make([]byte, n), Sha256: make([]byte, sha256.Size)}}}
		if proto.Size(frame) > limit {
			t.Fatal("computed payload exceeds real protobuf bound", limit, n, proto.Size(frame))
		}
		frame.GetData().Data = make([]byte, n+1)
		if proto.Size(frame) <= limit {
			t.Fatal("chunk capacity did not find exact boundary", limit, n)
		}
	}
	if _, err := snapshotChunkCapacity(71, 1); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("frame unable to carry one byte accepted", err)
	}
	if n, err := snapshotChunkCapacity(1024, 10); err != nil || n != 10 {
		t.Fatal("requested payload ignored", n, err)
	}
	if n, err := snapshotChunkCapacity(math.MaxInt, math.MaxInt); err != nil || uint64(n) > math.MaxUint32 {
		t.Fatal("large integer frame calculation overflowed", n, err)
	}
}

func wireTestAck() snapshot.Ack {
	return snapshot.Ack{Session: snapshot.Session{Key: snapshot.Key{DeliveryID: 37}, Attachment: 5},
		NextOffset: 512, ChunkBytes: 128, State: snapshot.DeliveryAccepted, RemainingTotal: time.Second,
		ProgressTimeout: time.Minute, ReceiverRunID: "receiver-run"}
}

func TestSnapshotWireAckAndFailureRoundTrip(t *testing.T) {
	t.Parallel()
	ack := wireTestAck()
	encoded, err := encodeSnapshotAck(ack, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeSnapshotAck(encoded)
	if err != nil || got != ack {
		t.Fatal("ack changed", got, err)
	}
	for _, reason := range []string{snapshot.ReasonSizeLimit, snapshot.ReasonFrameLimit, snapshot.ReasonChecksum,
		snapshot.ReasonGroupBusy, snapshot.ReasonCapacity, snapshot.ReasonRestartRequired, snapshot.ReasonUnsupported} {
		failure := &snapshot.TransferError{Reason: reason, Size: 900, Limit: 64}
		encoded, err := encodeSnapshotAck(ack, failure)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeSnapshotAck(encoded)
		var detail *snapshot.TransferError
		if !errors.As(err, &detail) || detail.Reason != reason || detail.Size != 900 || detail.Limit != 64 ||
			got.State != snapshot.DeliveryFailed || got.RestartNeeded != (reason == snapshot.ReasonRestartRequired) {
			t.Fatal("failure evidence changed", got, err)
		}
		if !errors.Is(err, failure.Unwrap()) {
			t.Fatal("fault classification changed", err)
		}
	}
	ack.Session.Attachment, ack.NextOffset, ack.ChunkBytes = 0, 0, 0
	ack.State = snapshot.DeliveryNotNeeded
	if _, err := encodeSnapshotAck(ack, nil); err != nil {
		t.Fatal("header-only native skip required payload acknowledgement", err)
	}
}

func TestSnapshotWireAckRefusalsAndBoundedError(t *testing.T) {
	t.Parallel()
	ack, err := encodeSnapshotAck(wireTestAck(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*loadpb.SnapshotAck){
		func(a *loadpb.SnapshotAck) { a.State = "future_success" },
		func(a *loadpb.SnapshotAck) { a.ReceiverRunId = "" },
		func(a *loadpb.SnapshotAck) { a.RestartNeeded = true },
		func(a *loadpb.SnapshotAck) { a.RemainingTotalNanos = math.MaxUint64 },
		func(a *loadpb.SnapshotAck) { a.Failure = &loadpb.SnapshotFailure{Code: "unavailable", Reason: "bad"} },
	} {
		bad := proto.Clone(ack).(*loadpb.SnapshotAck)
		mutate(bad)
		if _, err := decodeSnapshotAck(bad); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("ambiguous/invalid ack accepted", err)
		}
	}
	large := errors.New(strings.Repeat("界", snapshotTextLimit) + "\x00")
	encoded, err := encodeSnapshotAck(wireTestAck(), large)
	if err != nil || len(encoded.Failure.Message) > snapshotTextLimit || !utf8.ValidString(encoded.Failure.Message) {
		t.Fatal("error text was not bounded valid UTF-8", encoded, err)
	}
	if _, err := decodeSnapshotAck(nil); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("nil ack claimed evidence", err)
	}
}
