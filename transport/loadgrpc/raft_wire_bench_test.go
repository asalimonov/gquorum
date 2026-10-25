//go:build !functional

package loadgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// JSON framing below is the former production splitter, kept only to measure
// full preflight/splitting and the outer protobuf cost on identical inputs.
func legacySplitBatch(batch load.RaftBatch, limit int) ([]*loadpb.Envelope, error) {
	var frames []*loadpb.Envelope
	for start := 0; start < len(batch.Messages); {
		chunk, err := legacyFindBatchChunk(batch, start, limit)
		if err != nil {
			return nil, err
		}
		frames = append(frames, chunk.frame)
		start += chunk.count
	}
	return frames, nil
}

type legacyBatchChunk struct {
	frame *loadpb.Envelope
	count int
}

type legacyBatchSearch struct {
	lo, hi int
	best   legacyBatchChunk
}

// Preflight every message before sending. Binary search keeps the original
// order and never truncates an oversized message.
func legacyFindBatchChunk(batch load.RaftBatch, start, limit int) (legacyBatchChunk, error) {
	search := legacyBatchSearch{lo: 1, hi: len(batch.Messages) - start}
	for search.lo <= search.hi {
		if err := search.try(batch, start, limit); err != nil {
			return legacyBatchChunk{}, err
		}
	}
	if search.best.frame == nil {
		return legacyBatchChunk{}, fault.New(fault.Overloaded, "single Raft message exceeds load frame limit", nil)
	}
	return search.best, nil
}

func legacyEncodeBatchChunk(batch load.RaftBatch, start, count, limit int) (legacyBatchChunk, error) {
	part := batch
	part.Messages = batch.Messages[start : start+count]
	frame, err := envelope(part, limit, limit)
	return legacyBatchChunk{frame: frame, count: count}, err
}

func (s *legacyBatchSearch) try(batch load.RaftBatch, start, limit int) error {
	count := s.lo + (s.hi-s.lo)/2
	chunk, err := legacyEncodeBatchChunk(batch, start, count, limit)
	if err == nil {
		s.best = chunk
		s.lo = count + 1
	} else if errors.Is(err, fault.ErrOverloaded) {
		s.hi = count - 1
	} else {
		return err
	}
	return nil
}

type benchmarkRaftCodec struct {
	name   string
	encode func(load.RaftBatch, int) ([][]byte, error)
	decode func([][]byte, int) (load.RaftBatch, error)
}

func benchmarkBinaryEncode(batch load.RaftBatch, limit int) ([][]byte, error) {
	frames, err := encodeRaftFrames(batch, limit)
	if err != nil {
		return nil, err
	}
	wire := make([][]byte, len(frames))
	for i, f := range frames {
		wire[i], err = proto.Marshal(f)
		if err != nil {
			return nil, err
		}
	}
	return wire, nil
}
func benchmarkJSONEncode(batch load.RaftBatch, limit int) ([][]byte, error) {
	if err := validateBatch(batch); err != nil {
		return nil, err
	}
	frames, err := legacySplitBatch(batch, limit)
	if err != nil {
		return nil, err
	}
	wire := make([][]byte, len(frames))
	for i, f := range frames {
		wire[i], err = proto.Marshal(f)
		if err != nil {
			return nil, err
		}
	}
	return wire, nil
}
func benchmarkBinaryDecode(wire [][]byte, limit int) (load.RaftBatch, error) {
	var result load.RaftBatch
	for _, data := range wire {
		frame := new(loadpb.RaftFrame)
		if err := proto.Unmarshal(data, frame); err != nil {
			return result, err
		}
		part, err := decodeRaftHeader(frame, limit)
		if err != nil {
			return result, err
		}
		if err := decodeRaftMessages(context.Background(), frame, &part); err != nil {
			return result, err
		}
		result.From, result.To, result.DeliveryID = part.From, part.To, part.DeliveryID
		result.Messages = append(result.Messages, part.Messages...)
	}
	return result, nil
}
func benchmarkJSONDecode(wire [][]byte, limit int) (load.RaftBatch, error) {
	var result load.RaftBatch
	for _, data := range wire {
		frame := new(loadpb.Envelope)
		if err := proto.Unmarshal(data, frame); err != nil {
			return result, err
		}
		var part load.RaftBatch
		if err := decodeEnvelope(frame, &part, limit, limit); err != nil {
			return result, err
		}
		result.From, result.To, result.DeliveryID = part.From, part.To, part.DeliveryID
		result.Messages = append(result.Messages, part.Messages...)
	}
	return result, nil
}

func BenchmarkRaftFrameCodecs(b *testing.B) {
	cases := []struct {
		name        string
		kind        raftpb.MessageType
		count, data int
	}{
		{"heartbeat", raftpb.MsgHeartbeat, 1, 0},
		{"app-response", raftpb.MsgAppResp, 1, 0},
		{"read-index", raftpb.MsgReadIndex, 1, 24},
		{"append", raftpb.MsgApp, 1, 256},
		{"batch", raftpb.MsgApp, 64, 256},
	}
	codecs := []benchmarkRaftCodec{{"json", benchmarkJSONEncode, benchmarkJSONDecode}, {"binary", benchmarkBinaryEncode, benchmarkBinaryDecode}}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			batch := binaryTestBatch(tc.count)
			for i := range batch.Messages {
				msg := &batch.Messages[i]
				msg.Type = tc.kind
				msg.Context = nil
				msg.Entries = nil
				switch tc.kind {
				case raftpb.MsgApp:
					msg.Entries = []raftpb.Entry{{Index: uint64(i + 1), Term: 3, Data: make([]byte, tc.data)}}
				case raftpb.MsgReadIndex:
					msg.Context = make([]byte, tc.data)
				}
			}
			limit := 4096
			for _, codec := range codecs {
				b.Run(codec.name, func(b *testing.B) {
					wire, err := codec.encode(batch, limit)
					if err != nil {
						b.Fatal(err)
					}
					got, err := codec.decode(wire, limit)
					if err != nil || got.From != batch.From || got.To != batch.To || got.DeliveryID != batch.DeliveryID || !sameRaftMessages(got.Messages, batch.Messages) {
						b.Fatal("codec changed benchmark data", err)
					}
					size := 0
					for _, data := range wire {
						size += len(data)
					}
					b.Run("encode", func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if _, err := codec.encode(batch, limit); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(size), "wire-B/op")
						b.ReportMetric(float64(len(wire)), "frames/op")
					})
					b.Run("decode", func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if _, err := codec.decode(wire, limit); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(size), "wire-B/op")
						b.ReportMetric(float64(len(wire)), "frames/op")
					})
				})
			}
		})
	}
}
