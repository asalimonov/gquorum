//go:build !functional

package raftstate

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestSnapshotSliceDecoderPreservesStrictRecordSemantics(t *testing.T) {
	t.Parallel()
	encoded, err := EncodeSnapshot(codecSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		encoded,
		bytes.ReplaceAll(encoded, []byte(`"application"`), []byte(`"\u0041PPLICATION"`)),
		bytes.ReplaceAll(encoded, []byte(`"schema"`), []byte(`"ſCHEMA"`)),
		bytes.ReplaceAll(encoded, []byte(`"application":"ZW50aXJlIGFwcGxpY2F0aW9u"`), []byte(`"application":[1,2,3]`)),
		bytes.ReplaceAll(encoded, []byte(`"schema":2`), []byte(`"schema":1,"schema":2`)),
		bytes.ReplaceAll(encoded, []byte(`"authority":{`), []byte(`"authority":{"unknown":0,`)),
		bytes.ReplaceAll(encoded, []byte(`"grant":{`), []byte(`"grant":{"unknown":0,`)),
		bytes.ReplaceAll(encoded, []byte(`"action":{`), []byte(`"action":{"unknown":0,`)),
		bytes.ReplaceAll(encoded, []byte(`"schema":2`), []byte(`"extra":{"ignored":["escaped\"}",{}]},"schema":2`)),
		[]byte(`{"schema":2,"authority":{"install_index":0},"authority":null,"application":"YQ=="}`),
		[]byte(`{"application":"YQ==","application":"Yg==","application":null}`),
		[]byte(`{"cluster_id":"a\\\"}[]b","schema":2,"application":[12,34]}`),
		[]byte(`{"schema":2} {}`), []byte(`null`), []byte(`[]`), []byte(`{}`), []byte(`{"application":{}}`),
	}
	for _, data := range cases {
		compareSnapshotDecoders(t, data)
	}
}

func compareSnapshotDecoders(t testing.TB, data []byte) {
	t.Helper()
	var previous, current Snapshot
	previousErr, currentErr := decode(data, &previous), decodeSnapshotEnvelope(data, &current)
	if (previousErr == nil) != (currentErr == nil) {
		t.Fatalf("decoder acceptance changed: previous=%v current=%v input=%q", previousErr, currentErr, data)
	}
	if currentErr == nil && !reflect.DeepEqual(previous, current) {
		t.Fatal("decoded state changed")
	}
}

func FuzzSnapshotSliceDecoder(f *testing.F) {
	data, err := EncodeSnapshot(codecSnapshot())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add([]byte(`{"application":[1,2],"authority":null}`))
	f.Add([]byte(`{"application":"\\\"}","schema":2}`))
	f.Fuzz(func(t *testing.T, data []byte) { compareSnapshotDecoders(t, data) })
}

func benchmarkSnapshotSliceDecode(b *testing.B, data []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := DecodeSnapshot(data, "cluster", "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSnapshotDecodeAllocatesApplicationNotEncodedBuffer(t *testing.T) {
	t.Parallel()
	snapshot := codecSnapshot()
	snapshot.Application = bytes.Repeat([]byte{0x7e}, 4<<20)
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	result := testing.Benchmark(func(b *testing.B) { benchmarkSnapshotSliceDecode(b, data) })
	// Base64 output allocation plus small authority/identity fields and allocator
	// size-class rounding. A full encoded decoder buffer cannot fit this bound.
	limit := int64(len(snapshot.Application) + (256 << 10))
	if result.AllocedBytesPerOp() > limit {
		t.Fatalf("decoder-sized allocation: %d bytes/op, maximum %d", result.AllocedBytesPerOp(), limit)
	}
	t.Logf("encoded=%d application=%d allocated=%d bytes/op", len(data), len(snapshot.Application), result.AllocedBytesPerOp())
}

func BenchmarkSnapshotSliceDecode(b *testing.B) {
	snapshot := codecSnapshot()
	snapshot.Application = bytes.Repeat([]byte{0x7e}, 4<<20)
	data, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkSnapshotSliceDecode(b, data)
}

func BenchmarkSnapshotEnvelopeDecoders(b *testing.B) {
	snapshot := codecSnapshot()
	snapshot.Application = bytes.Repeat([]byte{0x7e}, 4<<20)
	data, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("slice", func(b *testing.B) { benchmarkSnapshotSliceDecode(b, data) })
	b.Run("previous_buffered", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			var decoded Snapshot
			if err := decode(data, &decoded); err != nil {
				b.Fatal(err)
			}
			if err := decoded.validate(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
