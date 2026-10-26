package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/rand/v2"

	"github.com/asalimonov/gquorum/fault"
)

const (
	syntheticMagic            = "GQSNAP01"
	syntheticHeaderBytes      = 16
	syntheticFramingAllowance = 64 << 10
	syntheticBlockBytes       = 64 << 10
)

type syntheticSpec struct {
	FramingBytes uint64 `json:"framing_bytes,omitempty"`
	Version      uint32 `json:"version"`
	Seed         uint64 `json:"seed"`
	EncodedBytes uint64 `json:"encoded_bytes"`
}
type syntheticRecord struct{ ID, Phase, Token string }
type syntheticState struct {
	Spec    syntheticSpec     `json:"spec"`
	Counter uint64            `json:"counter"`
	Records []syntheticRecord `json:"records"`
}
type syntheticObservation struct {
	Spec              syntheticSpec `json:"spec"`
	Counter           uint64        `json:"counter"`
	ApplicationBytes  uint64        `json:"application_bytes"`
	ApplicationSHA256 string        `json:"application_sha256"`
	Restores          uint64        `json:"restores"`
	AppliedIndex      uint64        `json:"applied_index"`
}

func (s syntheticSpec) validate() error {
	if s.Version != 1 || s.EncodedBytes <= syntheticFramingAllowance || s.EncodedBytes > uint64(math.MaxInt) || s.FramingBytes > syntheticFramingAllowance {
		return fault.ErrInvalid
	}
	return nil
}
func newSyntheticState(spec syntheticSpec) *syntheticState {
	seed := sha256.Sum256([]byte(hex.EncodeToString(binary.BigEndian.AppendUint64(nil, spec.Seed))))
	return &syntheticState{Spec: spec, Records: []syntheticRecord{
		{ID: "opaque-prepare", Phase: "prepared", Token: hex.EncodeToString(seed[:16])},
		{ID: "opaque-replay", Phase: "committed", Token: hex.EncodeToString(seed[16:])}}}
}
func syntheticSnapshot(ctx context.Context, state machineState) ([]byte, error) {
	spec := state.Synthetic.Spec
	if err := spec.validate(); err != nil {
		return nil, err
	}
	header, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	// Engine JSON/base64 adds about 4/3 plus authority framing. Leave explicit
	// headroom; qualification checks the actual encoded transfer manifest.
	raw := (spec.EncodedBytes - spec.framingBytes()) / 4 * 3
	minimum := uint64(syntheticHeaderBytes + len(header) + sha256.Size)
	if raw < minimum {
		return nil, fault.ErrInvalid
	}
	data := make([]byte, raw)
	copy(data, syntheticMagic)
	binary.BigEndian.PutUint64(data[8:16], uint64(len(header)))
	copy(data[syntheticHeaderBytes:], header)
	start := syntheticHeaderBytes + len(header)
	if err = fillSynthetic(ctx, data[start:len(data)-sha256.Size], state.Synthetic); err != nil {
		return nil, err
	}
	sum, err := hashSynthetic(ctx, data[:len(data)-sha256.Size])
	if err != nil {
		return nil, err
	}
	copy(data[len(data)-sha256.Size:], sum[:])
	return data, nil
}
func fillSynthetic(ctx context.Context, data []byte, state *syntheticState) error {
	random := syntheticRandom(state)
	for offset := 0; offset < len(data); {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(syntheticBlockBytes, len(data)-offset)
		if _, err := random.Read(data[offset : offset+n]); err != nil {
			return err
		}
		offset += n
	}
	return nil
}
func decodeSynthetic(ctx context.Context, data []byte) (machineState, error) {
	if len(data) < syntheticHeaderBytes+sha256.Size || !bytes.HasPrefix(data, []byte(syntheticMagic)) {
		return machineState{}, fault.ErrUnreadable
	}
	length := binary.BigEndian.Uint64(data[8:16])
	if length > uint64(len(data)-syntheticHeaderBytes-sha256.Size) {
		return machineState{}, fault.ErrUnreadable
	}
	end := uint64(syntheticHeaderBytes) + length
	var state machineState
	if err := decodeJSON(data[syntheticHeaderBytes:end], &state); err != nil {
		return state, err
	}
	if state.Synthetic == nil || state.Synthetic.Spec.validate() != nil || validateMachineState(state) != nil {
		return state, fault.ErrUnreadable
	}
	if uint64(len(data)) != (state.Synthetic.Spec.EncodedBytes-state.Synthetic.Spec.framingBytes())/4*3 {
		return state, fault.ErrUnreadable
	}
	expected := newSyntheticState(state.Synthetic.Spec)
	if len(state.Synthetic.Records) != len(expected.Records) {
		return state, fault.ErrUnreadable
	}
	for i, record := range expected.Records {
		if state.Synthetic.Records[i] != record {
			return state, fault.ErrUnreadable
		}
	}
	sum, err := hashSynthetic(ctx, data[:len(data)-sha256.Size])
	if err != nil {
		return state, err
	}
	if !bytes.Equal(sum[:], data[len(data)-sha256.Size:]) {
		return state, fault.ErrUnreadable
	}
	// Verify the full deterministic payload with bounded scratch space as well
	// as its integrity digest; no second snapshot-sized fixture is allocated.
	payload := data[end : len(data)-sha256.Size]
	scratch := make([]byte, min(syntheticBlockBytes, len(payload)))
	random := syntheticRandom(state.Synthetic)
	for offset := 0; offset < len(payload); {
		if err := ctx.Err(); err != nil {
			return state, err
		}
		n := min(len(scratch), len(payload)-offset)
		if _, err := random.Read(scratch[:n]); err != nil {
			return state, err
		}
		if !bytes.Equal(scratch[:n], payload[offset:offset+n]) {
			return state, fault.ErrUnreadable
		}
		offset += n
	}
	return state, nil
}

func hashSynthetic(ctx context.Context, data []byte) ([sha256.Size]byte, error) {
	hash := sha256.New()
	for offset := 0; offset < len(data); {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, err
		}
		n := min(syntheticBlockBytes, len(data)-offset)
		if _, err := hash.Write(data[offset : offset+n]); err != nil {
			return [sha256.Size]byte{}, err
		}
		offset += n
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(sum[:0]))
	return sum, ctx.Err()
}

func (s syntheticSpec) framingBytes() uint64 {
	if s.FramingBytes > 0 {
		return s.FramingBytes
	}
	return syntheticFramingAllowance
}

func syntheticRandom(state *syntheticState) *rand.ChaCha8 {
	input := binary.BigEndian.AppendUint64(nil, state.Spec.Seed)
	input = binary.BigEndian.AppendUint64(input, state.Counter)
	input = binary.BigEndian.AppendUint32(input, state.Spec.Version)
	return rand.NewChaCha8(sha256.Sum256(input))
}
