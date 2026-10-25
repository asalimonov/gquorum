package recoverymodel

import (
	"encoding/json"
	"math"

	"github.com/asalimonov/gquorum/fault"
)

// DecodeManifest validates a bounded schema-1 operation, including its acceptance.
func DecodeManifest(data []byte) (LocalManifest, error) {
	var r LocalManifest
	if err := DecodeStrict(data, &r, ManifestMaxBytes); err != nil {
		return LocalManifest{}, err
	}
	if err := ValidateManifest(r); err != nil {
		return LocalManifest{}, err
	}
	return r, nil
}

// EncodeManifest refuses malformed or oversized authority before publication.
func EncodeManifest(r LocalManifest) ([]byte, error) {
	if err := ValidateManifest(r); err != nil {
		return nil, err
	}
	data, err := EncodeCanonical(r)
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > ManifestMaxBytes {
		return nil, fault.New(fault.Invalid, "recovery manifest exceeds format bound", nil)
	}
	return data, nil
}

// DecodeCurrent validates a bounded pointer, not the authority it names.
func DecodeCurrent(data []byte) (Current, error) {
	var r Current
	if err := DecodeStrict(data, &r, HeaderMaxBytes); err != nil {
		return Current{}, err
	}
	if err := ValidateCurrent(r); err != nil {
		return Current{}, err
	}
	return r, nil
}

// DecodeSeedMarker separates the fixed header budget from its bounded grant.
func DecodeSeedMarker(data []byte, maxGrantBytes uint64) (SeedMarker, error) {
	var r SeedMarker
	if maxGrantBytes == 0 || maxGrantBytes > math.MaxUint64-HeaderMaxBytes {
		return r, fault.ErrInvalid
	}
	if err := DecodeStrict(data, &r, maxGrantBytes+HeaderMaxBytes); err != nil {
		return SeedMarker{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return SeedMarker{}, err
	}
	grantBytes := uint64(len(fields["grant"]))
	if grantBytes > maxGrantBytes || uint64(len(data))-grantBytes > HeaderMaxBytes {
		return SeedMarker{}, fault.ErrUnreadable
	}
	if err := ValidateSeedMarker(r); err != nil {
		return SeedMarker{}, err
	}
	return r, nil
}
