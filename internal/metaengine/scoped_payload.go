package metaengine

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

// Decode shape only. Identity, authority, timestamps and CAS are checked against
// replicated state by each handler, never inferred from successful JSON decoding.
func decodeScopedPayload(data []byte, into any, required ...string) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return fault.New(fault.Invalid, "invalid scoped proposal", err)
	}
	if d.Decode(new(any)) != io.EOF {
		return fault.ErrInvalid
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fault.New(fault.Invalid, "invalid scoped proposal object", err)
	}
	for _, field := range required {
		value := bytes.TrimSpace(fields[field])
		if len(value) == 0 || bytes.Equal(value, []byte("null")) {
			return fault.New(fault.Invalid, "missing scoped proposal field: "+field, nil)
		}
	}
	var correlation string
	if err := json.Unmarshal(fields["proposal_id"], &correlation); err != nil || strings.TrimSpace(correlation) == "" {
		return fault.New(fault.Invalid, "missing proposal correlation ID", err)
	}
	return nil
}
