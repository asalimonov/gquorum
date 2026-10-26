package raftstate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

const maxJSONEscapeBytes = 6

// Decode the existing envelope from its owned slice. A json.Decoder would first
// grow another buffer containing the entire base64 application value. Only the
// authority control object needs the shared strict record decoder. Unmarshal
// reads the input slice directly and allocates owned application bytes; the key
// walk checks strict schema without scanning/decoding the base64 field again.
func decodeSnapshotEnvelope(input []byte, target *Snapshot) error {
	if err := json.Unmarshal(input, target); err != nil {
		return fault.New(fault.Unreadable, "invalid engine snapshot JSON", err)
	}
	data := bytes.TrimSpace(input)
	if bytes.Equal(data, []byte("null")) {
		return nil // Preserve the subsequent schema validation of a null record.
	}
	if data[0] != '{' {
		return fault.New(fault.Unreadable, "engine snapshot must be an object", nil)
	}
	for pos := snapshotWhitespace(data, 1); data[pos] != '}'; {
		keyEnd := snapshotStringEnd(data, pos)
		// The longest recognized name is application. Even escaped/case-folded
		// spellings fit six bytes per character. Keep this strict-key check
		// bounded rather than allocating a large name again for its error.
		if keyEnd-pos > maxJSONEscapeBytes*len("application")+2 {
			return fault.New(fault.Unreadable, "unknown engine snapshot field", nil)
		}
		var name string
		if err := json.Unmarshal(data[pos:keyEnd], &name); err != nil {
			return fault.New(fault.Unreadable, "invalid engine snapshot field", err)
		}
		start := snapshotWhitespace(data, snapshotWhitespace(data, keyEnd)+1)
		end := snapshotValueEnd(data, start)
		if err := validateSnapshotField(name, data[start:end]); err != nil {
			return fault.New(fault.Unreadable, "invalid engine snapshot field", err)
		}
		pos = snapshotWhitespace(data, end)
		if data[pos] == ',' {
			pos = snapshotWhitespace(data, pos+1)
		}
	}
	return nil
}

func validateSnapshotField(name string, data []byte) error {
	switch {
	case strings.EqualFold(name, "schema"), strings.EqualFold(name, "cluster_id"),
		strings.EqualFold(name, "group_id"), strings.EqualFold(name, "application"):
		return nil // Types and application bytes were checked by Unmarshal above.
	case strings.EqualFold(name, "authority"):
		var authority AuthorityState
		return decode(data, &authority) // Preserve nested unknown-field refusal.
	default:
		return fmt.Errorf("json: unknown field %q", name)
	}
}

// These cursors run only after Unmarshal has checked grammar, nesting and field types.
// They retain spans, not copies, and skip base64 strings using the byte search.
func snapshotWhitespace(data []byte, pos int) int {
	cursor := pos
	for data[cursor] == ' ' || data[cursor] == '\n' || data[cursor] == '\r' || data[cursor] == '\t' {
		cursor++
	}
	return cursor
}

func snapshotStringEnd(data []byte, pos int) int {
	for next := pos + 1; ; {
		quote := next + bytes.IndexByte(data[next:], '"')
		backslashes := 0
		for before := quote - 1; data[before] == '\\'; before-- {
			backslashes++
		}
		if backslashes%2 == 0 {
			return quote + 1
		}
		next = quote + 1
	}
}

func snapshotValueEnd(data []byte, pos int) int {
	if data[pos] == '"' {
		return snapshotStringEnd(data, pos)
	}
	depth := 0
	for cursor := pos; cursor < len(data); cursor++ {
		switch data[cursor] {
		case '"':
			cursor = snapshotStringEnd(data, cursor) - 1
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return cursor
			}
			depth--
			if depth == 0 {
				return cursor + 1
			}
		case ',', ' ', '\n', '\r', '\t':
			if depth == 0 {
				return cursor
			}
		default:
			continue
		}
	}
	return len(data)
}
