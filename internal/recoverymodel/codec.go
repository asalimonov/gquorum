package recoverymodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
)

const maxJSONDepth = 128

// EncodeCanonical uses stable JSON field/map ordering for typed recovery values.
// Callers canonicalize identity-keyed slices before encoding accepted evidence.
func EncodeCanonical(value any) ([]byte, error) {
	return json.Marshal(value)
}

// DecodeStrict rejects unbounded, ambiguous or unsupported record encodings.
// I/O callers must also bound reads before constructing data.
func DecodeStrict(data []byte, into any, maxBytes uint64) error {
	if maxBytes == 0 || uint64(len(data)) > maxBytes {
		return fault.New(fault.Unreadable, "recovery record exceeds byte limit", nil)
	}
	if !utf8.Valid(data) {
		return fault.New(fault.Unreadable, "recovery record is not UTF-8", nil)
	}
	tokens := json.NewDecoder(bytes.NewReader(data))
	tokens.UseNumber()
	if err := checkJSONValue(tokens, 0, reflect.TypeOf(into)); err != nil {
		return fault.New(fault.Unreadable, "invalid recovery record", err)
	}
	if _, err := tokens.Token(); err != io.EOF {
		return fault.New(fault.Unreadable, "trailing recovery record data", err)
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return fault.New(fault.Unreadable, "invalid recovery record fields", err)
	}
	return nil
}

func checkJSONValue(d *json.Decoder, depth int, shape reflect.Type) error {
	if depth > maxJSONDepth {
		return errors.New("recovery record nesting exceeds limit")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	opening, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch opening {
	case '{':
		return checkJSONObject(d, depth, shape)
	case '[':
		for d.More() {
			if err := checkJSONValue(d, depth+1, arrayItem(shape)); err != nil {
				return err
			}
		}
		return checkClosingDelimiter(d, ']')
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

func checkJSONObject(d *json.Decoder, depth int, shape reflect.Type) error {
	keys := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || keys[name] {
			return fmt.Errorf("duplicate or invalid recovery field %q", name)
		}
		keys[name] = true
		child, known := objectField(shape, name)
		if !known {
			return fmt.Errorf("unknown recovery field %q", name)
		}
		if err := checkJSONValue(d, depth+1, child); err != nil {
			return err
		}
	}
	return checkClosingDelimiter(d, '}')
}

func checkClosingDelimiter(d *json.Decoder, expected json.Delim) error {
	closing, err := d.Token()
	if err != nil {
		return err
	}
	if closing != expected {
		return errors.New("mismatched recovery record delimiter")
	}
	return nil
}
