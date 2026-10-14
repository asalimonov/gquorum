package jsonwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Decode accepts exactly one non-null JSON value and rejects unknown fields.
func Decode(data []byte, dst any) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("null value")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
