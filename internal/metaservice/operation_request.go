package metaservice

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ApplyVersion keeps approval and explicit retry distinct on the existing
// authenticated administration route. Empty Kind is schema3-only until Task78.
type applyVersionRequest struct {
	Kind     string               `json:"kind,omitempty"`
	Version  uint64               `json:"version,omitempty"`
	Previous topology.OperationID `json:"previous,omitempty"`
}

func decodeApplyVersionRequest(data []byte) (applyVersionRequest, error) {
	var request applyVersionRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF {
		return request, fault.ErrInvalid
	}
	return request, request.validate()
}

func (request applyVersionRequest) validate() error {
	switch request.Kind {
	case "", "approve":
		if request.Version == 0 || request.Previous != "" {
			return fault.ErrInvalid
		}
	case "retry":
		if request.Version != 0 || strings.TrimSpace(string(request.Previous)) == "" {
			return fault.ErrInvalid
		}
	default:
		return fault.ErrInvalid
	}
	return nil
}
