package task

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/load"
)

const (
	maxCommandBytes      = 64 << 10
	maxExecutorNameBytes = 128
	hashHexLength        = sha256.Size * 2
)

var kinds = []string{"create", "assign", "revoke", "start", "finish"}

// Encode builds a proposal. The RequestID is diagnostic: commands are CAS or
// idempotent, so the machine needs no deduplication table.
func Encode(kind string, body any) (load.Proposal, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return load.Proposal{}, err
	}
	data, err := json.Marshal(Command{Version: SchemaVersion, Kind: kind, Body: raw})
	if err != nil {
		return load.Proposal{}, err
	}
	sum := sha256.Sum256(data)
	p := load.Proposal{RequestID: kind + "/" + hex.EncodeToString(sum[:8]), Data: data}
	if _, err := Decode(load.Command{RequestID: p.RequestID, Data: data}); err != nil {
		return load.Proposal{}, err
	}
	return p, nil
}

// Decode strictly parses the envelope and its typed body. Semantic checks that
// depend on committed state belong to Transition.
func Decode(entry load.Command) (Command, error) {
	var c Command
	if len(entry.Data) > maxCommandBytes {
		return c, errors.New("command too large")
	}
	if err := jsonwire.Decode(entry.Data, &c); err != nil {
		return c, err
	}
	if c.Version != SchemaVersion || !slices.Contains(kinds, c.Kind) {
		return c, errors.New("unsupported command")
	}
	var body any
	switch c.Kind {
	case "create":
		body = new(Create)
	case "assign":
		body = new(Assign)
	case "revoke":
		body = new(Revoke)
	case "start":
		body = new(Start)
	case "finish":
		body = new(Finish)
	default: // The supported command kinds were checked above.
	}
	return c, jsonwire.Decode(c.Body, body)
}

// ReferencedIDs returns the task IDs whose rows Apply must load for c.
func ReferencedIDs(c Command) []int64 {
	var ids []int64
	switch c.Kind {
	case "assign":
		var b Assign
		decodeErr := jsonwire.Decode(c.Body, &b)
		if decodeErr != nil || len(b.Tasks) > MaxBatch {
			return nil
		}
		for _, item := range b.Tasks {
			ids = append(ids, item.TaskID)
		}
	case "revoke":
		var b Revoke
		decodeErr := jsonwire.Decode(c.Body, &b)
		if decodeErr != nil || len(b.Tasks) > MaxBatch {
			return nil
		}
		for _, item := range b.Tasks {
			ids = append(ids, item.Key.TaskID)
		}
	case "start":
		var b Start
		if jsonwire.Decode(c.Body, &b) == nil {
			ids = append(ids, b.Key.TaskID)
		}
	case "finish":
		var b Finish
		if jsonwire.Decode(c.Body, &b) == nil {
			ids = append(ids, b.Key.TaskID)
		}
	default: // Other command kinds reference no existing task IDs.
	}
	return ids
}

// ValidExecutor bounds an executor reference.
func ValidExecutor(e Executor) bool {
	_, err := CheckedInt(e.ReplicaID)
	return err == nil && e.ReplicaID > 0 && len(e.NodeName) > 0 && len(e.NodeName) <= maxExecutorNameBytes &&
		strings.TrimSpace(e.NodeName) == e.NodeName
}

// CanonicalHash accepts exactly 64 lowercase hex characters.
func CanonicalHash(value string) bool {
	if len(value) != hashHexLength || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// KeyValid reports a positive task ID and attempt.
func KeyValid(key Key) bool { return key.TaskID > 0 && key.Attempt > 0 }
