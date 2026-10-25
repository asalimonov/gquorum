package model

import (
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// SchemaVersion identifies the private recovery record format.
const SchemaVersion uint32 = 1

const (
	defaultRecordBytes   uint64 = 64 << 20
	defaultGroupBytes    uint64 = 1 << 30
	defaultMetadataBytes uint64 = 128 << 20
	maxRecoveryIDBytes          = 128
	maxAcceptanceYear           = 9999
)

// ManifestMaxBytes and HeaderMaxBytes bound private journal decoding.
const (
	ManifestMaxBytes uint64 = 64 << 20
	HeaderMaxBytes   uint64 = 64 << 10
)

// ResolveResources fills absent encoded-byte limits without changing authorization.
func ResolveResources(r Resources) (Resources, error) {
	if r.MaxRecordBytes == 0 {
		r.MaxRecordBytes = defaultRecordBytes
	}
	if r.MaxGroupBytes == 0 {
		r.MaxGroupBytes = defaultGroupBytes
	}
	if r.MaxMetadataBytes == 0 {
		r.MaxMetadataBytes = defaultMetadataBytes
	}
	for _, n := range []uint64{r.MaxRecordBytes, r.MaxGroupBytes, r.MaxMetadataBytes} {
		if n >= uint64(math.MaxInt) {
			return Resources{}, fault.New(fault.Invalid, "recovery byte limit exceeds addressable input", nil)
		}
	}
	return r, nil
}

// ValidateRecoveryID accepts only safe, non-reserved operation components.
func ValidateRecoveryID(id string) error {
	if len(id) == 0 || len(id) > maxRecoveryIDBytes || id == "." || id == ".." ||
		id == "current.json" || id == "manifest.json" {
		return fault.New(fault.Invalid, "invalid recovery ID", nil)
	}
	for _, c := range []byte(id) {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' {
			continue
		}
		return fault.New(fault.Invalid, "recovery ID must be a safe ASCII component", nil)
	}
	return nil
}

// ValidateRequest validates inspection input; Inspect may generate an absent ID.
func ValidateRequest(r Request) error {
	if err := validateRequestBudget(r); err != nil {
		return err
	}
	if r.Mode != Rebuild && r.Mode != ResetForJoin {
		return fault.New(fault.Invalid, "unknown recovery mode", nil)
	}
	if r.RecoveryID != "" {
		if err := ValidateRecoveryID(r.RecoveryID); err != nil {
			return err
		}
	}
	if !validText(r.Actor) || !validText(r.Comment) {
		return fault.New(fault.Invalid, "recovery actor and comment are required", nil)
	}
	if r.ClusterID != "" && !validText(r.ClusterID) {
		return fault.New(fault.Invalid, "invalid recovery destination", nil)
	}
	if r.Mode == ResetForJoin && (r.ClusterID == "" || len(r.RequiredGroups) != 0) {
		return fault.New(fault.Invalid, "reset requires a destination and no group assertions", nil)
	}
	return validateRequiredGroups(r.RequiredGroups)
}

func validateRequestBudget(request Request) error {
	remaining := ManifestMaxBytes
	consume := func(value string) bool {
		if uint64(len(value)) > remaining {
			return false
		}
		remaining -= uint64(len(value))
		return true
	}
	for _, value := range []string{request.RecoveryID, request.ClusterID, request.Actor, request.Comment} {
		if !consume(value) {
			return fault.New(fault.Invalid, "request cannot fit accepted manifest", nil)
		}
	}
	for _, group := range request.RequiredGroups {
		if remaining < 2 || uint64(len(group)) > remaining-2 {
			return fault.New(fault.Invalid, "group assertions cannot fit accepted manifest", nil)
		}
		remaining -= uint64(len(group)) + 2
	}
	return nil
}

func validateRequiredGroups(groups []string) error {
	seen := make(map[string]bool, len(groups))
	for _, group := range groups {
		if err := topology.ValidateID(group); err != nil {
			return err
		}
		if seen[group] {
			return fault.New(fault.Invalid, "duplicate required recovery group", nil)
		}
		seen[group] = true
	}
	return nil
}

// BackupName derives a human-readable name once at durable approval.
func BackupName(at time.Time, id string) (string, error) {
	if err := ValidateRecoveryID(id); err != nil {
		return "", err
	}
	if at.IsZero() || at.Year() < 1 || at.Year() > maxAcceptanceYear {
		return "", fault.New(fault.Invalid, "invalid recovery acceptance time", nil)
	}
	return at.UTC().Format("20060102T150405Z") + "-" + id, nil
}

func validText(s string) bool {
	return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0) && utf8.ValidString(s)
}
