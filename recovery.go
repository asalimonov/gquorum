package gquorum

import (
	"context"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Recovery is an immutable configuration handle with no resource lifetime.
// Each operation acquires and releases its own source ownership.
type Recovery struct {
	source  recoverymodel.SourceOptions
	options recovery.Options
}

// NewRecovery validates/copies recovery configuration without I/O or callbacks.
// Supply zero or one recovery.Options value. Unrelated Node settings are ignored.
func NewRecovery(opts NodeOptions, options ...recovery.Options) (*Recovery, error) {
	if len(options) > 1 {
		return nil, recoveryRefusal("invalid_request",
			"at most one recovery options value is allowed", fault.ErrInvalid)
	}
	source, err := recoverySourceOptions(opts)
	if err != nil {
		return nil, err
	}
	var selected recovery.Options
	if len(options) == 1 {
		selected = options[0]
	}
	if _, err := recoverymodel.ResolveResources(selected.Resources); err != nil {
		return nil, recoveryRefusal("resource_bound", "invalid recovery resource settings", err)
	}
	return &Recovery{source: source, options: selected}, nil
}

func recoverySourceOptions(opts NodeOptions) (recoverymodel.SourceOptions, error) {
	if opts.Loads.Storage.Store != nil || opts.Loads.Storage.Retirement != nil {
		// TODO(not planned): recover injected/custom stores; use Node-owned WAL configuration.
		return recoverymodel.SourceOptions{}, recoveryRefusal("unsupported_source",
			"recovery requires Node-owned WAL configuration", fault.ErrUnsupported)
	}
	valid := func(s string) bool {
		return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0) && utf8.ValidString(s)
	}
	if !valid(opts.StorageDir) || !valid(opts.Identity.NodeName) ||
		opts.Identity.ClusterID != "" && !valid(opts.Identity.ClusterID) {
		return recoverymodel.SourceOptions{}, recoveryRefusal("invalid_source",
			"source directory and node name are required; expected cluster must be valid", fault.ErrInvalid)
	}
	source := recoverymodel.SourceOptions{
		Paths:    recoverymodel.Paths{StorageDir: opts.StorageDir},
		NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID,
	}
	if wal := opts.Loads.Storage.WAL; wal != nil {
		if !valid(wal.RootDir) || pathWithin(opts.StorageDir, wal.RootDir) {
			return recoverymodel.SourceOptions{}, recoveryRefusal("invalid_source",
				"system storage must be outside an explicit WAL root", fault.ErrInvalid)
		}
		source.Paths.LoadWALDir = wal.RootDir
	}
	for _, root := range []string{source.Paths.StorageDir, source.Paths.LoadWALDir} {
		for _, part := range strings.Split(root, string(filepath.Separator)) {
			if part == "@gquorum-recovery" {
				return recoverymodel.SourceOptions{}, recoveryRefusal("invalid_source",
					"private recovery directories cannot be source roots", fault.ErrInvalid)
			}
		}
	}
	return source, nil
}

func recoveryRefusal(code, message string, cause error) error {
	return &recovery.Refusal{Issue: recovery.Issue{
		Code: code, Message: message, Blocking: true, Remedy: recovery.RemedyInvestigate,
	}, Cause: cause}
}

func pendingRecovery(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return recoveryRefusal("implementation_pending", operation+" is not implemented yet", fault.ErrUnsupported)
}
