//go:build functional

package gquorum

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func recoverySourceWithLimits(t *testing.T) NodeOptions {
	t.Helper()
	source := authorityNodeOptions(t, t.TempDir(), "source-custom-rules")
	source.Limits = topology.Limits{MaxNodes: 4}
	participant, err := NewNode(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, participant)
	return source
}

func TestRecoveryNodeInheritsAcceptedRulesAndRefusesConflictingStartupBeforeMarker(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inherit", "explicit conflict", "wrong cluster", "wrong bootstrap", "unjournaled successor"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			source := recoverySourceWithLimits(t)
			facade, operation := acceptedRecovery(t, source, nil)
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			options := authorityNodeOptions(t, source.StorageDir, "destination")
			options.Node = source.Bootstrap.Founders[0].Node
			options, err = RecoveryStartOptions(status, options)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "explicit conflict":
				options.Limits = topology.Limits{MaxNodes: 3}
			case "wrong cluster":
				options.Identity.ClusterID = "different-cluster"
			case "wrong bootstrap":
				options.Bootstrap.Join = true
				options.Bootstrap.Founders = nil
				options.Bootstrap.JoinSeeds = []string{"127.0.0.1:1"}
			case "unjournaled successor":
				if err := os.WriteFile(filepath.Join(source.StorageDir, authorityDBFilename), []byte("unexpected successor"), 0o600); err != nil {
					t.Fatal(err)
				}
			default:
			}
			participant, err := NewNode(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopAuthorityNode(t, participant) })
			err = participant.Start(t.Context())
			if mode == "inherit" {
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := participant.WaitReady(ctx); err != nil {
					t.Fatal(err, participant.Status())
				}
				if got := participant.(*node).metaOpts.Limits; got != source.Limits.WithDefaults() {
					t.Fatal(got)
				}
				stopAuthorityNode(t, participant)
				restart := authorityNodeOptions(t, source.StorageDir, "rules-restart")
				restart.Identity.ClusterID, restart.Identity.MemberID = status.Identity.ClusterID, status.Identity.MemberID
				restart.Bootstrap.Founders[0].MemberID = status.Identity.MemberID
				restarted, err := NewNode(restart)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { stopAuthorityNode(t, restarted) })
				if err := restarted.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := restarted.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid startup accepted")
			}
			if mode == "unjournaled successor" {
				if !errors.Is(err, fault.ErrConflict) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatal(err)
			}
			stopAuthorityNode(t, participant)
			record, err := recoverydisk.Read(t.Context(), facade.source.Paths, operation.RecoveryID)
			if err != nil || record.SuccessorStartedAt != nil || record.Phase != model.AwaitingStart {
				t.Fatal(record, err)
			}
			if _, err := os.Stat(filepath.Join(source.StorageDir, metaDBFilename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refusal acquired destination metadata", err)
			}
			if mode != "unjournaled successor" {
				if _, err := os.Stat(filepath.Join(source.StorageDir, authorityDBFilename)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal acquired destination authority", err)
				}
			}
			reported, err := facade.Status(t.Context(), operation.RecoveryID)
			if err != nil || reported.Phase != recovery.AwaitingStart {
				t.Fatal(reported, err)
			}
		})
	}
}
