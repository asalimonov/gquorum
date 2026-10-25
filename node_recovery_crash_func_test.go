//go:build functional

package gquorum

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
)

func TestRecoveryRestartResumesEachPreFoundingAcquisitionBoundary(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"authority only", "fresh unpinned meta", "pinned before v1", "acquired metadata missing", "foreign authority", "unpinned nonfresh meta"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			source := recoveryFixtureOptions(t, false)
			facade, operation := acceptedRecovery(t, source, nil)
			status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := recoverydisk.Acquire(t.Context(), facade.source.Paths)
			if err != nil {
				t.Fatal(err)
			}
			journal, err := recoverydisk.Open(t.Context(), owner, operation.RecoveryID)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.MarkSuccessorStarted(t.Context(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			identity := status.Identity
			identity.InstanceID = "interrupted-process"
			if boundary == "foreign authority" {
				identity.ClusterID = "foreign"
			}
			authority, err := authoritystore.Open(source.authorityDBPath(), identity, authoritystore.FreshEvidence)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "acquired metadata missing" {
				if err := authority.MarkMetadataAcquired(t.Context(), identity); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "fresh unpinned meta" || boundary == "pinned before v1" || boundary == "unpinned nonfresh meta" {
				meta, err := metastore.Open(source.metaDBPath())
				if err != nil {
					t.Fatal(err)
				}
				if boundary == "pinned before v1" {
					if err := meta.EnsureIdentity(identity.NodeName, identity.ClusterID, identity.MemberID); err != nil {
						t.Fatal(err)
					}
				}
				if boundary == "unpinned nonfresh meta" {
					if err := meta.SaveOverride(1); err != nil {
						t.Fatal(err)
					}
				}
				if err := meta.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Release the process's actual acquired files exactly at this crash boundary;
			// no source/destination file is deleted or repaired to make restart pass.
			if err := authority.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			options := authorityNodeOptions(t, source.StorageDir, "resumed-process")
			options.Node = source.Bootstrap.Founders[0].Node
			options, err = RecoveryStartOptions(status, options)
			if err != nil {
				t.Fatal(err)
			}
			participant, err := NewNode(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopAuthorityNode(t, participant) })
			startErr := participant.Start(t.Context())
			if boundary == "acquired metadata missing" || boundary == "foreign authority" || boundary == "unpinned nonfresh meta" {
				if !errors.Is(startErr, fault.ErrUnreadable) {
					t.Fatal("contradictory acquisition was resumed", startErr)
				}
				return
			}
			if startErr != nil {
				t.Fatal(startErr)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := participant.WaitReady(ctx); err != nil {
				t.Fatal(err, participant.Status())
			}
		})
	}
}
