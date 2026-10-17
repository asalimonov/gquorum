//go:build functional

package authoritystore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

func localIdentity() topology.Identity {
	return topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "process"}
}
func localGrant(revision uint64) topology.AuthorityGrant {
	a := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 1, GroupID: "work", NodeName: "n1", ReplicaID: 7, Kind: topology.StepPrepareReplica}
	return topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: revision}, Purpose: topology.AuthorityEffect, OperationID: a.OperationID, Action: &a, Resources: []topology.AuthorityResource{resource("work", "n1")}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 7}}}
}
func openLocal(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private", "authority.bolt")
	s, err := Open(path, localIdentity(), FreshEvidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, path
}
func completeLocal(t *testing.T, s *Store, g topology.AuthorityGrant) FenceRecord {
	t.Helper()
	r, err := s.Fence(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.CompleteFence(t.Context(), g.Key, r.Record.Sequence)
	if err != nil || e.Validate(g) != nil {
		t.Fatal(e, err)
	}
	return r.Record
}
func receiptFor(g topology.AuthorityGrant, sequence uint64) topology.EffectReceipt {
	return topology.EffectReceipt{Key: g.Key, Resource: g.Resources[0], Action: g.Action, Target: g.Targets[0], Outcome: topology.EffectApplied, LocalSequence: sequence}
}

func TestPendingFenceAndRunningEffectSurviveRestart(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	g := localGrant(10)
	r, err := s.Fence(t.Context(), g)
	if err != nil || r.Same || r.Record.Complete || r.Record.Sequence != 1 {
		t.Fatal(r.Record, r.Same, err)
	}
	if _, err := s.BeginEffect(t.Context(), g, g.Targets[0]); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	nextID := localIdentity()
	nextID.InstanceID = "restart"
	s, err = Open(path, nextID, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err = s.Fence(t.Context(), g)
	if err != nil || !r.Same || r.Record.Sequence != 1 || r.Record.Complete {
		t.Fatal(r.Record, r.Same, err)
	}
	e, err := s.CompleteFence(t.Context(), g.Key, r.Record.Sequence)
	if err != nil || e.Receiver.InstanceID != "restart" {
		t.Fatal(e, err)
	}
	if got, err := s.BeginEffect(t.Context(), g, g.Targets[0]); err != nil || got != nil {
		t.Fatal(got, err)
	}
	if _, err := s.BeginEffect(t.Context(), g, g.Targets[0]); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("running operation was blindly repeated", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, nextID, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	running, err := s.Running(t.Context(), "work")
	if err != nil || len(running) != 1 || running[0].Sequence != 1 {
		t.Fatal(running, err)
	}
	running[0].Grant.Action.NodeName = "mutated"
	proof := receiptFor(g, 1)
	if err := s.FinishEffect(t.Context(), proof); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishEffect(t.Context(), proof); err != nil {
		t.Fatal("receipt replay failed", err)
	}
	got, err := s.BeginEffect(t.Context(), g, g.Targets[0])
	if err != nil || got == nil || got.LocalSequence != 1 {
		t.Fatal(got, err)
	}
	got.Action.NodeName = "changed"
	proof.Outcome = topology.EffectAlreadySatisfied
	if err := s.FinishEffect(t.Context(), proof); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("contradictory receipt accepted", err)
	}
}

func TestNewFenceRejectsOldAdmissionAndJoinsOldConsumption(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	old := localGrant(10)
	completeLocal(t, s, old)
	if _, err := s.BeginEffect(t.Context(), old, old.Targets[0]); err != nil {
		t.Fatal(err)
	}
	next := localGrant(11)
	next.Purpose, next.Action = topology.AuthorityFence, nil
	r, err := s.Fence(t.Context(), next)
	if err != nil || r.Record.Sequence != 2 {
		t.Fatal(r.Record, err)
	}
	if _, err := s.BeginEffect(t.Context(), old, old.Targets[0]); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	if _, err := s.Fence(t.Context(), old); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	if _, err := s.CompleteFence(t.Context(), old.Key, 1); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	wrong := next.Clone()
	wrong.OperationID = "changed"
	if _, err := s.Fence(t.Context(), wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	bad := receiptFor(old, 2)
	if err := s.FinishEffect(t.Context(), bad); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("new sequence accepted for old effect", err)
	}
	if err := s.FinishEffect(t.Context(), receiptFor(old, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteFence(t.Context(), next.Key, r.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginEffect(t.Context(), next, next.Targets[0]); err == nil {
		t.Fatal("fence-only grant authorized an action")
	}
}

func TestAuthorityPinsMissingFilesAndExactRebind(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	g := localGrant(12)
	completeLocal(t, s, g)
	previous := localIdentity()
	granted := previous
	granted.MemberID = 2
	granted.InstanceID = "replacement"
	if err := s.Rebind(t.Context(), previous, granted, "admission"); err != nil {
		t.Fatal(err)
	}
	if err := s.Rebind(t.Context(), previous, granted, "admission"); err != nil {
		t.Fatal(err)
	}
	if err := s.Rebind(t.Context(), previous, granted, "other"); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if _, err := s.Fence(t.Context(), g); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old member admitted after rebind", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if foreign, err := Open(path, previous, RetainedEvidence); !errors.Is(err, fault.ErrIdentityMismatch) {
		if foreign != nil {
			foreign.Close()
		}
		t.Fatal(err)
	}
	s, err := Open(path, granted, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Current(t.Context(), "work")
	if err != nil || !r.Found || !r.Record.Grant.Equal(g) || !r.Record.Complete {
		t.Fatal(r.Record, r.Found, err)
	}
	missing := filepath.Join(t.TempDir(), "missing.bolt")
	if _, err := Open(missing, previous, RetainedEvidence); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing retained store was created", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	info, err = os.Stat(filepath.Dir(path))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal(info, err)
	}
}

func TestAuthorityStoreRejectsCorruptStateAndSequenceOverflow(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"schema", "partial", "foreign_grant", "effect_sequence", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, path := openLocal(t)
			g := localGrant(10)
			completeLocal(t, s, g)
			if _, err := s.BeginEffect(t.Context(), g, g.Targets[0]); err != nil {
				t.Fatal(err)
			}
			err := s.db.Update(func(tx *bolt.Tx) error {
				switch mode {
				case "schema":
					return tx.Bucket([]byte("format")).Put([]byte("schema_version"), []byte("99"))
				case "partial":
					return tx.DeleteBucket([]byte("effects"))
				case "foreign_grant":
					g.Key.ClusterID = "foreign"
					return put(tx.Bucket([]byte("fences")), []byte("work"), FenceRecord{Grant: g, Sequence: 1, Complete: true})
				case "effect_sequence":
					key, err := effectKey(g.Key, g.Targets[0])
					if err != nil {
						return err
					}
					return put(tx.Bucket([]byte("effects")), key, EffectRecord{Grant: g, Target: g.Targets[0], Sequence: 3, State: "running"})
				case "overflow":
					return put(tx.Bucket([]byte("fences")), []byte("work"), FenceRecord{Grant: g, Sequence: math.MaxUint64, Complete: true})
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "overflow" {
				g.Key.Revision++
				if _, err := s.Fence(t.Context(), g); !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal(err)
				}
				return
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(path, localIdentity(), RetainedEvidence); err == nil {
				reopened.Close()
				t.Fatal("corruption accepted")
			}
		})
	}
}

func TestAuthorityStoreCanceledAndClosedOperations(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Fence(ctx, localGrant(1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ok, err := s.Current(t.Context(), "work"); err != nil || ok.Found {
		t.Fatal(ok.Found, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Current(t.Context(), "work"); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestReadOnlyIdentityInspection(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "missing")
	if found, err := LoadIdentity(missing); err != nil || found.Found {
		t.Fatal(found.Found, err)
	}
	s, path := openLocal(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := LoadIdentity(path)
	want := localIdentity()
	want.InstanceID = ""
	if err != nil || !id.Found || id.Identity != want {
		t.Fatal(id.Identity, id.Found, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection rewrote evidence", err)
	}
}

func TestCompletedReadmissionReceiptRemainsReadableAfterRebind(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	g := localGrant(12)
	g.Purpose, g.Action, g.OperationID, g.AdmissionID = topology.AuthorityReadmit, nil, "", "admission"
	floor := completeLocal(t, s, g)
	if _, err := s.BeginEffect(t.Context(), g, g.Targets[0]); err != nil {
		t.Fatal(err)
	}
	if found, err := s.Receipt(t.Context(), g, g.Targets[0]); err != nil || found.Found {
		t.Fatal("running intent became completion proof", found.Found, err)
	}
	proof := receiptFor(g, floor.Sequence)
	proof.AdmissionID = g.AdmissionID
	if err := s.FinishEffect(t.Context(), proof); err != nil {
		t.Fatal(err)
	}
	previous, granted := localIdentity(), localIdentity()
	granted.MemberID, granted.InstanceID = 2, "replacement"
	if err := s.Rebind(t.Context(), previous, granted, g.AdmissionID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginEffect(t.Context(), g, g.Targets[0]); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old member admitted", err)
	}
	next := localGrant(13)
	next.Targets[0].MemberID = granted.MemberID
	completeLocal(t, s, next)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, granted, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Receipt(t.Context(), g, g.Targets[0])
	if err != nil || !got.Found || got.Receipt != proof {
		t.Fatal(got.Receipt, got.Found, err)
	}
	bad := g.Clone()
	bad.AdmissionID = "unrelated"
	if _, err := s.Receipt(t.Context(), bad, bad.Targets[0]); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("changed grant reused receipt", err)
	}
}

func TestFenceRetainsUncertainEffectWithoutSuccessReceipt(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	old := localGrant(10)
	completeLocal(t, s, old)
	if _, err := s.BeginEffect(t.Context(), old, old.Targets[0]); err != nil {
		t.Fatal(err)
	}
	records, err := s.Running(t.Context(), "work")
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	record := records[0]
	if err := s.FenceEffect(t.Context(), record, old.Key); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("same grant abandoned its own effect", err)
	}
	next := localGrant(11)
	next.Purpose, next.Action = topology.AuthorityFence, nil
	floor, err := s.Fence(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteFence(t.Context(), next.Key, floor.Record.Sequence); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("unjoined running intent allowed completion", err)
	}
	altered := record
	altered.Sequence++
	if err := s.FenceEffect(t.Context(), altered, next.Key); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("altered record ended an intent", err)
	}
	if err := s.FenceEffect(t.Context(), record, next.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteFence(t.Context(), next.Key, floor.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.FenceEffect(t.Context(), record, next.Key); err != nil {
		t.Fatal("idempotent fencing failed", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, localIdentity(), RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, err := s.Running(t.Context(), "work"); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	if found, err := s.Receipt(t.Context(), old, old.Targets[0]); err != nil || found.Found {
		t.Fatal("uncertain outcome became completion proof", found.Found, err)
	}
	if err := s.FinishEffect(t.Context(), receiptFor(old, record.Sequence)); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("late completion crossed the fence", err)
	}
	if _, err := s.BeginEffect(t.Context(), old, old.Targets[0]); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("fenced operation resumed", err)
	}
}

func TestMetaStopIntentFencesParticipationUntilMemberReplacement(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"running", "complete", "fenced"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s, path := openLocal(t)
			if stopped, err := s.MetaParticipationStopped(t.Context()); err != nil || stopped {
				t.Fatal(stopped, err)
			}
			grant := localGrant(10)
			grant.Key.GroupID, grant.Action.GroupID, grant.Resources[0].GroupID = topology.MetaGroupID, topology.MetaGroupID, topology.MetaGroupID
			grant.Action.Kind, grant.Action.ReplicaID, grant.Targets[0].ReplicaID = topology.StepStopReplica, 1, 1
			floor := completeLocal(t, s, grant)
			if stopped, err := s.MetaParticipationStopped(t.Context()); err != nil || stopped {
				t.Fatal("unconsumed grant stopped participation", stopped, err)
			}
			if _, err := s.BeginEffect(t.Context(), grant, grant.Targets[0]); err != nil {
				t.Fatal(err)
			}
			if state == "complete" {
				if err := s.FinishEffect(t.Context(), receiptFor(grant, floor.Sequence)); err != nil {
					t.Fatal(err)
				}
			}
			if state == "fenced" {
				running, err := s.Running(t.Context(), topology.MetaGroupID)
				if err != nil || len(running) != 1 {
					t.Fatal(running, err)
				}
				next := grant.Clone()
				next.Key.Revision++
				next.Purpose, next.Action = topology.AuthorityFence, nil
				floor, err := s.Fence(t.Context(), next)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.FenceEffect(t.Context(), running[0], next.Key); err != nil {
					t.Fatal(err)
				}
				if _, err = s.CompleteFence(t.Context(), next.Key, floor.Record.Sequence); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			id := localIdentity()
			id.InstanceID = "restart"
			reopened, err := Open(path, id, RetainedEvidence)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if stopped, err := reopened.MetaParticipationStopped(t.Context()); err != nil || !stopped {
				t.Fatal("restart or higher floor lost stop evidence", stopped, err)
			}
			if state == "complete" {
				next := grant.Clone()
				next.Key.Revision++
				next.Purpose, next.Action = topology.AuthorityFence, nil
				completeLocal(t, reopened, next)
			}
			completed, err := reopened.CompletedMetaStop(t.Context())
			if err != nil || completed.Found != (state == "complete") || completed.Found && (!completed.Record.Grant.Equal(grant) || completed.Record.Receipt == nil) {
				t.Fatal("historical completed stop lookup", completed.Record, completed.Found, err)
			}
			successor := id
			successor.MemberID++
			if err = reopened.Rebind(t.Context(), id, successor, "verified-readmission"); err != nil {
				t.Fatal(err)
			}
			if stopped, err := reopened.MetaParticipationStopped(t.Context()); err != nil || stopped {
				t.Fatal("old stop fenced new member", stopped, err)
			}
			if found, err := reopened.CompletedMetaStop(t.Context()); err != nil || found.Found {
				t.Fatal("former member stop authorized successor release", found.Found, err)
			}
		})
	}
}
