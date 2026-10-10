package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (a *application) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	target, active, err := a.documents(ctx)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	names := map[string]map[uint64]bool{}
	for _, doc := range []*topology.Document{&target, &active} {
		if group := doc.Group(id); group != nil {
			for _, replica := range group.Replicas {
				if names[replica.NodeName] == nil {
					names[replica.NodeName] = map[uint64]bool{}
				}
				names[replica.NodeName][replica.ReplicaID] = true
			}
		}
	}
	if len(names) == 0 {
		return topology.GroupStatus{GroupID: id}, nil
	}
	type observation struct {
		status  topology.GroupStatus
		replica uint64
	}
	observed := make([]observation, 0, len(names))
	live := map[uint64]bool{}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		probeCtx, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
		reply, callErr := a.callNode(probeCtx, name, "Status", id, 0, nil)
		cancel()
		if callErr != nil {
			continue
		}
		var status topology.GroupStatus
		if json.Unmarshal(reply.Data, &status) != nil || status.GroupID != id || status.HostNode != name {
			return topology.GroupStatus{}, fault.ErrIdentityMismatch
		}
		if status.Found {
			if !names[name][reply.ReplicaId] {
				return topology.GroupStatus{}, fault.ErrIdentityMismatch
			}
			live[reply.ReplicaId] = true
			observed = append(observed, observation{status, reply.ReplicaId})
		}
	}
	if err := ctx.Err(); err != nil {
		return topology.GroupStatus{}, err
	}
	for _, observed := range observed {
		if observed.status.LeaderID == observed.replica {
			result := observed.status.Clone()
			result.HealthKnown = true
			result.LiveVoters = live
			return result, nil
		}
	}
	if len(observed) == 0 {
		if active.Group(id) == nil {
			return topology.GroupStatus{GroupID: id}, nil
		}
		return topology.GroupStatus{GroupID: id, Found: true}, fault.ErrUnavailable
	}
	return topology.GroupStatus{GroupID: id, Found: true}, fault.ErrUnavailable
}

func (a *application) CreateGroup(ctx context.Context, approved topology.AuthorizedStep) error {
	if err := a.meta.Authorize(ctx, approved); err != nil {
		return err
	}
	target, err := a.meta.Target(ctx)
	if err != nil {
		return err
	}
	group := target.Group(approved.GroupID)
	if group == nil {
		return fault.ErrIdentityMismatch
	}
	var firstError error
	for _, replica := range group.Replicas {
		if replica.Role == topology.ReplicaVoter {
			if _, err := a.callNode(ctx, replica.NodeName, "Effect", approved.GroupID, replica.ReplicaID, approved); err != nil {
				if firstError == nil {
					firstError = err
				}
			}
		}
	}
	return firstError
}
func (a *application) PrepareReplica(ctx context.Context, approved topology.AuthorizedStep) error {
	_, err := a.callNode(ctx, approved.NodeName, "Effect", approved.GroupID, approved.ReplicaID, approved)
	return err
}
func (a *application) ApplyMembership(ctx context.Context, approved topology.AuthorizedStep) error {
	return a.leaderEffect(ctx, approved)
}
func (a *application) TransferLeader(ctx context.Context, approved topology.AuthorizedStep) error {
	return a.leaderEffect(ctx, approved)
}
func (a *application) leaderEffect(ctx context.Context, approved topology.AuthorizedStep) error {
	if err := a.meta.Authorize(ctx, approved); err != nil {
		return err
	}
	status, err := a.Status(ctx, approved.GroupID)
	if err != nil {
		return err
	}
	if status.LeaderID == 0 {
		return fault.ErrUnavailable
	}
	name, err := a.owner(ctx, approved.GroupID, status.LeaderID)
	if err != nil {
		return err
	}
	_, err = a.callNode(ctx, name, "Effect", approved.GroupID, status.LeaderID, approved)
	return err
}
func (a *application) StopReplica(ctx context.Context, approved topology.AuthorizedStep) error {
	_, err := a.callNode(ctx, approved.NodeName, "Effect", approved.GroupID, approved.ReplicaID, approved)
	return err
}
func (a *application) CleanupReplicaState(ctx context.Context, approved topology.AuthorizedStep) error {
	_, err := a.callNode(ctx, approved.NodeName, "Effect", approved.GroupID, approved.ReplicaID, approved)
	return err
}
func (a *application) DeleteGroupState(ctx context.Context, approved topology.AuthorizedStep) error {
	if err := a.meta.Authorize(ctx, approved); err != nil {
		return err
	}
	target, active, err := a.documents(ctx)
	if err != nil {
		return err
	}
	names := map[string]bool{}
	for _, doc := range []*topology.Document{&target, &active} {
		if group := doc.Group(approved.GroupID); group != nil {
			for _, r := range group.Replicas {
				names[r.NodeName] = true
			}
		}
	}
	// Include the immediately preceding desired document for prepared replicas
	// from an interrupted execution that never became Active.
	if approved.Version > 1 {
		if previous, err := a.meta.Get(ctx, approved.Version-1); err == nil {
			if group := previous.Group(approved.GroupID); group != nil {
				for _, r := range group.Replicas {
					names[r.NodeName] = true
				}
			}
		}
	}
	for name := range names {
		if _, err := a.callNode(ctx, name, "Effect", approved.GroupID, 0, approved); err != nil {
			return err
		}
	}
	return nil
}

func (a *application) takeLocal(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case a.localGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (a *application) openLocal(ctx context.Context, id string, replica uint64, mode load.Mode, peers []raft.Peer) (*localReplica, error) {
	if err := a.takeLocal(ctx); err != nil {
		return nil, err
	}
	defer func() { <-a.localGate }()
	if g, exists := a.runtime.Load(id); exists {
		if g.ReplicaID() != replica {
			return nil, fault.ErrIdentityMismatch
		}
		a.mu.RLock()
		local := a.locals[id]
		a.mu.RUnlock()
		if local == nil || local.group != g {
			return nil, fault.ErrIdentityMismatch
		}
		if g.Running() {
			return local, nil
		}
		if err := g.Stop(ctx); err != nil {
			return nil, err
		}
		if err := a.runtime.StopLoad(ctx, id, replica); err != nil {
			return nil, err
		}
	}
	machine := newMachine()
	g, err := a.runtime.OpenLoad(load.Options{GroupID: id, NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, ReplicaID: replica, Mode: mode, Peers: peers, Store: a.clusterStore(a.store, id, replica), Machine: machine, Transport: loadTransport{app: a, group: id, replica: replica}, Timing: load.Timing{TickInterval: a.cfg.TickInterval, ElectionTicks: 10, WALWriteWait: a.cfg.WALWriteWait}, SnapshotThreshold: a.cfg.SnapshotThreshold})
	if err != nil {
		return nil, err
	}
	local := &localReplica{group: g, machine: machine}
	a.mu.Lock()
	a.locals[id] = local
	a.mu.Unlock()
	if err = g.Start(a.lifeCtx); err == nil {
		err = g.WaitReady(ctx)
	}
	if err != nil {
		_ = a.runtime.StopLoad(context.Background(), id, replica)
		a.mu.Lock()
		if a.locals[id] == local {
			delete(a.locals, id)
		}
		a.mu.Unlock()
		return nil, err
	}
	return local, nil
}
func (a *application) stopLocal(ctx context.Context, id string, expected uint64) error {
	if err := a.takeLocal(ctx); err != nil {
		return err
	}
	defer func() { <-a.localGate }()
	if g, exists := a.runtime.Load(id); exists && g.ReplicaID() != expected {
		// An exact approved replacement proves the old handle is gone. A stale
		// cleanup must acknowledge that result without stopping its successor.
		if _, err := a.meta.LoadReplicaScope(ctx, storage.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, GroupID: id, ReplicaID: g.ReplicaID()}); err != nil {
			return err
		}
		return nil
	}
	if err := a.runtime.StopLoad(ctx, id, expected); err != nil {
		return err
	}
	a.mu.Lock()
	if old := a.locals[id]; old != nil && old.group.ReplicaID() == expected {
		delete(a.locals, id)
	}
	a.mu.Unlock()
	return nil
}
func (a *application) receiveEffect(ctx context.Context, approved topology.AuthorizedStep, receiver uint64) (err error) {
	if err := a.meta.Authorize(ctx, approved); err != nil {
		return err
	}
	if err := a.beforeClusterEffect(ctx, approved); err != nil {
		return err
	}
	defer func() { a.afterClusterEffect(approved, err) }()
	if err := a.meta.Authorize(ctx, approved); err != nil {
		return err
	}
	switch approved.Kind {
	case topology.StepCreateFoundingGroup:
		target, err := a.meta.Target(ctx)
		if err != nil {
			return err
		}
		group := target.Group(approved.GroupID)
		if group == nil {
			return fault.ErrIdentityMismatch
		}
		var peers []raft.Peer
		localVoter := false
		for _, r := range group.Replicas {
			if r.Role == topology.ReplicaVoter {
				peers = append(peers, raft.Peer{ID: r.ReplicaID})
				if r.NodeName == a.cfg.NodeName && r.ReplicaID == receiver {
					localVoter = true
				}
			}
		}
		if !localVoter {
			return fault.ErrIdentityMismatch
		}
		local, err := a.openLocal(ctx, approved.GroupID, receiver, load.Founding, peers)
		if err != nil {
			return err
		}
		cs := local.group.Status().ConfState
		got := slices.Clone(cs.Voters)
		want := make([]uint64, len(peers))
		for i, p := range peers {
			want[i] = p.ID
		}
		slices.Sort(got)
		slices.Sort(want)
		if len(cs.VotersOutgoing)+len(cs.Learners)+len(cs.LearnersNext) != 0 || slices.ContainsFunc(got, func(id uint64) bool { return !slices.Contains(want, id) }) {
			return fault.ErrIdentityMismatch
		}
		// Startup readiness precedes commitment of the founding ConfChanges.
		// Start every founder even if an earlier one is awaiting its peers, then
		// retry and attest the complete exact voter set before acknowledging.
		if !slices.Equal(got, want) {
			return fault.ErrUnavailable
		}
		return nil
	case topology.StepPrepareReplica:
		if approved.NodeName != a.cfg.NodeName || approved.ReplicaID != receiver {
			return fault.ErrIdentityMismatch
		}
		_, err := a.openLocal(ctx, approved.GroupID, receiver, load.Joining, nil)
		return err
	case topology.StepStopReplica:
		if approved.NodeName != a.cfg.NodeName || approved.ReplicaID != receiver {
			return fault.ErrIdentityMismatch
		}
		return a.stopLocal(ctx, approved.GroupID, receiver)
	case topology.StepCleanupReplicaState:
		if approved.NodeName != a.cfg.NodeName || approved.ReplicaID != receiver {
			return fault.ErrIdentityMismatch
		}
		return a.cleanupLocal(ctx, approved, receiver)
	case topology.StepDeleteGroupState:
		if receiver != 0 {
			return fault.ErrIdentityMismatch
		}
		record, found, err := a.meta.LookupLoadReplica(ctx, approved.GroupID)
		if err != nil {
			return err
		}
		if !found {
			probe, err := a.store.Probe(ctx, approved.GroupID)
			if err != nil {
				return err
			}
			if probe.Exists {
				return fault.ErrIdentityMismatch
			}
			return nil
		}
		return a.cleanupLocal(ctx, approved, record.ReplicaID)
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica, topology.StepTransferLeader:
		g, exists := a.runtime.Load(approved.GroupID)
		if !exists || !g.Running() {
			return fault.ErrReplicaNotReady
		}
		if g.ReplicaID() != receiver {
			return fault.ErrIdentityMismatch
		}
		if !g.IsLeader() {
			return fault.ErrNotLeader
		}
		if approved.Kind == topology.StepTransferLeader {
			return g.TransferLeader(ctx, approved.ReplicaID)
		}
		status, err := a.Status(ctx, approved.GroupID)
		if err != nil {
			return err
		}
		if err := a.meta.Authorize(ctx, approved); err != nil {
			return err
		}
		var change raftpb.ConfChangeV2
		switch approved.Kind {
		case topology.StepAddLearner:
			change = membership.AddLearner(approved.ReplicaID)
		case topology.StepPromoteVoter:
			change = membership.PromoteLearner(approved.ReplicaID)
		case topology.StepDemoteVoter:
			change = membership.DemoteVoter(approved.ReplicaID)
		case topology.StepRemoveReplica:
			change = membership.RemoveReplica(approved.ReplicaID)
		}
		return a.coordinator.Apply(ctx, g.Membership(), observedLive{approved.GroupID, status.LiveVoters}, change)
	default:
		return fault.ErrUnsupported
	}
}

type observedLive struct {
	group string
	ids   map[uint64]bool
}

func (h observedLive) Live(group string) (map[uint64]bool, bool) { return h.ids, group == h.group }

func (a *application) cleanupLocal(ctx context.Context, approved topology.AuthorizedStep, expected uint64) error {
	if err := a.stopLocal(ctx, approved.GroupID, expected); err != nil {
		return err
	}
	probe, err := a.store.Probe(ctx, approved.GroupID)
	if err != nil {
		return err
	}
	want := storage.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, GroupID: approved.GroupID, ReplicaID: expected}
	if probe.Exists && probe.Identity != want {
		if approved.Kind == topology.StepDeleteGroupState {
			return fault.ErrIdentityMismatch
		}
		if _, err := a.meta.LoadReplicaScope(ctx, probe.Identity); err != nil {
			return err
		}
		return nil
	}
	if _, err := a.store.Quarantine(ctx, approved.GroupID, expected); err != nil {
		return err
	}
	return a.meta.ForgetLoadReplica(ctx, approved, expected)
}

func (a *application) readmission(ctx context.Context, change meta.Readmission) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	own := a.meta.Identity()
	before, after := change.Previous, change.Granted
	path := filepath.Join(a.cfg.DataDir, "meta.bolt")
	if before.NodeName != a.cfg.NodeName || after.NodeName != a.cfg.NodeName || before.ClusterID != a.cfg.ClusterID || after.ClusterID != a.cfg.ClusterID || before.MemberID == 0 || after.MemberID == 0 || before.MemberID == after.MemberID || change.AdmissionID == "" || change.MetaDBPath != path || own.MemberID != 0 && own.MemberID != before.MemberID {
		return fault.ErrIdentityMismatch
	}
	if err := a.takeLocal(ctx); err != nil {
		return err
	}
	defer func() { <-a.localGate }()
	for _, group := range change.PreviousGroups {
		if group.ID == topology.MetaGroupID {
			continue
		}
		if err := topology.ValidateLoadID(group.ID); err != nil {
			return err
		}
		previousIDs := map[uint64]bool{}
		for _, old := range group.Replicas {
			if old.NodeName != a.cfg.NodeName || old.ReplicaID == 0 {
				return fault.ErrIdentityMismatch
			}
			previousIDs[old.ReplicaID] = true
		}
		for _, granted := range change.GrantedGroups {
			if granted.ID != group.ID {
				continue
			}
			for _, fresh := range granted.Replicas {
				if fresh.NodeName == a.cfg.NodeName && previousIDs[fresh.ReplicaID] {
					return fault.ErrIdentityMismatch
				}
			}
		}
		// PreviousGroups can contain both Active and interrupted Target IDs.
		// Stop and quarantine the actual retained identity once, after verifying
		// its membership in that exact catalog; ordering must not select an ID.
		if local, found := a.runtime.Load(group.ID); found {
			if !previousIDs[local.ReplicaID()] {
				return fault.ErrIdentityMismatch
			}
			if err := a.runtime.StopLoad(ctx, group.ID, local.ReplicaID()); err != nil {
				return err
			}
			a.mu.Lock()
			delete(a.locals, group.ID)
			a.mu.Unlock()
		}
		probe, err := a.store.Probe(ctx, group.ID)
		if err != nil {
			return err
		}
		if !probe.Exists {
			continue
		}
		want := storage.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, GroupID: group.ID, ReplicaID: probe.Identity.ReplicaID}
		if probe.Identity != want || !previousIDs[probe.Identity.ReplicaID] {
			return fault.ErrIdentityMismatch
		}
		if _, err := a.store.Quarantine(ctx, group.ID, probe.Identity.ReplicaID); err != nil {
			return err
		}
	}
	stored, found, err := meta.LoadStoredIdentity(path)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !found {
		return nil
	}
	if stored.NodeName != before.NodeName || stored.ClusterID != before.ClusterID || stored.MemberID != before.MemberID {
		return fault.ErrIdentityMismatch
	}
	nonce := sha256.Sum256([]byte(change.AdmissionID))
	backup := path + fmt.Sprintf(".retired-%d-", before.MemberID) + hex.EncodeToString(nonce[:8])
	if _, err := os.Stat(backup); err == nil {
		return fault.New(fault.IdentityMismatch, "readmission quarantine destination already exists", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(path, backup)
}
