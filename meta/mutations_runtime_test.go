package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMutationApprovalFailureReturnsDurablyStagedDocument(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := leader.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{ExpectedVersion: 1, Actor: "operator", Apply: true})
	if !errors.Is(err, fault.ErrNoController) || result.Document.Version != 2 || result.ExecutionID != "" {
		t.Fatal("lost staged document on approval failure", result, err)
	}
	latest, target, active := leader.state().Views()
	if latest.Version != 2 || target.Version != 1 || active.Version != 1 {
		t.Fatal("failed approval changed Target", latest.Version, target.Version, active.Version)
	}
	if _, err = leader.CreateLoadGroup(ctx, GroupSpec{ID: "another"}, MutationOptions{ExpectedVersion: 1}); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("stale edit overwrote durable stage", err)
	}
}
func TestPublicLoadGroupCreateDeleteAndRetiredID(t *testing.T) {
	controller := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}
	groups := publicFoundingClusterWith(t, 3, func(o *Options) { o.LoadGroups = controller })
	leader := readyPublic(t, groups)
	controller.mu.Lock()
	controller.meta = groups
	controller.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	result, err := leader.CreateLoadGroup(ctx, GroupSpec{ID: "work", Nodes: []string{"n1"}}, MutationOptions{ExpectedVersion: 1, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := leader.WaitExecution(ctx, result.ExecutionID)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == 2 })
	result, err = leader.DeleteLoadGroup(ctx, "work", MutationOptions{ExpectedVersion: 2, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = leader.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{Preview: true}); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal("approved deletion did not immediately retire ID", err)
	}
	status, err = leader.WaitExecution(ctx, result.ExecutionID)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == 3 })
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if len(controller.hosts) != 0 || len(controller.groups) != 0 {
		t.Fatal("deletion left caller state")
	}
}
func TestPublicTransferMetaLeaderFromFollowerUsesActiveScope(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var target *Group
	for _, g := range groups {
		if g != leader {
			target = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := target.TransferLeader(ctx, topology.MetaGroupID, target.currentIdentity().NodeName); err != nil {
		t.Fatal(err)
	}
	waitPublic(t, target.IsLeader)
	if target.state().Latest().Version != 1 {
		t.Fatal("immediate transfer created document")
	}
}
