//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Approval refusal is a single-service contract, independent of quorum size.
func TestMutationApprovalFailureReturnsDurablyStagedDocument(t *testing.T) {
	t.Parallel()
	leader := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := leader.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{ExpectedVersion: 1, Actor: "operator", Apply: true})
	if !errors.Is(err, fault.ErrNoController) || result.Document.Version != 2 || result.OperatorID != "" {
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

func TestLocalMetaStatusDoesNotWaitForCallerLoadController(t *testing.T) {
	t.Parallel()
	controller := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) { o.LoadGroups = controller }))
	controller.mu.Lock()
	result := make(chan topology.GroupStatus, 1)
	go func() { result <- g.LocalStatus() }()
	var status topology.GroupStatus
	select {
	case status = <-result:
		controller.mu.Unlock()
	case <-time.After(time.Second):
		controller.mu.Unlock()
		<-result
		t.Fatal("local meta observation waited on caller controller")
	}
	if !status.Found || status.HostNode != g.Identity().NodeName || status.GroupID != topology.MetaGroupID || status.Term == 0 || status.LeaderID != g.Identity().MemberID || len(status.ConfState.Voters) != 1 {
		t.Fatal("local status lost native identity", status)
	}
	status.ConfState.Voters[0] = 999
	if g.LocalStatus().ConfState.Voters[0] == 999 {
		t.Fatal("local status aliased engine membership")
	}
}
