//go:build functional

package metaservice

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMutationAcceptanceSurvivesLaterStageAndExactReplay(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	spec := GroupSpec{ID: "work"}
	options := MutationOptions{ExpectedVersion: 1, Actor: "operator"}
	accepted, err := g.CreateLoadGroup(ctx, spec, options)
	if err != nil || accepted.Document.Version != 2 {
		t.Fatal(accepted, err)
	}
	next := accepted.Document.Clone()
	next.Comment, next.RebasedFrom = "unrelated later stage", 999
	later, err := g.Stage(ctx, next, accepted.Document.Version)
	if err != nil || later.Version != 3 || later.RebasedFrom != 0 {
		t.Fatal("raw Stage retained forged generated provenance", later, err)
	}
	replay, err := g.CreateLoadGroup(ctx, spec, options)
	if err != nil || !reflect.DeepEqual(replay.Document, accepted.Document) || replay.OperationID != "" ||
		!reflect.DeepEqual(g.state().Latest(), later) || g.state().Target().Version != 1 {
		t.Fatal("retry lost its original acceptance or changed newer intent", replay, err)
	}
	options.Actor = "different-actor"
	if _, err := g.CreateLoadGroup(ctx, spec, options); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("changed intent reused an acceptance", err)
	}
}

// Approval refusal is a single-service contract, independent of quorum size.
func TestMutationApprovalFailureReturnsDurablyStagedDocument(t *testing.T) {
	t.Parallel()
	leader := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := leader.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{ExpectedVersion: 1, Actor: "operator", Apply: true})
	if !errors.Is(err, fault.ErrNoController) || result.Document.Version != 2 || result.OperationID != "" ||
		result.RebasedTo != 0 {
		t.Fatal("lost staged document on approval failure", result, err)
	}
	views := leader.state().Views()
	latest, target := views.Latest, views.Target
	if latest.Version != 2 || target.Version != 1 || views.FullyConverged.Version != 1 {
		t.Fatal("failed approval changed Target", latest.Version, target.Version, views.FullyConverged.Version)
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
