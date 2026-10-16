//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestOperationPlanningCapturesOnlyAffectedNativeAndRechecksScope(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stable", "unrelated approval", "owner", "declaration", "deletion"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newMutationFixture(t)
			target := f.g.machine.Target()
			target.Version++
			target.Group("work").Labels = map[string]string{"owner": "jobs"}
			if mode == "deletion" {
				target.Groups = slices.DeleteFunc(target.Groups, func(g topology.Group) bool { return g.ID == "work" })
			}
			c := &scopedPlanningController{statusController: &statusController{value: topology.GroupStatus{GroupID: "work", Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}}
			f.g.opts.LoadGroups = c
			reads := 0
			read := func(ctx context.Context, q metaengine.QueryRequest) (metaengine.PlanRead, error) {
				reads++
				input, err := f.g.machine.PlanningInput(ctx, target, q.Native)
				if err != nil {
					return metaengine.PlanRead{}, err
				}
				result := metaengine.PlanRead{Input: input, Target: target.Clone(), Scopes: []metaengine.ScopeExpectation{{GroupID: "work", SourceVersion: 1}}}
				if reads == 2 {
					switch mode {
					case "unrelated approval":
						result.Input.Approved.Version++
					case "owner":
						result.Scopes[0].Owner = "exec-v2-a1"
					case "declaration":
						result.Input.Approved.Group("work").Labels = map[string]string{"owner": "other"}
					}
				}
				return result, nil
			}
			got, err := f.g.capturePlan(t.Context(), metaengine.QueryRequest{Kind: "approval_plan", Version: target.Version}, read)
			if mode == "owner" || mode == "declaration" {
				if !errors.Is(err, fault.ErrVersionConflict) {
					t.Fatal("stale capture accepted", err)
				}
				return
			}
			if err != nil || reads != 2 {
				t.Fatal(got, err, reads)
			}
			if mode == "deletion" {
				if len(c.reads) != 0 {
					t.Fatal("deletion read lost quorum", c.reads)
				}
			} else if !slices.Equal(c.reads, []string{"work"}) {
				t.Fatal("contacted sibling", c.reads)
			}
			plan, err := f.g.planOperation(got)
			if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != "work" {
				t.Fatal(plan, err)
			}
		})
	}
}

func TestOperationScopeIgnoresSiblingAndOwnRunningTransition(t *testing.T) {
	t.Parallel()
	before := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1, Blocks: []topology.BlockStatus{{GroupID: "a", State: topology.OperationPending}, {GroupID: "b", State: topology.OperationRunning, CurrentStep: 2}}}, Plan: topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: "a", Steps: []int{1, 3}}, {GroupID: "b", Steps: []int{2}}}}}
	after := before.Clone()
	after.Status.Blocks[0].State = topology.OperationRunning
	after.Status.Blocks[0].CurrentStep = 1
	after.Status.Blocks[1].State = topology.OperationFailed
	if err := sameOperationScope(before, after, "a"); err != nil {
		t.Fatal("sibling or own running transition invalidated work", err)
	}
	after.Status.Blocks[0].CurrentStep = 3
	if err := sameOperationScope(before, after, "a"); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("advanced cursor accepted", err)
	}
}

func TestOperationForwardingPreservesKindAndExactOutcome(t *testing.T) {
	t.Parallel()
	for _, request := range []applyVersionRequest{{Kind: "approve", Version: 9}, {Kind: "retry", Previous: "exec-v9-a1"}} {
		for _, mode := range []string{"accepted", "lost reply", "conflict", "bad reply"} {
			t.Run(request.Kind+"/"+mode, func(t *testing.T) {
				t.Parallel()
				calls := 0
				want := topology.OperationID("exec-v9-a2")
				call := func(ctx context.Context, method string, body any) ([]byte, error) {
					calls++
					encoded, _ := json.Marshal(body)
					got, err := decodeApplyVersionRequest(encoded)
					if err != nil || got != request || method != "ApplyVersion" || ctx != t.Context() {
						t.Fatal(got, method, err)
					}
					switch mode {
					case "lost reply":
						return nil, context.DeadlineExceeded
					case "conflict":
						return nil, metatransport.ReplyError(metatransport.Reply(&fault.Error{Code: fault.VersionConflict, ExpectedVersion: 9, CurrentVersion: 11, RebasedTo: 10}))
					case "bad reply":
						return []byte(`""`), nil
					}
					return json.Marshal(want)
				}
				got, err := forwardOperation(t.Context(), request, call)
				if calls != 1 {
					t.Fatal("implicit retry", calls)
				}
				switch mode {
				case "accepted":
					if err != nil || got != want {
						t.Fatal(got, err)
					}
				case "lost reply":
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				case "conflict":
					var detail *fault.Error
					if !errors.As(err, &detail) || detail.RebasedTo != 10 || detail.CurrentVersion != 11 {
						t.Fatal(err)
					}
				case "bad reply":
					if !errors.Is(err, fault.ErrUnreadable) {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestOperationServicesRemainClosedBeforeScopedPersistence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := singleMetaFixtureWith(t, func(*Options) {})[0]
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	before := g.machine.Latest().Version
	for _, request := range []applyVersionRequest{{Kind: "approve", Version: before}, {Kind: "retry", Previous: "exec-v1-a1"}} {
		data, _ := json.Marshal(request)
		if err := metatransport.ReplyError(g.applyVersionReply(ctx, data)); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("scoped route reached schema3 mutation", err)
		}
	}
	if _, err := g.Approve(ctx, before); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := g.RetryOperation(ctx, "exec-v1-a1"); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := g.Operation(ctx, "exec-v1-a1"); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if g.machine.Latest().Version != before {
		t.Fatal("closed route wrote state")
	}
	controller, err := operator.New(g.OperationControllerConfig())
	if err != nil {
		t.Fatal("final scoped wiring is incomplete", err)
	}
	_ = controller
}
