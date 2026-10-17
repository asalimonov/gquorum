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
						result.Target.Version++
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

func TestOperationServicesUseScopedPersistence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := readyPublic(t, singleMetaFixture(t))
	initial := g.machine.Latest().Version
	request := func(value applyVersionRequest) (topology.OperationID, error) {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		reply := g.applyVersionReply(ctx, data)
		if err := metatransport.ReplyError(reply); err != nil {
			return "", err
		}
		var id topology.OperationID
		if err := json.Unmarshal(reply.Data, &id); err != nil {
			t.Fatal(err)
		}
		return id, nil
	}
	founding := topology.MakeOperationID(initial, 1)
	if id, err := request(applyVersionRequest{Kind: "approve", Version: initial}); err != nil || id != founding {
		t.Fatal("approval replay lost the original operation", id, err)
	}
	if _, err := request(applyVersionRequest{Kind: "retry", Previous: founding}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("succeeded operation became retryable", err)
	}
	desired := g.machine.Latest()
	desired.Node("n1").Labels = map[string]string{"scope": "current-format"}
	staged, err := g.Stage(ctx, desired, initial)
	if err != nil {
		t.Fatal(err)
	}
	id, err := request(applyVersionRequest{Kind: "approve", Version: staged.Version})
	if err != nil {
		t.Fatal(err)
	}
	status, err := g.WaitOperation(ctx, id)
	if err != nil || status.State != topology.OperationSucceeded || len(status.Blocks) != 1 || status.Blocks[0].GroupID != topology.MetaGroupID {
		t.Fatal("approved operation did not converge through scoped composition", status, err)
	}
	page, err := g.ReadProgress(ctx, topology.ProgressQuery{GroupID: topology.MetaGroupID})
	if err != nil || len(page.Groups) != 1 || page.Groups[0].ConvergedVersion != staged.Version || page.FullyConverged.Version != staged.Version {
		t.Fatal("progress lost committed completion", page, err)
	}
	if again, err := g.Approve(ctx, staged.Version); err != nil || again != id {
		t.Fatal("approval retry created another attempt", again, err)
	}
	// No affected scopes is a valid approved document. Its explicit empty
	// capture must survive the same JSON read/proposal path as nonempty plans.
	comment := g.machine.Latest()
	comment.Comment = "annotation-only approval"
	staged, err = g.Stage(ctx, comment, comment.Version)
	if err != nil {
		t.Fatal(err)
	}
	emptyID, err := g.Approve(ctx, staged.Version)
	if err != nil {
		t.Fatal("approve empty scope capture", err)
	}
	empty, err := g.WaitOperation(ctx, emptyID)
	if err != nil || empty.State != topology.OperationSucceeded || len(empty.Blocks) != 0 {
		t.Fatal("annotation-only approval fabricated a block or failed", empty, err)
	}
	if replay, err := g.Approve(ctx, staged.Version); err != nil || replay != emptyID {
		t.Fatal("empty approval replay changed identity", replay, err)
	}
	if _, err := operator.New(g.OperationControllerConfig()); err != nil {
		t.Fatal("scoped controller composition is incomplete", err)
	}
}
