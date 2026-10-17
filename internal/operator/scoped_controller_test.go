//go:build !functional

package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedControllerRejectsInvalidComposition(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Config){
		"coordinator":         func(c *Config) { c.Coordinator = nil },
		"rules":               func(c *Config) { c.Rules.SchemaVersion = 0 },
		"limits":              func(c *Config) { c.Rules.Limits.MaxNodes = -1 },
		"authority":           func(c *Config) { c.Authority = nil },
		"effects":             func(c *Config) { c.Groups = nil },
		"owner":               func(c *Config) { c.OwnsGroup = nil },
		"interval":            func(c *Config) { c.Interval = -time.Second },
		"retry":               func(c *Config) { c.MaxRetryInterval = -time.Second },
		"proposal timeout":    func(c *Config) { c.ProposalTimeout = -time.Second },
		"missing scope plan":  func(c *Config) { c.ScopedPlanValidator = nil },
		"missing scope final": func(c *Config) { c.ScopedFinalValidator = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := scopedControllerConfig(&scopedCoordinatorTest{leader: true})
			change(&cfg)
			if _, err := New(cfg); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
}

func TestScopedControllerFinalizesOnlySelectedScopeAndRechecksLeadership(t *testing.T) {
	t.Parallel()
	for _, loss := range []string{"none", "selection", "plan", "final", "canceled", "other owner"} {
		t.Run(loss, func(t *testing.T) {
			t.Parallel()
			op := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1,
				State: topology.OperationRunning, TotalBlocks: 2, Blocks: []topology.BlockStatus{
					{GroupID: "a", State: topology.OperationFailed}, {GroupID: "b", State: topology.OperationRunning}}},
				Plan:      topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: "a"}, {GroupID: "b"}}},
				UpdatedAt: time.Now().Add(time.Hour)}
			coord := &scopedCoordinatorTest{leader: true, work: []WorkItem{{Operation: op, GroupID: "b"}}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if loss == "selection" {
				coord.onSelect = func() { coord.leader = false }
			}
			cfg := scopedControllerConfig(coord)
			planCalls, finalCalls := 0, 0
			if loss == "other owner" {
				cfg.OwnsGroup = func(string) bool { return false }
			}
			cfg.ScopedPlanValidator = func(_ context.Context, got topology.Operation, group string) error {
				planCalls++
				if got.Status.ID != op.Status.ID || group != "b" {
					t.Fatal(got, group)
				}
				got.Plan.Blocks[1].GroupID = "caller"
				if loss == "plan" {
					coord.leader = false
				}
				return nil
			}
			cfg.ScopedFinalValidator = func(_ context.Context, got topology.Operation, group string) error {
				finalCalls++
				if got.Plan.Blocks[1].GroupID != "b" || group != "b" {
					t.Fatal("validator aliased work", got)
				}
				if loss == "final" {
					coord.leader = false
				}
				if loss == "canceled" {
					cancel()
				}
				return nil
			}
			controller, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			work, readErr := controller.readScopedWork(ctx, 1)
			if readErr == nil && len(work) != 0 {
				controller.runScopedWork(ctx, work[0])
			}
			if loss != "none" {
				if len(coord.transitions) != 0 {
					t.Fatal("recorded after leadership/cancellation", coord.transitions)
				}
				return
			}
			if planCalls != 1 || finalCalls != 1 || len(coord.transitions) != 1 {
				t.Fatal(planCalls, finalCalls, coord.transitions)
			}
			got := coord.transitions[0]
			if got.OperationID != op.Status.ID || got.Version != 2 || got.Attempt != 1 || got.GroupID != "b" ||
				got.StepSeq != 0 || got.State != topology.OperationSucceeded || got.At.Before(op.UpdatedAt) {
				t.Fatal(got)
			}
		})
	}
}

func TestScopedControllerRecordsOnlySelectedBlockRefusal(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{"waiting": fault.ErrUnavailable, "failed": fault.ErrReplanRequired} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			op := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1,
				State: topology.OperationRunning, TotalBlocks: 1, Blocks: []topology.BlockStatus{{GroupID: "a", State: topology.OperationRunning}}},
				Plan: topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: "a"}}}}
			coord := &scopedCoordinatorTest{leader: true, work: []WorkItem{{Operation: op, GroupID: "a"}}}
			cfg := scopedControllerConfig(coord)
			cfg.ScopedPlanValidator = func(context.Context, topology.Operation, string) error { return failure }
			cfg.ScopedFinalValidator = func(context.Context, topology.Operation, string) error {
				t.Fatal("final validator after refusal")
				return nil
			}
			controller, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			controller.runScopedWork(t.Context(), coord.work[0])
			if len(coord.transitions) != 1 {
				t.Fatal(coord.transitions)
			}
			got := coord.transitions[0]
			if got.GroupID != "a" || got.OperationID != op.Status.ID || got.Version != 2 || got.Attempt != 1 {
				t.Fatal(got)
			}
			if name == "waiting" {
				if !got.State.Waiting() || got.Blocked == nil || got.Blocked.Code != topology.BlockedQuorumUnavailable || got.Error != "" {
					t.Fatal(got)
				}
			} else if got.State != topology.OperationFailed || got.ErrorCode != string(fault.ReplanRequired) || got.Error == "" || got.Blocked != nil {
				t.Fatal(got)
			}
		})
	}
}
