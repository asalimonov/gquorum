//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func progressCount(page topology.ProgressPage) int {
	count := len(page.Groups) + len(page.Nodes) + len(page.Cleanup)
	for _, op := range page.Operations {
		count += len(op.Blocks)
	}
	return count
}

func TestProgressPagesOneBudgetAndOwnedCoherentRows(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	prepareScopedFixture(t, &s)
	s.AppliedIndex = 91
	before, _ := json.Marshal(s)
	all, err := queryScopedProgress(&s, topology.ProgressQuery{Limit: 1000})
	if err != nil || all.AppliedIndex != 91 || all.LatestVersion != s.Latest.Version ||
		all.TargetVersion != s.Target.Version || all.FullyConverged != s.FullyConverged ||
		all.ReadAt.IsZero() || all.Next != "" {
		t.Fatal("incoherent page", all, err)
	}
	var groups, nodes, blocks, debts []string
	query := topology.ProgressQuery{Limit: 1}
	for {
		page, err := queryScopedProgress(&s, query)
		if err != nil || progressCount(page) != 1 {
			t.Fatal("page exceeded its single shared budget", page, err)
		}
		for _, row := range page.Groups {
			if len(nodes)+len(blocks)+len(debts) != 0 {
				t.Fatal("group came after later kind")
			}
			groups = append(groups, row.GroupID)
		}
		for _, row := range page.Nodes {
			if len(blocks)+len(debts) != 0 {
				t.Fatal("node came after later kind")
			}
			nodes = append(nodes, row.NodeName)
		}
		for _, row := range page.Operations {
			if len(debts) != 0 {
				t.Fatal("operation came after cleanup")
			}
			blocks = append(blocks, string(row.ID)+"/"+row.Blocks[0].GroupID)
		}
		for _, row := range page.Cleanup {
			debts = append(debts, row.Identity.GroupID)
		}
		if page.Next == "" {
			break
		}
		query.After = page.Next
	}
	if len(groups)+len(nodes)+len(blocks)+len(debts) != progressCount(all) ||
		!slices.IsSorted(groups) || !slices.IsSorted(nodes) || !slices.IsSorted(blocks) || !slices.IsSorted(debts) {
		t.Fatal("unstable union ordering or missing records", groups, nodes, blocks, debts)
	}
	for i := range all.Groups {
		for _, declaration := range []*topology.Group{all.Groups[i].Desired, all.Groups[i].Approved, all.Groups[i].Converged} {
			if declaration != nil {
				declaration.Replicas[0].NodeName = "caller"
			}
		}
	}
	for i := range all.Nodes {
		if all.Nodes[i].Converged != nil {
			all.Nodes[i].Converged.Name = "caller"
		}
	}
	for i := range all.Cleanup {
		if all.Cleanup[i].RetiredAt != nil {
			*all.Cleanup[i].RetiredAt = time.Time{}
		}
		if all.Cleanup[i].CollectedAt != nil {
			*all.Cleanup[i].CollectedAt = time.Time{}
		}
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("progress exposed borrowed state")
	}
	encoded, _ := json.Marshal(all)
	for _, secret := range []string{`"token"`, `"retirement_key"`, `"grant"`} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("ordinary progress exposed proof", secret)
		}
	}
}

func TestProgressHugeOperationCannotEscapeBudget(t *testing.T) {
	t.Parallel()
	// Projection-only fixture: no effects execute. The complete aggregate remains
	// running while the first block failed, even when that is the only returned block.
	s := scopedState{Latest: topology.Document{ClusterID: "c", Version: 2},
		Target: topology.Document{ClusterID: "c", Version: 2}, AppliedIndex: 19}
	op := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2,
		Attempt: 1, State: topology.OperationRunning, TotalBlocks: 2003}, Plan: topology.Plan{Version: 2}}
	for i := range 2003 {
		group := fmt.Sprintf("g%04d", i)
		op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: group})
		op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: group, State: topology.OperationRunning,
			Blocked: &topology.BlockedReason{Code: topology.BlockedDependency}})
	}
	op.Status.Blocks[0].State = topology.OperationFailed
	s.Operations = []topology.Operation{op}
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 0, 100, 1000} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			query := topology.ProgressQuery{Limit: limit}
			wantLimit := limit
			if wantLimit == 0 {
				wantLimit = 100
			}
			seen := 0
			for {
				page, err := queryScopedProgress(&s, query)
				if err != nil || progressCount(page) != min(wantLimit, 2003-seen) || len(page.Operations) != 1 {
					t.Fatal("huge operation escaped budget", progressCount(page), err)
				}
				status := page.Operations[0]
				if status.TotalBlocks != 2003 || !status.BlocksTruncated || status.State != topology.OperationRunning {
					t.Fatal("partial blocks recomputed aggregate or hid truncation", status)
				}
				for _, block := range status.Blocks {
					if block.GroupID != fmt.Sprintf("g%04d", seen) {
						t.Fatal("skipped/repeated block", block, seen)
					}
					block.Blocked.Code = "caller"
					seen++
				}
				if page.Next == "" {
					break
				}
				query.After = page.Next
			}
			if seen != 2003 || s.Operations[0].Status.Blocks[0].Blocked.Code == "caller" {
				t.Fatal("lost block or borrowed blocker", seen)
			}
		})
	}
}

func TestProgressContinuationObservesNewIndexWithoutSnapshotLease(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	prepareScopedFixture(t, &s)
	first, err := queryScopedProgress(&s, topology.ProgressQuery{Limit: 1})
	if err != nil || first.Groups[0].GroupID != topology.MetaGroupID {
		t.Fatal(first, err)
	}
	// An apply between reads can remove the cursor record and add both before and
	// after it. Continuation is an exclusive key, not a position or pinned version.
	applied := make(chan error, 1)
	go func() {
		s.Groups = slices.DeleteFunc(s.Groups, func(row groupConvergence) bool { return row.GroupID == topology.MetaGroupID })
		for _, doc := range []*topology.Document{&s.Latest, &s.Target} {
			doc.Groups = slices.DeleteFunc(doc.Groups, func(group topology.Group) bool { return group.ID == topology.MetaGroupID })
		}
		s.Groups = append(s.Groups, groupConvergence{GroupID: "0"}, groupConvergence{GroupID: "zzz"})
		s.AppliedIndex++
		var buildErr error
		s.indexes, buildErr = s.buildIndexes()
		applied <- buildErr
	}()
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	next, err := queryScopedProgress(&s, topology.ProgressQuery{After: first.Next, Limit: 1000})
	if err != nil || next.AppliedIndex != first.AppliedIndex+1 || next.Groups[0].GroupID == "0" ||
		next.Groups[len(next.Groups)-1].GroupID != "zzz" {
		t.Fatal(next, err)
	}
	fresh, err := queryScopedProgress(&s, topology.ProgressQuery{Limit: 1})
	if err != nil || fresh.Groups[0].GroupID != "0" {
		t.Fatal(fresh, err)
	}
	empty, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: "absent"})
	if err != nil || progressCount(empty) != 0 || empty.Next != "" || empty.AppliedIndex != s.AppliedIndex {
		t.Fatal("empty page lost its coherent header", empty, err)
	}
}

func TestEmptyMachineProgressPreservesAppliedReadBoundary(t *testing.T) {
	t.Parallel()
	m := mustMachine(t, testRules(), nil)
	if page, err := m.Progress(topology.ProgressQuery{}); err != nil || page.ReadAt.IsZero() ||
		page.LatestVersion != 0 || len(page.Groups)+len(page.Nodes)+len(page.Operations)+len(page.Cleanup) != 0 {
		t.Fatal(page, err)
	}
	if _, err := m.Query(t.Context(), []byte(`{"kind":"progress","progress":{}}`)); err != nil {
		t.Fatal("empty progress query failed", err)
	}
}
