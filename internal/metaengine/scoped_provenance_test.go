//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedRebaseRejectsApprovedSourceAndInvalidAncestry(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"source skipped":    func(s *scopedState) { s.Latest.RebasedFrom = 2 },
		"repair skipped":    func(s *scopedState) { s.Latest.ParentVersion = 4 },
		"repair wrong base": func(s *scopedState) { s.Target.ParentVersion = 2 },
		"target":            func(s *scopedState) { s.Target = s.History[3] },
		"marker":            func(s *scopedState) { s.FullyConverged.Version = 4 },
		"group source":      func(s *scopedState) { s.Groups[0].SourceVersion = 4 },
		"node source":       func(s *scopedState) { s.Nodes[0].SourceVersion = 4 },
		"operation":         func(s *scopedState) { s.Operations[0].Status.Version, s.Operations[0].Status.ID = 4, "exec-v4-a1" },
		"block source":      func(s *scopedState) { s.Operations[0].Plan.Blocks[0].SourceVersion = 4 },
		"grant":             func(s *scopedState) { s.AuthorityRecords[0].Grant.Action.Version = 4 },
		"stop":              func(s *scopedState) { s.Cleanup[0].StopAction.Version = 4 },
		"retire":            func(s *scopedState) { s.Cleanup[0].RetireAction.Version = 4 },
		"remove":            func(s *scopedState) { s.Cleanup[0].Removed.Action.Version = 4 },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedCodecFixture()
			corrupt(&s)
			index, err := s.buildIndexes()
			if err != nil {
				t.Fatal(err)
			}
			if err := validateScopedRebase(&s, index, s.Latest); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid rebase provenance accepted", err)
			}
		})
	}
	s := scopedCodecFixture()
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("valid L/R/S provenance refused", err)
	}
}

func TestScopedRebasePreservesMultipleUnapprovedEdits(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	// v3 and v4 are both unapproved edits above Target v2 when repair v5 occurs.
	// L.Parent is v3; R.Parent must still be the prior approved v2.
	s.Operations = s.Operations[1:]
	s.Revocations = nil
	s.Target.ParentVersion = 2
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("multi-edit staged intent rejected", err)
	}
}

func TestScopedRebaseUsesApprovalOrderInsteadOfStagedLineage(t *testing.T) {
	t.Parallel()
	for _, parent := range []uint64{0, 1, 2} {
		s := scopedCodecFixture()
		// Accepted v4 has an old document lineage (raw Stage or RollbackTo).
		// Repair v5 still derives from the most recent approved version, v3.
		s.History[3].ParentVersion = parent
		if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
			t.Fatal("historical document lineage blocked an independent rebase", parent, err)
		}
		// Changing the repair base must not hide v3's retained approval, even
		// though that version no longer lies on v4's ParentVersion chain.
		s.Target.ParentVersion = 1
		if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal("document lineage bypassed an intermediate approval", parent, err)
		}
	}
}

func TestScopedCheckpointCompactionRequiresApprovedContext(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"group", "node"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s := scopedGraphFixture()
			s.Operations, s.History = nil, nil
			s.Groups[1].Owner, s.FullyConverged = "", topology.FullyConverged{}
			s.Latest = s.Target.Clone()
			s.Latest.Version, s.Latest.ParentVersion = 3, 2
			// Only staged Latest keeps the old checkpoint declaration.
			if kind == "group" {
				s.Target.Group("shard0").Labels = map[string]string{"approved": "change"}
			} else {
				s.Target.Node("n1").Labels = map[string]string{"approved": "change"}
			}
			if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("staged-only context validated compacted checkpoint", err)
			}
			// Retaining the proven source makes the same snapshot valid again.
			s.History = scopedGraphFixture().History
			if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
				t.Fatal("retained approved source was not usable", err)
			}
		})
	}
}

func TestScopedRebaseCannotSkipApprovedAncestorBehindStagedEdits(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	s.VoterSwitches = nil // This scenario uses v5 as staged intent, with no approved role effect.
	staged := s.History[3].Clone()
	staged.Version, staged.ParentVersion = 5, 4
	s.History = append(s.History, staged)
	s.Target.Version, s.Target.ParentVersion = 6, 3
	s.Latest.Version, s.Latest.ParentVersion, s.Latest.RebasedFrom = 7, 6, 5
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("valid approved ancestor refused", err)
	}
	s.Target.ParentVersion = 2
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("repair skipped approved ancestor behind staged edits", err)
	}
	s.History[3].ParentVersion = 4
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("nondecreasing staged ancestry accepted", err)
	}
}
