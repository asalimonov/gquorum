//go:build functional

package gquorum

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type ensureMutationProbe struct {
	expected   []uint64
	approved   []uint64
	result     MutationResult
	err        error
	approveErr error
}

func (p *ensureMutationProbe) CreateGroup(_ context.Context, _ GroupSpec, opts MutationOptions) (MutationResult, error) {
	p.expected = append(p.expected, opts.ExpectedVersion)
	if opts.Approve || opts.Preview {
		panic("creation implicitly approves or previews")
	}
	return p.result, p.err
}
func (p *ensureMutationProbe) Approve(_ context.Context, version uint64) (topology.OperatorID, error) {
	p.approved = append(p.approved, version)
	return topology.MakeOperatorID(version, 1), p.approveErr
}
func (*ensureMutationProbe) Get(context.Context, uint64) (topology.Document, error) {
	panic("unexpected ancestry walk")
}

func TestEnsureCreationPinsCASAndApprovesOnlyReplayedRebasedHead(t *testing.T) {
	t.Parallel()
	spec, group := GroupSpec{ID: "work"}, topology.Group{ID: "work"}
	request := ensureRequest{Spec: spec, Options: EnsureOptions{Actor: "app", Comment: "create"}}
	base := topology.Document{Version: 2}
	view := topology.ReadView{Latest: base, Target: base, Active: base}
	probe := &ensureMutationProbe{err: context.DeadlineExceeded}
	if _, err := request.step(t.Context(), probe, view); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	original := base.Clone()
	original.Version, original.ParentVersion, original.ChangedBy, original.Comment = 3, 2, "app", "create"
	original.Groups = []topology.Group{group}
	// An independent repair changes the parent while preserving this exact intent.
	repaired := base.Clone()
	repaired.Version = 4
	repaired.Nodes = []topology.Node{{Name: "repair"}}
	head := repaired.Clone()
	head.Version, head.ParentVersion, head.RebasedFrom, head.ChangedBy, head.Comment = 5, 4, 3, "app", "create"
	head.Groups = []topology.Group{group}
	view.Latest, view.Target = head, repaired
	probe.result, probe.err = MutationResult{Document: original, RebasedTo: 5}, nil
	if _, err := request.step(t.Context(), probe, view); err != nil {
		t.Fatal(err)
	}
	if len(probe.expected) != 2 || probe.expected[0] != 2 || probe.expected[1] != 2 || len(probe.approved) != 0 {
		t.Fatal("changed original CAS or implicitly approved", probe)
	}
	if _, err := request.step(t.Context(), probe, view); err != nil {
		t.Fatal(err)
	}
	if len(probe.approved) != 1 || probe.approved[0] != 5 {
		t.Fatal("approved original or unrelated document", probe.approved)
	}
}

func TestEnsureCreationRefusesForeignHeadAndDoesNotRebaseCAS(t *testing.T) {
	t.Parallel()
	spec, group := GroupSpec{ID: "work"}, topology.Group{ID: "work"}
	opts := EnsureOptions{Actor: "app", Comment: "create"}
	original := topology.Document{Version: 3, ParentVersion: 2, ChangedBy: "app", Comment: "create", Groups: []topology.Group{group}}
	foreign := original.Clone()
	foreign.Version, foreign.ChangedBy = 4, "other"
	view := topology.ReadView{Latest: foreign, Target: topology.Document{Version: 2}}
	request := ensureRequest{Spec: spec, Options: opts, ExpectedVersion: 2, HeadVersion: 3}
	probe := &ensureMutationProbe{result: MutationResult{Document: original}}
	if _, err := request.step(t.Context(), probe, view); !errors.Is(err, fault.ErrConflict) {
		t.Fatal(err)
	}
	if len(probe.approved) != 0 || len(probe.expected) != 1 || probe.expected[0] != 2 {
		t.Fatal(probe)
	}
	// A restart can recognize its own exact head, but not another audit intent.
	request = ensureRequest{Spec: spec, Options: opts}
	view.Latest = original
	view.Latest.Comment = "another reason"
	if _, err := request.step(t.Context(), probe, view); !errors.Is(err, fault.ErrConflict) {
		t.Fatal(err)
	}
	if len(probe.approved) != 0 {
		t.Fatal(probe.approved)
	}
}

func TestEnsureApprovalFollowsOnlyFSMNamedRebase(t *testing.T) {
	t.Parallel()
	spec, group := GroupSpec{ID: "work"}, topology.Group{ID: "work"}
	base := topology.Document{Version: 2}
	head := topology.Document{Version: 3, ParentVersion: 2, ChangedBy: "app", Comment: "create", Groups: []topology.Group{group}}
	request := ensureRequest{Spec: spec, Options: EnsureOptions{Actor: "app", Comment: "create"}, ExpectedVersion: 2, HeadVersion: 3}
	probe := &ensureMutationProbe{approveErr: &fault.Error{Code: fault.VersionConflict, ExpectedVersion: 3, CurrentVersion: 5, RebasedTo: 5}}
	if _, err := request.step(t.Context(), probe, topology.ReadView{Latest: head, Target: base}); err != nil {
		t.Fatal(err)
	}
	if request.ExpectedVersion != 2 || request.HeadVersion != 5 {
		t.Fatal(request)
	}
	base.Version, head.Version, head.ParentVersion, head.RebasedFrom = 4, 5, 4, 3
	probe.approveErr = nil
	if _, err := request.step(t.Context(), probe, topology.ReadView{Latest: head, Target: base}); err != nil {
		t.Fatal(err)
	}
	if len(probe.approved) != 2 || probe.approved[0] != 3 || probe.approved[1] != 5 || len(probe.expected) != 0 {
		t.Fatal(probe)
	}
	probe.approveErr = fault.ErrVersionConflict
	if _, err := request.step(t.Context(), probe, topology.ReadView{Latest: head, Target: base}); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("guessed successor after unqualified conflict", err)
	}
}
