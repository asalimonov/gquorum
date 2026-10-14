package gquorum

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// EnsureOptions identifies the caller of EnsureGroup. Actor is required: it is
// recorded as the document's ChangedBy and is how a retry recognizes its own
// staged-but-unapproved creation.
type EnsureOptions struct {
	Actor   string `json:"actor"`
	Comment string `json:"comment,omitempty"`
}

const ensurePoll = 200 * time.Millisecond

// EnsureGroup makes spec present in Active topology. It is idempotent across
// retries, restarts and concurrent callers using the same spec and actor. It
// approves only its own staged creation and never overwrites a different
// group: those cases return fault.ErrConflict. The returned group is an
// observation of Active, not a linearizable barrier.
func (c *clusterView) EnsureGroup(ctx context.Context, spec GroupSpec, opts EnsureOptions) (topology.Group, error) {
	if err := topology.ValidateLoadID(spec.ID); err != nil {
		return topology.Group{}, err
	}
	if strings.TrimSpace(opts.Actor) == "" {
		return topology.Group{}, fault.New(fault.Invalid, "EnsureGroup requires an actor", nil)
	}
	if opts.Comment == "" {
		opts.Comment = fmt.Sprintf("ensure group %q", spec.ID)
	}
	retry := fault.RetryOptions{InitialBackoff: ensurePoll, MaxBackoff: time.Second}
	for {
		view, err := fault.Retry(ctx, retry, c.ReadTopology)
		if err != nil {
			return topology.Group{}, err
		}
		step, err := c.ensureStep(ctx, view, spec, opts)
		if step.Done || (err != nil && !fault.Retryable(err) && !errors.Is(err, fault.ErrVersionConflict)) {
			return step.Group, err
		}
		if err := sleepContext(ctx, ensurePoll); err != nil {
			return topology.Group{}, err
		}
	}
}

type ensureResult struct {
	Group topology.Group
	Done  bool
}

// ensureStep performs at most one mutation for the observed view. Done reports
// a final result; a nil error with Done=false means re-read and continue.
func (c *clusterView) ensureStep(ctx context.Context, view topology.ReadView, spec GroupSpec,
	opts EnsureOptions) (ensureResult, error) {
	if g := view.Active.Group(spec.ID); g != nil {
		if !groupMatches(spec, *g) {
			return ensureResult{Done: true}, fault.New(fault.Conflict, fmt.Sprintf(
				"group %q exists with a different specification", spec.ID), nil)
		}
		return ensureResult{Group: cloneGroup(*g), Done: true}, nil
	}
	if g := view.Target.Group(spec.ID); g != nil {
		if !groupMatches(spec, *g) {
			return ensureResult{Done: true}, fault.New(fault.Conflict, fmt.Sprintf(
				"approved group %q has a different specification", spec.ID), nil)
		}
		return ensureResult{}, nil // approved and converging
	}
	if view.Latest.Version != view.Target.Version {
		if !ownCreation(view.Latest, view.Target, spec, opts.Actor) {
			return ensureResult{Done: true}, fault.New(fault.Conflict,
				fmt.Sprintf("unapproved staged topology at version %d", view.Latest.Version), nil)
		}
		_, err := c.Approve(ctx, view.Latest.Version)
		return ensureResult{}, err
	}
	_, err := c.CreateGroup(ctx, spec, MutationOptions{
		ExpectedVersion: view.Latest.Version,
		Actor:           opts.Actor,
		Comment:         opts.Comment,
		Approve:         true,
	})
	return ensureResult{}, err
}

// groupMatches reports whether every field set in spec equals the group's
// declaration. Unset fields are unconstrained.
func groupMatches(spec GroupSpec, g topology.Group) bool {
	if spec.ID != g.ID || spec.Storage != g.Storage {
		return false
	}
	if spec.Labels != nil && !maps.Equal(spec.Labels, g.Labels) {
		return false
	}
	if spec.Placement != nil && (g.Placement == nil || !sameJSON(*spec.Placement, *g.Placement)) {
		return false
	}
	if spec.VoterConfig != nil && (g.VoterConfig == nil || !sameJSON(*spec.VoterConfig, *g.VoterConfig)) {
		return false
	}
	if spec.Hosting != nil && (g.Hosting == nil || !sameJSON(*spec.Hosting, *g.Hosting)) {
		return false
	}
	if len(spec.Nodes) != 0 {
		if !matchesExplicitHosts(spec.Nodes, g.Hosting) {
			return false
		}
	}
	return true
}

// ownCreation reports whether latest is exactly the actor's unapproved creation
// of spec on top of target, with no other topology change.
func ownCreation(latest, target topology.Document, spec GroupSpec, actor string) bool {
	if latest.ParentVersion != target.Version || latest.ChangedBy != actor || target.Group(spec.ID) != nil {
		return false
	}
	created := latest.Group(spec.ID)
	if created == nil || len(latest.Groups) != len(target.Groups)+1 || !groupMatches(spec, *created) {
		return false
	}
	if !sameJSON(latest.Nodes, target.Nodes) || !sameJSON(latest.Placement, target.Placement) {
		return false
	}
	for _, group := range target.Groups {
		other := latest.Group(group.ID)
		if other == nil || !sameJSON(*other, group) {
			return false
		}
	}
	return true
}

// sameJSON compares values through their persisted encoding, so documents that
// have round-tripped through storage compare equal to freshly built values.
func sameJSON(a, b any) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(x, y)
}

func cloneGroup(g topology.Group) topology.Group {
	doc := topology.Document{Groups: []topology.Group{g}}
	return doc.Clone().Groups[0]
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func matchesExplicitHosts(nodes []string, hosting *topology.HostingRule) bool {
	owned := slices.Clone(nodes)
	slices.Sort(owned)
	want := topology.HostingRule{Mode: topology.HostingExplicit, Nodes: owned}
	if hosting == nil || !sameJSON(want, *hosting) {
		return false
	}
	return true
}
