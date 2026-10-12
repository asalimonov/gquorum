package metaservice

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) SetGroupVoterPolicy(ctx context.Context, id string, p topology.VoterPolicy, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, "set group voter policy", false, func(doc *topology.Document, _ time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		if group.VoterPolicy == nil {
			// Legacy learners were intentional. Adoption must not silently make
			// them eligible or expand the existing host set.
			var hosts []string
			for i := range group.Replicas {
				r := &group.Replicas[i]
				r.PinnedLearner = r.Role == topology.ReplicaLearner
				hosts = append(hosts, r.NodeName)
			}
			if id != topology.MetaGroupID {
				slices.Sort(hosts)
				group.Hosting = &topology.HostingRule{Mode: topology.HostingExplicit, Nodes: hosts}
			}
		}
		policy := p
		group.VoterPolicy = &policy
		return nil
	})
}

func (g *Group) SetGroupHosting(ctx context.Context, id string, h topology.HostingRule, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, "set group hosting", true, func(doc *topology.Document, now time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		if group.VoterPolicy == nil {
			return fault.New(fault.Invalid, "adopt a voter policy before changing hosting", nil)
		}
		if err := h.Validate(); err != nil {
			return err
		}
		for _, name := range h.Nodes {
			if group.Hosting != nil && slices.Contains(group.Hosting.Nodes, name) {
				continue
			}
			if err := eligibleHost(*doc, id, name); err != nil {
				return err
			}
		}
		hosting := h
		hosting.Nodes = slices.Clone(h.Nodes)
		group.Hosting = &hosting
		return g.enforcePlacement(doc, now)
	})
}

func (g *Group) SetReplicaPinnedLearner(ctx context.Context, id string, key topology.ReplicaKey, pin bool, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, "set pinned learner", true, func(doc *topology.Document, now time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		if group.VoterPolicy == nil {
			return fault.New(fault.Invalid, "adopt a voter policy before changing learner eligibility", nil)
		}
		for i := range group.Replicas {
			r := &group.Replicas[i]
			if r.NodeName != key.NodeName || r.ReplicaID != key.ReplicaID {
				continue
			}
			if r.PinnedLearner != pin {
				r.PinnedLearner, r.UpdatedAt = pin, now
				if pin {
					r.Role = topology.ReplicaLearner
				}
			}
			return nil
		}
		return fault.New(fault.IdentityMismatch, "learner pin does not name the exact replica", nil)
	})
}

func eligibleHost(doc topology.Document, groupID, name string) error {
	n := doc.Node(name)
	if n == nil {
		return unknownNode(name)
	}
	if n.State != topology.NodeActive || !doc.GroupPlacement(groupID).Allows(n.Type) {
		return fault.New(fault.Invalid, "requested host is not eligible", nil)
	}
	return nil
}

// materializeHosting is called after validating capacity for all groups. New
// assignments in established groups are learners; only fresh group creation
// below uses founding voter selection.
func materializeHosting(doc *topology.Document, group *topology.Group, now time.Time) error {
	hosts, err := topology.RequiredLoadHosts(*doc, group.ID)
	if err != nil {
		return err
	}
	group.Replicas = slices.DeleteFunc(group.Replicas, func(r topology.Replica) bool { return !slices.Contains(hosts, r.NodeName) })
	for _, name := range hosts {
		if slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == name }) {
			continue
		}
		id, err := group.AllocateReplicaID()
		if err != nil {
			return err
		}
		group.Replicas = append(group.Replicas, topology.Replica{NodeName: name, ReplicaID: id, Role: topology.ReplicaLearner, CreatedAt: now, UpdatedAt: now})
	}
	return nil
}

func (g *Group) createLoadGroup(doc *topology.Document, spec GroupSpec, now time.Time) error {
	if doc.Group(spec.ID) != nil {
		return fault.New(fault.Invalid, "group already exists", nil)
	}
	if spec.Nodes != nil && spec.Hosting != nil {
		return fault.New(fault.Invalid, "specify Nodes or Hosting, not both", nil)
	}
	p := topology.DefaultVoterPolicy()
	if spec.VoterPolicy != nil {
		p = *spec.VoterPolicy
	}
	h := topology.HostingRule{Mode: topology.HostingAllEligible}
	if spec.Hosting != nil {
		h = *spec.Hosting
		h.Nodes = slices.Clone(h.Nodes)
	}
	if len(spec.Nodes) != 0 {
		h = topology.HostingRule{Mode: topology.HostingExplicit, Nodes: slices.Clone(spec.Nodes)}
		slices.Sort(h.Nodes)
	}
	if err := h.Validate(); err != nil {
		return err
	}
	group := topology.Group{ID: spec.ID, NextReplicaID: 1, VoterPolicy: &p, Hosting: &h, CreatedAt: now, UpdatedAt: now}
	if spec.Placement != nil {
		placement := *spec.Placement
		placement.AllowedNodeTypes = slices.Clone(placement.AllowedNodeTypes)
		group.Placement = &placement
	}
	doc.Groups = append(doc.Groups, group)
	for _, name := range h.Nodes {
		if err := eligibleHost(*doc, spec.ID, name); err != nil {
			return err
		}
	}
	lo, hi := doc.GroupBounds(spec.ID)
	if err := p.Validate(lo, hi); err != nil {
		return err
	}
	if err := topology.ValidateHostingCapacity(*doc, g.rules); err != nil {
		return err
	}
	groupPtr := doc.Group(spec.ID)
	if err := materializeHosting(doc, groupPtr, now); err != nil {
		return err
	}
	hosts, err := topology.RequiredLoadHosts(*doc, spec.ID)
	if err != nil {
		return err
	}
	roles := topology.FoundingRoles(hosts, nil, p.TargetVoters)
	for i := range groupPtr.Replicas {
		groupPtr.Replicas[i].Role = roles[groupPtr.Replicas[i].NodeName]
	}
	return nil
}
