package metaservice

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// SetGroupVoterConfig stages guarded adoption or modification of replicated
// voter configuration.
func (g *Group) SetGroupVoterConfig(ctx context.Context, id string, p topology.VoterConfig,
	o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "set_group_voter_config",
		Input:   mutationInput{GroupID: id, Value: p},
		Comment: "set group voter configuration"},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			group := doc.Group(id)
			if group == nil {
				return unknownGroup(id)
			}
			if group.VoterConfig == nil {
				// Legacy learners were intentional. Adoption must not silently
				// make
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
			voterConfig := p
			group.VoterConfig = &voterConfig
			return g.preparePlacementEdit(before, doc, now)
		},
	)
}

// SetGroupHosting stages validated explicit or all-eligible load hosting
// intent.
func (g *Group) SetGroupHosting(ctx context.Context, id string, h topology.HostingRule,
	o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "set_group_hosting",
		Input:   mutationInput{GroupID: id, Value: h},
		Comment: "set group hosting", CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			group := doc.Group(id)
			if group == nil {
				return unknownGroup(id)
			}
			if group.VoterConfig == nil {
				return fault.New(fault.Invalid, "adopt a voter configuration before changing hosting", nil)
			}
			hosting := h
			hosting.Nodes = slices.Sorted(slices.Values(h.Nodes))
			if err := hosting.Validate(); err != nil {
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
			group.Hosting = &hosting
			return g.preparePlacementEdit(before, doc, now)
		})
}

// SetReplicaPinnedLearner stages the promotion restriction for one exact group
// replica.
func (g *Group) SetReplicaPinnedLearner(ctx context.Context, id string, setting topology.LearnerPin,
	o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "set_replica_pinned_learner",
		Input:   mutationInput{GroupID: id, Value: setting},
		Comment: "set pinned learner", CheckHealth: true},
		func(doc *topology.Document, now time.Time) error { return setLearnerPin(doc, id, setting, now) })
}

func setLearnerPin(doc *topology.Document, id string, setting topology.LearnerPin, now time.Time) error {
	key, pin := setting.Replica, setting.Pinned
	group := doc.Group(id)
	if group == nil {
		return unknownGroup(id)
	}
	if group.VoterConfig == nil {
		return fault.New(fault.Invalid, "adopt a voter configuration before changing learner eligibility", nil)
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

func createLoadGroup(doc *topology.Document, spec GroupSpec, now time.Time) error {
	if doc.Group(spec.ID) != nil {
		return fault.New(fault.Invalid, "group already exists", nil)
	}
	if spec.Nodes != nil && spec.Hosting != nil {
		return fault.New(fault.Invalid, "specify Nodes or Hosting, not both", nil)
	}
	if err := topology.ValidateStorageClass(spec.Storage); err != nil {
		return err
	}
	p := topology.DefaultVoterConfig()
	if spec.VoterConfig != nil {
		p = *spec.VoterConfig
	}
	h := topology.HostingRule{Mode: topology.HostingAllEligible}
	if spec.Hosting != nil {
		h = *spec.Hosting
		h.Nodes = slices.Sorted(slices.Values(h.Nodes))
	}
	if len(spec.Nodes) != 0 {
		h = topology.HostingRule{Mode: topology.HostingExplicit, Nodes: slices.Clone(spec.Nodes)}
		slices.Sort(h.Nodes)
	}
	if err := h.Validate(); err != nil {
		return err
	}
	group := topology.Group{
		ID:            spec.ID,
		NextReplicaID: 1,
		VoterConfig:   &p,
		Hosting:       &h,
		Labels:        maps.Clone(spec.Labels),
		Storage:       spec.Storage,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if spec.Placement != nil {
		placement := *spec.Placement
		placement.AllowedNodeTypes = slices.Sorted(slices.Values(placement.AllowedNodeTypes))
		placement.NodeSelector = maps.Clone(placement.NodeSelector)
		group.Placement = &placement
	}
	doc.Groups = append(doc.Groups, group)
	for _, name := range h.Nodes {
		if err := eligibleHost(*doc, spec.ID, name); err != nil {
			return err
		}
	}
	lo, hi := doc.GroupBounds(spec.ID)
	return p.Validate(lo, hi)
}
