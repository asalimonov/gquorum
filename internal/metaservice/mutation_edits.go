package metaservice

import (
	"context"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func stampMutationGroups(base topology.Document, next *topology.Document, now time.Time) {
	for i := range next.Groups {
		group := &next.Groups[i]
		if old := base.Group(group.ID); old != nil &&
			(old.NextReplicaID != group.NextReplicaID || !reflect.DeepEqual(old.Replicas, group.Replicas) ||
				!reflect.DeepEqual(old.Placement, group.Placement) ||
				!reflect.DeepEqual(old.VoterConfig, group.VoterConfig) ||
				!reflect.DeepEqual(old.Hosting, group.Hosting)) {
			group.UpdatedAt = now
		}
	}
}

func setMetaLearner(doc *topology.Document, name string) {
	meta := doc.MetaGroup()
	for i := range meta.Replicas {
		if meta.Replicas[i].NodeName == name {
			meta.Replicas[i].Role = topology.ReplicaLearner
			break
		}
	}
}

func (g *Group) prepareNormalRetirement(doc *topology.Document, node *topology.Node, name string) error {
	page, err := g.state().Progress(topology.ProgressQuery{NodeName: name})
	if err != nil {
		return err
	}
	var old *topology.Node
	for _, row := range page.Nodes {
		if row.NodeName == name {
			old = row.Converged
			break
		}
	}
	if old != nil && old.State != topology.NodeDraining && old.State != topology.NodeDrained {
		return fault.New(fault.Invalid, "node must have a converged drain before retirement", nil)
	}
	if old == nil {
		target := g.state().Target()
		if admitted := target.Node(name); admitted != nil && admitted.State.Places() {
			return fault.New(fault.Invalid, "approved admission must converge before normal retirement", nil)
		}
		removeNodeReplicas(doc, name, loadReplicaRemoval)
	} else if node.State != topology.NodeDraining && node.State != topology.NodeDrained {
		return fault.ErrInvalid
	}
	return nil
}

func hasNodeLoadReplica(doc topology.Document, name string) bool {
	for _, group := range doc.LoadGroups() {
		for _, replica := range group.Replicas {
			if replica.NodeName == name {
				return true
			}
		}
	}
	return false
}

func (g *Group) prepareRollback(doc *topology.Document, old topology.Document, version uint64, now time.Time) error {
	if doc.Version == version {
		return fault.New(fault.Invalid, "version is already current", nil)
	}
	before := doc.Clone()
	floors := g.state().AllocationFloors()
	for i := range old.Groups {
		old.Groups[i].NextReplicaID = max(old.Groups[i].NextReplicaID, floors[old.Groups[i].ID])
	}
	for i := range old.Groups {
		if group := doc.Group(old.Groups[i].ID); group != nil {
			old.Groups[i].NextReplicaID = max(old.Groups[i].NextReplicaID, group.NextReplicaID)
		}
	}
	*doc = old.Clone()
	canPrepare := canPrepareRollback(before, *doc)
	if !canPrepare {
		return nil // complete-document validation handles deletion/retirement
	}
	doc.Version = before.Version
	return g.preparePlacementEdit(before, doc, now)
}

// canPrepareRollback excludes refounding and exact-identity changes from placement preparation.
func canPrepareRollback(before, next topology.Document) bool {
	if len(next.Groups) != len(before.Groups) {
		return false
	}
	for _, group := range next.Groups {
		current := before.Group(group.ID)
		if current == nil || group.NextReplicaID != current.NextReplicaID ||
			!reflect.DeepEqual(group.Replicas, current.Replicas) {
			return false
		}
	}
	return true
}

func (g *Group) reseatDocument(doc *topology.Document, views topology.ReadView,
	options metaengine.ReplicaRepairOptions) error {
	id, name := options.GroupID, options.NodeName
	group := doc.Group(id)
	if group == nil {
		return unknownGroup(id)
	}
	for _, r := range group.Replicas {
		if r.NodeName != name {
			continue
		}
		if r.ReplicaID != options.ReplicaID {
			return fault.New(fault.VersionConflict, "reseat predecessor no longer matches", nil)
		}
		request := options
		if request.Actor == "" {
			request.Actor = g.currentIdentity().NodeName
		}
		repair, err := metaengine.ReplicaRepairDocument(views.Target, views.Latest, request)
		if err != nil {
			return err
		}
		*doc = repair.Doc
		return nil
	}
	return unknownNode(name)
}

func (g *Group) leaderTransferTarget(ctx context.Context, doc topology.Document, id, name string) (uint64, error) {
	group := doc.Group(id)
	if group == nil {
		return 0, unknownGroup(id)
	}
	node := doc.Node(name)
	if node == nil {
		return 0, unknownNode(name)
	}
	if node.State != topology.NodeActive {
		return 0, fault.New(fault.Invalid, "leadership target must be active", nil)
	}
	var member uint64
	for _, r := range group.Replicas {
		if r.NodeName == name && r.Role == topology.ReplicaVoter {
			member = r.ReplicaID
		}
	}
	if member == 0 {
		return 0, fault.New(fault.Invalid, "leadership target must be a declared voter", nil)
	}
	if g.opts.Health != nil {
		health, err := g.opts.Health.Snapshot(ctx)
		if err != nil {
			return 0, err
		}
		if health != nil && health[name] != topology.HealthAlive {
			return 0, fault.New(fault.UnsafeMembership, "leadership target is not alive", nil)
		}
	}
	return member, nil
}
