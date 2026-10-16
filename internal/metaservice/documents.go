package metaservice

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Latest returns owned locally applied staged intent. WaitReady is the startup
// gate; reads do not promise a linearizable view of an unreachable leader.
func (g *Group) Latest(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Latest(), nil
}

// Target returns an owned locally applied approved topology document.
func (g *Group) Target(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Target(), nil
}

// Active returns an owned locally applied converged topology document.
func (g *Group) Active(ctx context.Context) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	return g.state().Active(), nil
}

// Get returns an owned retained topology version or a history-unavailable
// error.
func (g *Group) Get(ctx context.Context, version uint64) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	doc, ok := g.state().Get(version)
	if !ok {
		return topology.Document{}, fault.ErrHistoryUnavailable
	}
	return doc, nil
}

// History returns a bounded descending page and the oldest retained version.
func (g *Group) History(ctx context.Context, page topology.Page) (topology.HistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return topology.HistoryResult{}, err
	}
	if page.Offset < 0 || page.Limit < 0 {
		return topology.HistoryResult{}, fault.ErrInvalid
	}
	machine := g.state()
	all := machine.History(0, 0)
	result := topology.HistoryResult{}
	if len(all) > 0 {
		result.OldestVersion = all[len(all)-1].Version
	}
	if page.Offset < len(all) {
		end := len(all)
		if page.Limit > 0 && page.Limit < end-page.Offset {
			end = page.Offset + page.Limit
		}
		result.Documents = all[page.Offset:end]
	}
	return result, nil
}

// Stage proposes an owned document under replicated expected-version
// compare-and-swap.
func (g *Group) Stage(ctx context.Context, doc topology.Document, expected uint64) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	proposal := metaengine.ProposeDocument{Doc: doc.Clone(), ExpectedVersion: expected}
	if g.state().Latest().Version > expected {
		result, err := g.state().ReplayMutation(proposal)
		return result.Doc, err
	}
	result, err := g.propose(ctx, proposal)
	return result.Doc, err
}

// Authorize checks an exact step against locally applied approved execution
// state.
func (g *Group) Authorize(ctx context.Context, a topology.AuthorizedStep) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	return g.state().Authorize(ctx, a)
}

// Operator returns retained locally applied execution status for an exact
// operator ID.
func (g *Group) Operator(ctx context.Context, id topology.OperatorID) (topology.OperatorStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.OperatorStatus{}, err
	}
	status, ok := g.state().Operator(id)
	if !ok {
		return topology.OperatorStatus{}, fault.New(fault.HistoryUnavailable, "execution is not retained", nil)
	}
	return status, nil
}

// LocalStatus returns owned, locally observed meta Raft state. It does not
// probe
// peers or invoke caller load controllers. Found is false before engine startup
// and after shutdown; an observation is not a linearizable read or quorum
// proof.
func (g *Group) LocalStatus() topology.GroupStatus {
	if engine := g.engine.Load(); engine != nil {
		status := engine.Status()
		status.LeaderNodeName, _ = g.state().MemberOwner(status.LeaderID)
		return status
	}
	return topology.GroupStatus{GroupID: topology.MetaGroupID, HostNode: g.Identity().NodeName, ErrorCode: string(
		fault.Unavailable)}
}

// Status combines owned local document/runtime state with caller load-group
// status queries. The caller controller must honor ctx. Health is sampled by
// the observer; a mismatched approved version is reported as unsampled unknown
// with a zero observation timestamp until a current sample becomes available.
func (g *Group) Status(ctx context.Context) (topology.ClusterStatus, error) {
	return g.readClusterStatus(ctx, true)
}

// CachedStatus remains available after fencing without starting peer probes.
func (g *Group) CachedStatus(ctx context.Context) (topology.ClusterStatus, error) {
	return g.readClusterStatus(ctx, false)
}

func (g *Group) readClusterStatus(ctx context.Context, live bool) (topology.ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.ClusterStatus{}, err
	}
	machine := g.state()
	views := machine.Views()
	latest, target, active := views.Latest, views.Target, views.Active
	local := g.clusterStatusSnapshot(live)
	result := topology.ClusterStatus{
		Lifecycle:     local.lifecycle,
		ErrorCode:     g.terminalCode(),
		LatestVersion: latest.Version,
		TargetVersion: target.Version,
		ActiveVersion: active.Version,
		SelfRemoval:   local.removal,
	}
	placement, err := statusPlacement(active, target)
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	result.Placement = placement
	result.SessionLive = g.OwnSessionLive()
	if g.scheduler != nil {
		result.Scheduling = g.scheduler.Status()
	}
	result.Observation = g.HealthObservation()
	if result.Observation.Version != target.Version {
		result.Observation = topology.HealthObservation{Version: target.Version,
			Groups: topology.EvaluateAllGroupHealth(
				target, nil)}
	}
	result.Nodes = statusNodes(views, result.Observation, local.addresses)
	if engine := g.engine.Load(); engine != nil {
		status := engine.Status()
		status.LeaderNodeName, _ = machine.MemberOwner(status.LeaderID)
		g.cacheProgressObservation(status)
		result.MetaLeaderID = status.LeaderID
		result.MetaLeaderNodeName = status.LeaderNodeName
	}
	result.Groups, err = g.statusGroupProgress(ctx, views, local)
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	if operator, ok := machine.CurrentOperator(); ok {
		status := operator.Status
		result.Operator = &status
	}
	return result, nil
}

type localClusterStatus struct {
	lifecycle   string
	addresses   map[string]string
	includeLive bool
	removal     *topology.SelfRemovalStatus
}

func (g *Group) clusterStatusSnapshot(live bool) localClusterStatus {
	g.mu.RLock()
	lifecycle := g.lifecycle
	addresses := maps.Clone(g.addresses)
	includeLive := live && !g.quiescing && g.selfRemoval == nil
	var removal *topology.SelfRemovalStatus
	if g.selfRemoval != nil {
		owned := *g.selfRemoval
		removal = &owned
	}
	g.mu.RUnlock()
	return localClusterStatus{lifecycle: lifecycle, addresses: addresses,
		includeLive: includeLive, removal: removal}
}

func statusPlacement(active, target topology.Document) ([]topology.PlacementReport, error) {
	var reports []topology.PlacementReport
	for _, doc := range []topology.Document{active, target} {
		if doc.Version == 0 || len(reports) > 0 &&
			reports[len(reports)-1].Version == doc.Version {
			continue
		}
		ids := make([]string, 0, len(doc.Groups))
		for _, group := range doc.Groups {
			ids = append(ids, group.ID)
		}
		slices.Sort(ids)
		report, err := placementReportFor(doc, doc, ids)
		if err != nil {
			return nil, err
		}
		if report != nil {
			report.Source = "converged"
			if doc.Version == target.Version {
				report.Source = "approved"
			} else {
				report.ApprovedVersion = target.Version
			}
			report.AffectedGroups = nil // status is a snapshot, not an edit
			reports = append(reports, *report)
		}
	}
	return reports, nil
}

// statusNodes retains the schema-3 whole-Active effective-state projection
// until Task78 connects durable per-node progress to the public status.
func statusNodes(views topology.ReadView, observation topology.HealthObservation,
	addresses map[string]string) []topology.NodeStatus {
	names := statusNodeNames(views)
	out := collection.Slice[topology.NodeStatus](len(names))
	for _, name := range slices.Sorted(maps.Keys(names)) {
		if views.Target.Node(name) == nil && views.Active.Node(name) == nil {
			continue // Staged-only identity is intent, not effective cluster state.
		}
		out = append(out, statusNode(views, observation, addresses[name], name))
	}
	return out
}

func statusNodeNames(views topology.ReadView) map[string]bool {
	names := make(map[string]bool, len(views.Latest.Nodes)+len(views.Target.Nodes)+len(views.Active.Nodes))
	for _, doc := range []topology.Document{views.Latest, views.Target, views.Active} {
		for _, node := range doc.Nodes {
			names[node.Name] = true
		}
	}
	return names
}

func statusNode(views topology.ReadView, observation topology.HealthObservation,
	address, name string,
) topology.NodeStatus {
	node := views.Latest.Node(name)
	if node == nil {
		node = views.Target.Node(name)
	}
	if node == nil {
		node = views.Active.Node(name)
	}
	effective := topology.EffectiveNodeState(views.Active, name)
	if effective == "" {
		if approved := views.Target.Node(name); approved != nil {
			effective = approved.State
		} else {
			effective = topology.NodeExcluded
		}
	}
	health := topology.HealthUnknown
	if observation.Version == views.Target.Version {
		if observed, exists := observation.Nodes[name]; exists {
			health = observed
		}
	}
	return topology.NodeStatus{Node: *node, EffectiveState: effective, Health: health, Address: address}
}

func (g *Group) statusGroupProgress(ctx context.Context, views topology.ReadView,
	local localClusterStatus) ([]topology.GroupProgress, error) {
	if local.includeLive {
		if err := g.refreshLoadStatus(ctx, views.Active); err != nil {
			return nil, err
		}
	}
	rows := metaengine.LegacyGroupProgress(views)
	g.decorateProgress(rows)
	return rows, nil
}

func (g *Group) refreshLoadStatus(ctx context.Context, active topology.Document) error {
	for _, group := range active.LoadGroups() {
		observed := g.liveLoadStatus(ctx, group.ID)
		if err := ctx.Err(); err != nil {
			return err
		}
		g.cacheProgressObservation(observed)
	}
	return nil
}

func (g *Group) liveLoadStatus(ctx context.Context, id string) topology.GroupStatus {
	observed := topology.GroupStatus{GroupID: id}
	var err error
	if g.opts.LoadGroups == nil {
		err = fault.ErrNoController
	} else {
		observed, err = g.opts.LoadGroups.Status(ctx, id)
	}
	if err == nil && observed.GroupID != id {
		err = fault.ErrIdentityMismatch
	}
	if err != nil {
		code := fault.Unavailable
		var typed *fault.Error
		if errors.As(err, &typed) {
			code = typed.Code
		}
		observed = topology.GroupStatus{GroupID: id, ErrorCode: string(code)}
	}
	return observed
}

// Legacy scheduler adapter until Task78. Its caller still supplies real schema3
// Active; status/progress readers use exact approved/proven identities instead.
func legacyLeaderNodeName(status topology.GroupStatus, active topology.Document) topology.GroupStatus {
	status.LeaderNodeName = ""
	if status.LeaderID != 0 {
		if group := active.Group(status.GroupID); group != nil {
			for _, replica := range group.Replicas {
				if replica.ReplicaID == status.LeaderID {
					status.LeaderNodeName = replica.NodeName
					break
				}
			}
		}
	}
	return status
}
