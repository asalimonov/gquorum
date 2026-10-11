//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

func TestDocumentStatusPreviewGuardsAndHistoryThreeNodes(t *testing.T) { documentAPIContracts(t, 3) }
func TestDocumentStatusPreviewGuardsAndHistoryFiveNodes(t *testing.T)  { documentAPIContracts(t, 5) }
func TestDocumentStatusPreviewGuardsAndHistorySevenNodes(t *testing.T) { documentAPIContracts(t, 7) }

func assertAPIViews(t *testing.T, ctx context.Context, c *Cluster, doc topology.Document) {
	t.Helper()
	for _, node := range c.Nodes() {
		for _, view := range []string{"latest", "target", "active"} {
			observed, err := c.Document(ctx, node.Spec.Name, view)
			if err != nil || !reflect.DeepEqual(observed, doc) {
				t.Fatal("operation changed an owned document view", node.Spec.Name, view, observed, err)
			}
		}
		native, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil || len(native.Status.ConfState.Voters) != len(c.Spec.Nodes) || len(native.Status.ConfState.Learners) != 0 {
			t.Fatal("operation changed native meta membership", native, err)
		}
	}
}

func globalPlacementBody(rule topology.PlacementRule, options meta.MutationOptions) any {
	return map[string]any{"placement": rule, "mutation": options}
}

func documentAPIContracts(t *testing.T, size int) {
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	base := created.Document
	if err := c.Put(ctx, "node2", "work", "retained", "retained", "value"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if !status.SessionLive || status.MetaLeader == 0 || status.LatestVersion != base.Version || status.TargetVersion != base.Version || status.ActiveVersion != base.Version || len(status.Nodes) != size || len(status.Groups) != 2 {
				return false, nil
			}
			for _, n := range status.Nodes {
				if n.Node.State != topology.NodeActive || n.EffectiveState != topology.NodeActive || n.Health != topology.HealthAlive {
					return false, nil
				}
			}
			for _, g := range status.Groups {
				expected := 3
				if g.GroupID == topology.MetaGroupID {
					expected = size
				}
				declared := base.Group(g.GroupID)
				if declared == nil || !g.Found || len(g.ConfState.Voters) != expected || g.LeaderID == 0 || !slices.ContainsFunc(declared.Replicas, func(r topology.Replica) bool {
					return r.ReplicaID == g.LeaderID && (g.GroupID == topology.MetaGroupID || r.NodeName == g.HostNode)
				}) {
					return false, nil
				}
				if topology.QuorumOf(len(g.ConfState.Voters)) != expected/2+1 {
					return true, fmt.Errorf("wrong quorum math")
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal("status did not expose declared topology and both observed leaders", node.Spec.Name, err)
		}
	}
	assertAPIViews(t, ctx, c, base)
	if groups := base.LoadGroups(); len(groups) != 1 || groups[0].ID != "work" {
		t.Fatal("meta appeared in load document helpers", groups)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/groups", map[string]any{"group": meta.GroupSpec{ID: topology.MetaGroupID}, "mutation": meta.MutationOptions{Actor: "cluster-test"}}, base.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("meta accepted as a load group", err)
	}
	// Adapter codes are caller-defined; stable SDK fault identities and exact
	// CAS versions survive the adapter. Missing/stale requests commit nothing.
	response, err = c.Request(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(base.Placement, meta.MutationOptions{}), 0)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("missing If-Match accepted", err)
	}
	response, err = c.Request(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(base.Placement, meta.MutationOptions{}), base.Version-1)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("stale If-Match accepted", err)
	}
	var conflict struct {
		Code     string `json:"code"`
		Expected uint64 `json:"expected_version"`
		Current  uint64 `json:"current_version"`
	}
	if err := response.Decode(&conflict); err != nil || conflict.Code != string(fault.VersionConflict) || conflict.Expected != base.Version-1 || conflict.Current != base.Version {
		t.Fatal("CAS refusal lost exact versions", conflict, err)
	}
	assertAPIViews(t, ctx, c, base)
	receipts := map[string]Controls{}
	for _, node := range c.Nodes() {
		observed, err := c.Controls(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		receipts[node.Spec.Name] = observed
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes/node3/drain", meta.MutationOptions{Preview: true, Actor: "preview-test"}, base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var preview meta.MutationResult
	if err := response.Decode(&preview); err != nil || preview.ExecutionID != "" || preview.Document.Version != base.Version+1 || preview.Document.Node("node3").State != topology.NodeDraining {
		t.Fatal("preview staged or approved", preview, err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/topology/preview/plan", preview.Document, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil || !slices.ContainsFunc(plan.Steps, func(s topology.Step) bool {
		return s.GroupID == "work" && s.Kind == topology.StepRemoveReplica && s.NodeName == "node3" && s.ReplicaID == replicaID(base, "work", "node3")
	}) {
		t.Fatal("preview lacks the exact intended removal", plan, err)
	}
	foreign := preview.Document.Clone()
	foreign.ClusterID = "foreign-cluster"
	response, err = c.Request(ctx, "node2", "POST", "/api/topology/preview/plan", foreign, 0)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal("preview accepted foreign cluster", err)
	}
	assertAPIViews(t, ctx, c, base)
	for _, node := range c.Nodes() {
		observed, err := c.Controls(ctx, node.Spec.Name)
		if err != nil || !reflect.DeepEqual(observed, receipts[node.Spec.Name]) {
			t.Fatal("preview invoked an effect", node.Spec.Name, observed, err)
		}
	}
	if err := exactOwnedValues(ctx, c, base, "work", map[string]string{"retained": "value"}); err != nil {
		t.Fatal(err)
	}
	// Two approved documents preserve separate content in newest-first history.
	firstRule := base.Placement
	firstRule.MinReplicas = 2
	first, err := c.Mutate(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(firstRule, meta.MutationOptions{Apply: true, Actor: "history-first"}))
	if err != nil {
		t.Fatal(err)
	}
	secondRule := firstRule
	secondRule.MinReplicas = 1
	second, err := c.Mutate(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(secondRule, meta.MutationOptions{Apply: true, Actor: "history-second"}))
	if err != nil || second.Document.Version != first.Document.Version+1 {
		t.Fatal(second, err)
	}
	page := func(offset, limit int) meta.HistoryResult {
		t.Helper()
		response, err := c.Request(ctx, "node2", "GET", fmt.Sprintf("/api/topology/history?offset=%d&limit=%d", offset, limit), nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var result meta.HistoryResult
		if err := response.Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	all := page(0, 500)
	if len(all.Documents) < 4 || all.OldestVersion != 1 {
		t.Fatal("retained history is incomplete", all)
	}
	for i := 1; i < len(all.Documents); i++ {
		if all.Documents[i-1].Version <= all.Documents[i].Version {
			t.Fatal("history is not newest first", all)
		}
	}
	for _, p := range []struct{ offset, limit int }{{0, 1}, {1, 1}, {0, 2}} {
		got := page(p.offset, p.limit)
		if got.OldestVersion != all.OldestVersion || !reflect.DeepEqual(got.Documents, all.Documents[p.offset:p.offset+p.limit]) {
			t.Fatal("page differs from retained history", got, all)
		}
	}
	beyond := page(len(all.Documents)+10, 5)
	if len(beyond.Documents) != 0 || beyond.OldestVersion != all.OldestVersion {
		t.Fatal("empty page lost retention horizon", beyond)
	}
	for _, query := range []string{"?limit=lots", "?offset=-1", "?limit=0"} {
		response, err := c.Request(ctx, "node2", "GET", "/api/topology/history"+query, nil, 0)
		if err := requireStatus(response, err, 400); err != nil {
			t.Fatal(err)
		}
	}
	response, err = c.Request(ctx, "node2", "GET", fmt.Sprintf("/api/topology/%d", first.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var historical topology.Document
	if err := response.Decode(&historical); err != nil || !reflect.DeepEqual(historical, first.Document) || historical.Placement.MinReplicas != 2 {
		t.Fatal("exact history changed content", historical, err)
	}
	response, err = c.Request(ctx, "node2", "GET", fmt.Sprintf("/api/topology/%d", second.Document.Version+10000), nil, 0)
	if err := requireStatus(response, err, 404); err != nil {
		t.Fatal("uncommitted history was fabricated", err)
	}
	assertAPIViews(t, ctx, c, second.Document)
}

func TestNodeLifecyclePlacementAndLeaderTransferThreeNodes(t *testing.T) {
	lifecyclePlacementContracts(t, 3)
}
func TestNodeLifecyclePlacementAndLeaderTransferFiveNodes(t *testing.T) {
	lifecyclePlacementContracts(t, 5)
}
func TestNodeLifecyclePlacementAndLeaderTransferSevenNodes(t *testing.T) {
	lifecyclePlacementContracts(t, 7)
}

func assertActualAPIMembership(t *testing.T, ctx context.Context, c *Cluster, doc topology.Document, loadVoters int) {
	t.Helper()
	want := make([]uint64, 0, loadVoters)
	for _, replica := range doc.VotersOf("work") {
		want = append(want, replica.ReplicaID)
	}
	slices.Sort(want)
	if len(want) != loadVoters {
		t.Fatal("wrong declared voter count", want, loadVoters)
	}
	for _, node := range c.Nodes() {
		native, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil || len(native.Status.ConfState.Voters) != len(c.Spec.Nodes) || len(native.Status.ConfState.Learners) != 0 || native.Identity.MemberID != doc.MemberID(node.Spec.Name) {
			t.Fatal("lifecycle changed meta membership", native, err)
		}
		if err := Poll(ctx, func() (bool, error) {
			observed, err := groupStatus(ctx, c, node.Spec.Name, "work")
			got := slices.Clone(observed.ConfState.Voters)
			slices.Sort(got)
			return slices.Equal(got, want) && len(observed.ConfState.Learners) == 0 && observed.LeaderID != 0, err
		}); err != nil {
			t.Fatal("declared placement did not govern actual membership", node.Spec.Name, err)
		}
	}
}

func lifecyclePlacementContracts(t *testing.T, size int) {
	spec := matrixSpec(size)
	spec.Limits.MaxNodes = size + 1 // A staged admission whose process never starts.
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	base := created.Document
	if err := transferMeta(ctx, c, "node2", "node1", base.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node2", "work", "before-lifecycle", "before-lifecycle", "retained"); err != nil {
		t.Fatal(err)
	}
	assertActualAPIMembership(t, ctx, c, base, 3)
	for _, bad := range []topology.PlacementRule{{MinReplicas: 1, MaxReplicas: 2}, {MinReplicas: 1}} {
		response, err := c.Request(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(bad, meta.MutationOptions{Actor: "cluster-test"}), base.Version)
		if err := requireStatus(response, err, 400); err != nil {
			t.Fatal("invalid implicit shrink/half rule accepted", err)
		}
	}
	assertAPIViews(t, ctx, c, base)
	stagePlan := func(ingress, method, path string, body any, version uint64) (meta.MutationResult, topology.Plan) {
		t.Helper()
		response, err := c.Request(ctx, ingress, method, path, body, version)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var staged meta.MutationResult
		if err := response.Decode(&staged); err != nil || staged.ExecutionID != "" {
			t.Fatal(staged, err)
		}
		if err := Poll(ctx, func() (bool, error) {
			latest, err := c.Document(ctx, ingress, "latest")
			return latest.Version == staged.Document.Version, err
		}); err != nil {
			t.Fatal(err)
		}
		response, err = c.Request(ctx, ingress, "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var plan topology.Plan
		if err := response.Decode(&plan); err != nil {
			t.Fatal(err)
		}
		return staged, plan
	}
	drained, drainPlan := stagePlan(c.Spec.Nodes[size-1].Name, "POST", "/api/nodes/node3/drain", meta.MutationOptions{Actor: "operator"}, base.Version)
	if !slices.ContainsFunc(drainPlan.Steps, func(s topology.Step) bool {
		return s.Kind == topology.StepRemoveReplica && s.GroupID == "work" && s.NodeName == "node3" && s.ReplicaID == replicaID(base, "work", "node3")
	}) {
		t.Fatal("staged drain lacks exact removal", drainPlan)
	}
	assertActualAPIMembership(t, ctx, c, base, 3)
	drained.ExecutionID, err = c.Apply(ctx, c.Spec.Nodes[size-1].Name, drained.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	assertActualAPIMembership(t, ctx, c, drained.Document, 2)
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if status.MetaLeader != base.MemberID("node1") {
				return true, fmt.Errorf("drain changed meta leader: %+v", status)
			}
			return slices.ContainsFunc(status.Nodes, func(n topology.NodeStatus) bool {
				return n.Node.Name == "node3" && n.Node.State == topology.NodeDraining && n.EffectiveState == topology.NodeDrained && n.Health == topology.HealthAlive
			}), nil
		}); err != nil {
			t.Fatal("drain did not publish fresh liveness/effective state", node.Spec.Name, err)
		}
	}
	// A drained node remains a current meta voter. Explicit Join on its retained
	// disk must not quarantine that live lineage or allocate a replacement.
	beforeRestart, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").StartJoining(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", topology.MetaGroupID)
		if err != nil {
			return false, nil // The returning HTTP server may not be listening yet.
		}
		status, err := c.Status(ctx, "node3")
		return err == nil && status.SessionLive && status.ActiveVersion == drained.Document.Version &&
			local.Identity.MemberID == beforeRestart.Identity.MemberID &&
			local.Identity.InstanceID != beforeRestart.Identity.InstanceID &&
			len(local.Status.ConfState.Voters) == size && local.Status.Applied >= beforeRestart.Status.Applied, nil
	}); err != nil {
		t.Fatal("drained voter did not resume its retained lineage", err)
	}
	assertAPIViews(t, ctx, c, drained.Document)
	for _, node := range c.Nodes() {
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/cluster/parked", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var parked []meta.ParkedJoin
		if err := response.Decode(&parked); err != nil || len(parked) != 0 {
			t.Fatal("current drained voter was parked", node.Spec.Name, parked, err)
		}
	}
	response, err := c.Request(ctx, "node3", "GET", "/health/ready", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("meta restart silently activated the drained application", err)
	}
	capRule := topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}}
	capped, err := c.Mutate(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(capRule, meta.MutationOptions{Apply: true, Actor: "operator"}))
	if err != nil {
		t.Fatal(err)
	}
	activated, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/node3/activate", meta.MutationOptions{Apply: true, Actor: "operator"})
	if err != nil || activated.Document.Node("node3").State != topology.NodeActive || activated.Document.MemberID("node3") != base.MemberID("node3") || !reflect.DeepEqual(activated.Document.Placement, capped.Document.Placement) {
		t.Fatal("activation failed or changed its policy/meta pin", activated, err)
	}
	assertActualAPIMembership(t, ctx, c, activated.Document, 2)
	response, err = c.Request(ctx, "node3", "GET", "/api/load/work/keys/before-lifecycle", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("capped activation silently regrew a local replica", err)
	}
	growRule := capRule
	growRule.MaxReplicas = 3
	grown, growPlan := stagePlan("node2", "PUT", "/api/placement", globalPlacementBody(growRule, meta.MutationOptions{Actor: "operator"}), activated.Document.Version)
	fresh := replicaID(grown.Document, "work", "node3")
	if fresh <= replicaID(base, "work", "node3") {
		t.Fatal("policy growth reused a retired identity", grown)
	}
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter} {
		if !slices.ContainsFunc(growPlan.Steps, func(s topology.Step) bool {
			return s.Kind == kind && s.GroupID == "work" && s.NodeName == "node3" && s.ReplicaID == fresh
		}) {
			t.Fatal("restore lacks exact learner-first plan", kind, growPlan)
		}
	}
	assertActualAPIMembership(t, ctx, c, activated.Document, 2)
	grown.ExecutionID, err = c.Apply(ctx, "node2", grown.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	assertActualAPIMembership(t, ctx, c, grown.Document, 3)
	if err := exactOwnedValues(ctx, c, grown.Document, "work", map[string]string{"before-lifecycle": "retained"}); err != nil {
		t.Fatal("regrowth lost acknowledged data", err)
	}
	observed, err := groupStatus(ctx, c, "node2", "work")
	if err != nil {
		t.Fatal(err)
	}
	var target topology.Replica
	for _, replica := range grown.Document.VotersOf("work") {
		if replica.ReplicaID != observed.LeaderID {
			target = replica
			break
		}
	}
	if target.ReplicaID == 0 {
		t.Fatal("load group has no alternate voter")
	}
	response, err = c.Request(ctx, c.Spec.Nodes[size-1].Name, "POST", "/api/groups/work/leader/"+target.NodeName, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node2", "work")
		return status.LeaderID == target.ReplicaID, err
	}); err != nil {
		t.Fatal("accepted transfer did not move actual leadership", err)
	}
	assertAPIViews(t, ctx, c, grown.Document)
	response, err = c.Request(ctx, "node2", "POST", "/api/groups/undeclared/leader/"+target.NodeName, nil, 0)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("undeclared transfer accepted", err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": map[string]any{"name": "unsupported-field", "type": "worker", "unrecognized": "foreign"}, "mutation": meta.MutationOptions{Actor: "operator"}}, grown.Document.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("caller node decoder silently accepted an unsupported field", err)
	}
	wrongType := grown.Document.Clone()
	wrongType.Node("node3").Type = "observer"
	response, err = c.Request(ctx, "node2", "PUT", "/api/topology", wrongType, grown.Document.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("declared type changed without fresh admission", err)
	}
	assertAPIViews(t, ctx, c, grown.Document)
	// Declare then cancel a never-started admission. The unused ID stays burned;
	// only Latest moves before approval, and no real meta/load voter is allocated.
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": topology.Node{Name: "staged-only", Type: "worker"}, "mutation": meta.MutationOptions{Actor: "operator"}}, grown.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var added meta.MutationResult
	if err := response.Decode(&added); err != nil || added.Document.Node("staged-only") == nil || added.Document.MemberID("staged-only") <= uint64(size) || added.ExecutionID != "" {
		t.Fatal("staged admission lost fresh identity", added, err)
	}
	if err := Poll(ctx, func() (bool, error) {
		doc, err := c.Document(ctx, "node2", "latest")
		return doc.Version == added.Document.Version, err
	}); err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": topology.Node{Name: "staged-only", Type: "worker"}, "mutation": meta.MutationOptions{Actor: "operator"}}, added.Document.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("duplicate declared name was admitted", err)
	}

	for _, view := range []string{"target", "active"} {
		doc, err := c.Document(ctx, "node2", view)
		if err != nil || !reflect.DeepEqual(doc, grown.Document) {
			t.Fatal("staged admission changed an approved view", view, doc, err)
		}
	}
	assertActualAPIMembership(t, ctx, c, grown.Document, 3)
	excluded, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/staged-only/exclude", meta.MutationOptions{Apply: true, Actor: "operator"})
	if err != nil || excluded.Document.Node("staged-only") == nil || excluded.Document.Node("staged-only").State != topology.NodeExcluded || excluded.Document.MemberID("staged-only") != 0 || excluded.Document.MetaGroup().NextReplicaID != added.Document.MetaGroup().NextReplicaID {
		t.Fatal("cancellation lost tombstone/allocation floor", excluded, err)
	}
	assertActualAPIMembership(t, ctx, c, excluded.Document, 3)
	if excluded.Document.Node("never-declared") != nil {
		t.Fatal("document fabricated an unknown node")
	}
	assertAPIViews(t, ctx, c, excluded.Document)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}
