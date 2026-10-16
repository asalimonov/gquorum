//go:build clustertest

package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/asalimonov/gquorum"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestDuplicateNameIntruderExitsWithoutDisturbingHolderThreeNodes(t *testing.T) {
	t.Parallel()
	duplicateNameIntruder(t, 3)
}

// Port the source duplicate-name process test without the application UI/gossip
// renderer. Read the stable typed, exact-process warning through the public
// parked registry from every founder, including followers.
func duplicateNameIntruder(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	before, err := c.Document(ctx, "node1", "active")
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	holder := ""
	for _, node := range c.Spec.Nodes {
		if before.MemberID(node.Name) != status.MetaLeaderID {
			holder = node.Name
			break
		}
	}
	if holder == "" {
		t.Fatal("missing nonleader holder")
	}
	identity, err := c.Local(ctx, holder, topology.MetaGroupID)
	if err != nil || !identity.Found {
		t.Fatal(identity, err)
	}
	intruder, err := c.PrepareNameClaimingNode(ctx, NodeSpec{Name: "intruder", Type: c.Node(holder).Spec.Type}, holder)
	if err != nil {
		t.Fatal(err)
	}
	// Port-exit may race the listening-port readiness check, so Start can either
	// return its port acknowledgement or a container-exited error. The actual
	// container state and exit code are the authoritative assertion.
	startErr := intruder.Start(ctx)
	if err := Poll(ctx, func() (bool, error) {
		state, err := intruder.Container.State(ctx)
		if err != nil {
			return false, err
		}
		if state.Running {
			return false, nil
		}
		if state.ExitCode != 1 {
			return true, fmt.Errorf("duplicate-name process exited %d (start: %v)", state.ExitCode, startErr)
		}
		intruder.running.Store(false)
		return true, nil
	}); err != nil {
		t.Fatal("live name conflict must exit, not retry indefinitely", err)
	}
	logs, err := intruder.Logs(ctx)
	if err != nil || !strings.Contains(logs, "node name is held by another live process") {
		t.Fatal("exit is not an identity refusal", logs, err)
	}
	// Exact identity can be learned from the leader's retained refusal and matched
	// against the actual intruding executable's startup record after it exits.
	var refused topology.ParkedJoin
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Spec.Nodes {
			response, err := c.Request(ctx, node.Name, "GET", "/api/cluster/parked", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
			var entries []topology.ParkedJoin
			if err := response.Decode(&entries); err != nil {
				return true, err
			}
			if len(entries) != 1 {
				return false, fmt.Errorf("unexpected refusal registry: %+v", entries)
			}
			entry := entries[0]
			if entry.Name != holder || entry.InstanceID == "" || entry.InstanceID == identity.Identity.InstanceID || entry.ClusterID != c.ID || entry.Type != c.Node(holder).Spec.Type || entry.Address != "intruder:9000" || entry.ReportedMemberID != 0 || entry.ReportedDocVersion != 0 || entry.RefusalKind != "name_conflict" || entry.Reason == "" || entry.Attempts < 1 || entry.FirstSeen.IsZero() || entry.LastSeen.Before(entry.FirstSeen) {
				return true, fmt.Errorf("refusal has wrong reported process identity: %+v", entry)
			}
			if !strings.Contains(logs, "instance_id="+entry.InstanceID) || !strings.Contains(logs, "node="+holder) {
				return true, fmt.Errorf("warning does not identify the actual intruder: %+v", entry)
			}
			if refused.InstanceID != "" && !reflect.DeepEqual(refused, entry) {
				return true, fmt.Errorf("followers read different leader-owned warnings: %+v / %+v", refused, entry)
			}
			refused = entry
		}
		return true, nil
	}); err != nil {
		t.Fatal("typed conflict warning must survive intruder exit and forward from followers", err)
	}
	// A passive conflict never becomes an Add affordance. The public mutation
	// must refuse that admitted name before staging or allocating any new ID.
	response, err := c.Request(ctx, holder, "POST", "/api/nodes", map[string]any{
		"node": joiningNode(c.Node(holder).Spec), "mutation": gquorum.MutationOptions{Actor: "cluster-test", Approve: true},
	}, before.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("live-name warning must not authorize admission", err)
	}
	for _, node := range c.Spec.Nodes {
		after, err := c.Document(ctx, node.Name, "active")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("refused intruder changed topology", after, err)
		}
		ready, err := c.Request(ctx, node.Name, "GET", "/health/ready", nil, 0)
		if err := requireStatus(ready, err, 200); err != nil {
			t.Fatal("healthy founder stopped serving", node.Name, err)
		}
		observed, err := c.Status(ctx, node.Name)
		if err != nil {
			t.Fatal(err)
		}
		for _, placed := range observed.Nodes {
			if placed.Node.Name == holder && placed.Address != holder+":9000" {
				t.Fatal("refused intruder poisoned holder route", placed)
			}
		}
	}
	retained, err := c.Local(ctx, holder, topology.MetaGroupID)
	if err != nil || retained.Identity != identity.Identity || !retained.Found || len(retained.Status.ConfState.Voters) != size {
		t.Fatal("healthy holder process or voter identity changed", retained, err)
	}
	// The passive warning does not authorize admission or allocate another ID.
	latest, err := c.Document(ctx, holder, "latest")
	if err != nil || !reflect.DeepEqual(before, latest) {
		t.Fatal("conflict allocated a membership identity", latest, err)
	}
}

func TestExcludedSameDiskProcessParksAndReadmitsThreeNodes(t *testing.T) {
	t.Parallel()
	excludedSameDiskRejoin(t, 3)
}
func TestDownNodeRotatedIdentityQuarantinesAndRejoinsThreeNodes(t *testing.T) {
	t.Parallel()
	rotatedDownNodeRejoin(t, 3)
}

func rejoinWorkload(t *testing.T, size int) (context.Context, *Cluster, topology.Document, LocalObservation, map[string]string) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for i := range 10 {
		key := fmt.Sprintf("rejoin-%02d", i)
		value := "value-" + key
		if err := c.Put(ctx, "node3", "work", key, key, value); err != nil {
			t.Fatal(err)
		}
		values[key] = value
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", values); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node3", "work")
	if err != nil || !before.Found || before.ReplicaID == 0 {
		t.Fatal("missing exact returning replica", before, err)
	}
	return ctx, c, created.Document, before, values
}

func assertRejoinedIdentities(ctx context.Context, t *testing.T, c *Cluster, before, after topology.Document, previous LocalObservation, expectedProcess string, values map[string]string) {
	t.Helper()
	if after.MemberID("node3") <= before.MetaGroup().NextReplicaID-1 || replicaID(after, "work", "node3") <= before.Group("work").NextReplicaID-1 || len(after.MetaGroup().Replicas) != len(c.Spec.Nodes) {
		t.Fatal("readmission reused a retired identity or lost membership", before, after)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("returning process failed its serving gate", err)
	}
	if err := c.WaitActive(ctx, after.Version); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, after, "work", values); err != nil {
		t.Fatal("fresh identity failed snapshot recovery", err)
	}
	wantVoters, wantLearners := []uint64{}, []uint64{}
	for _, replica := range after.MetaGroup().Replicas {
		if replica.Role == topology.ReplicaVoter {
			wantVoters = append(wantVoters, replica.ReplicaID)
		} else {
			wantLearners = append(wantLearners, replica.ReplicaID)
		}
	}
	slices.Sort(wantVoters)
	slices.Sort(wantLearners)
	for _, node := range c.Spec.Nodes {
		native, err := c.Local(ctx, node.Name, topology.MetaGroupID)
		gotVoters, gotLearners := slices.Clone(native.Status.ConfState.Voters), slices.Clone(native.Status.ConfState.Learners)
		slices.Sort(gotVoters)
		slices.Sort(gotLearners)
		if err != nil || !native.Found || !slices.Equal(gotVoters, wantVoters) || !slices.Equal(gotLearners, wantLearners) || slices.Contains(gotVoters, previous.Identity.MemberID) || slices.Contains(gotLearners, previous.Identity.MemberID) {
			t.Fatal("fresh meta membership did not match the approved roles", native, err)
		}
		for _, old := range before.MetaGroup().Replicas {
			if old.NodeName != "node3" && after.MemberID(old.NodeName) != old.ReplicaID {
				t.Fatal("readmission rotated another meta member", old)
			}
		}
		response, err := c.Request(ctx, node.Name, "GET", "/api/cluster/parked", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var parked []topology.ParkedJoin
		if err := response.Decode(&parked); err != nil || len(parked) != 0 {
			t.Fatal("admitted process remains parked", parked, err)
		}
	}
	local, err := c.Local(ctx, "node3", "work")
	if err != nil || local.Identity.MemberID != after.MemberID("node3") || local.ReplicaID != replicaID(after, "work", "node3") || local.Identity.InstanceID != expectedProcess || local.Identity.InstanceID == previous.Identity.InstanceID || local.StorageIdentity == nil || local.StorageIdentity.ReplicaID != local.ReplicaID {
		t.Fatal("readmission did not keep the same returning process under exact fresh identities", local, err)
	}
	if err := ownDurableFiles(ctx, c, "node3", "work"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node3", "work", "after-rejoin", "after-rejoin", "serving"); err != nil {
		t.Fatal(err)
	}
	values = maps.Clone(values)
	values["after-rejoin"] = "serving"
	if err := exactOwnedValues(ctx, c, after, "work", values); err != nil {
		t.Fatal(err)
	}
	assertRetiredMetaPin(ctx, t, c, previous.Identity)
}

// Inspect the actual quarantined bbolt file, not just a successful callback log.
func assertRetiredMetaPin(ctx context.Context, t *testing.T, c *Cluster, old topology.Identity) {
	t.Helper()
	response, err := c.Request(ctx, old.NodeName, "GET", "/internal/test/meta-backups", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var paths []string
	if err := response.Decode(&paths); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatal("readmission did not quarantine one exact old meta store", paths)
	}
	for _, remote := range paths {
		if !regexp.MustCompile(`^/data/_meta\.bolt\.retired-[0-9]+-[0-9a-f]{16}$`).MatchString(remote) {
			t.Fatal("unexpected retired meta path", remote)
		}
		source, err := c.Node(old.NodeName).Container.CopyFileFromContainer(ctx, remote)
		if err != nil {
			t.Fatal(err)
		}
		storageDir := t.TempDir()
		file, err := os.Create(filepath.Join(storageDir, "_meta.bolt"))
		if err != nil {
			_ = source.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(file, io.LimitReader(source, 16<<20))
		err = errors.Join(copyErr, source.Close(), file.Close())
		if err != nil {
			t.Fatal(err)
		}
		pin, err := gquorum.LoadStoredIdentity(storageDir)
		if err != nil || !pin.Found || pin.Identity.NodeName != old.NodeName || pin.Identity.ClusterID != old.ClusterID || pin.Identity.MemberID != old.MemberID {
			t.Fatal("quarantined a foreign/successor meta pin", pin.Identity, old, err)
		}
	}
}

func excludedSameDiskRejoin(t *testing.T, size int) {
	t.Helper()
	ctx, c, before, previous, values := rejoinWorkload(t, size)
	drained, err := c.Mutate(ctx, "node2", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Actor: "operator", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	// Self-removal makes the process disappear during convergence; observe the
	// surviving execution explicitly before waiting for its clean exit.
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/node3/exclude", gquorum.MutationOptions{Actor: "operator", Approve: true}, drained.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var excluded gquorum.MutationResult
	if err := response.Decode(&excluded); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, "node1", excluded.OperatorID); err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node3")
	if err := Poll(ctx, func() (bool, error) {
		state, err := victim.Container.State(ctx)
		if err != nil {
			return false, err
		}
		if state.Running {
			return false, nil
		}
		if state.ExitCode != 0 {
			return true, fmt.Errorf("approved self-removal exited %d", state.ExitCode)
		}
		victim.running.Store(false)
		return true, nil
	}); err != nil {
		t.Fatal("exclusion alone must cause clean self-exit", err)
	}
	if err := c.WaitActive(ctx, excluded.Document.Version); err != nil {
		t.Fatal("restart requires fully active exclusion", err)
	}
	logs, err := victim.Logs(ctx)
	if err != nil || !strings.Contains(logs, "self-removal identity verified") || !strings.Contains(logs, "member_id="+strconv.FormatUint(previous.Identity.MemberID, 10)) {
		t.Fatal("self-exit did not verify its exact authorized identity", logs, err)
	}
	volume := victim.Volume
	if err := victim.StartJoining(ctx); err != nil {
		t.Fatal(err)
	}
	if victim.Volume != volume {
		t.Fatal("retained restart replaced the disk")
	}
	// HTTP starts before Node finishes loading and publishing its retained meta
	// identity. Wait for the actual parked lifecycle, not just an open port,
	// before asserting the recovered pin. This also proves admission was refused.
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Status(ctx, "node3")
		if err != nil {
			return false, err
		}
		return status.Lifecycle == "parked", nil
	}); err != nil {
		t.Fatal("retained excluded process did not reach parked state", err)
	}
	parkedIdentity, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil || parkedIdentity.Identity.MemberID != previous.Identity.MemberID || parkedIdentity.Identity.InstanceID == previous.Identity.InstanceID {
		t.Fatal("parked restart replaced pin or reused process", parkedIdentity, err)
	}
	// Keep the excluded process alive and nonserving while both surviving
	// participants read its exact typed refusal. The same parked process must
	// later resume; control-only startup must not reopen ordinary metadata reads.
	refused, err := c.Request(ctx, "node3", "GET", "/api/cluster/parked", nil, 0)
	if err := requireStatus(refused, err, 503); err != nil {
		t.Fatal("parked member admitted an ordinary metadata read", err)
	}
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if err := refused.Decode(&refusal); err != nil || refusal.Code != fault.Stopped {
		t.Fatal("parked member lost its participation refusal", refusal, err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Spec.Nodes {
			if node.Name == victim.Spec.Name {
				continue
			}
			reply, err := c.Request(ctx, node.Name, "GET", "/api/cluster/parked", nil, 0)
			if err := requireStatus(reply, err, 200); err != nil {
				return false, err
			}
			var entries []topology.ParkedJoin
			if err := reply.Decode(&entries); err != nil {
				return true, err
			}
			if len(entries) != 1 {
				return false, nil
			}
			p := entries[0]
			if p.Name != "node3" || p.InstanceID != parkedIdentity.Identity.InstanceID || p.ClusterID != c.ID || p.ReportedMemberID != previous.Identity.MemberID || p.RefusalKind != "excluded" || p.Address != "node3:9000" {
				return true, fmt.Errorf("wrong excluded refusal: %+v", p)
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("excluded return must park with exact typed identity", err)
	}
	hold := time.Now().Add(2 * time.Second)
	for time.Now().Before(hold) {
		live, err := c.Request(ctx, "node3", "GET", "/health/live", nil, 0)
		if err := requireStatus(live, err, 200); err != nil {
			t.Fatal(err)
		}
		ready, err := c.Request(ctx, "node3", "GET", "/health/ready", nil, 0)
		if err := requireStatus(ready, err, 503); err != nil {
			t.Fatal("excluded process is serving", err)
		}
		status, err := c.Status(ctx, "node3")
		if err != nil || status.Lifecycle != "parked" || status.SessionLive || status.SelfRemoval != nil {
			t.Fatal("excluded retained pin restarted its removed voter", status, err)
		}
		load, err := c.Local(ctx, "node3", "work")
		if err != nil || load.Found {
			t.Fatal("parked process hosts application state", load, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{"node": joiningNode(victim.Spec), "mutation": gquorum.MutationOptions{Actor: "operator", Approve: true}}, latest.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var admitted gquorum.MutationResult
	if err := response.Decode(&admitted); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, "node1", admitted.OperatorID); err != nil {
		t.Fatal(err)
	}
	assertRejoinedIdentities(ctx, t, c, before, admitted.Document, previous, parkedIdentity.Identity.InstanceID, values)
}

func rotatedDownNodeRejoin(t *testing.T, size int) {
	t.Helper()
	ctx, c, before, previous, values := rejoinWorkload(t, size)
	// A healthy node must be refused before any allocation/proposal.
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes/node3/readmit", map[string]any{"actor": "operator", "approve": true, "expected_member_id": before.MemberID("node3")}, before.Version)
	if err := requireStatus(response, err, 422); err != nil {
		t.Fatal("readmission rotated a live holder", err)
	}
	victim := c.Node("node3")
	if err := victim.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitClusterHealth(ctx, c, "node3"); err != nil {
		t.Fatal(err)
	}
	var rotated gquorum.MutationResult
	if err := Poll(ctx, func() (bool, error) {
		response, err = c.Request(ctx, "node2", "POST", "/api/nodes/node3/readmit", map[string]any{"actor": "operator", "approve": true, "expected_member_id": before.MemberID("node3")}, before.Version)
		if err != nil {
			return false, err
		}
		if response.Status == 202 {
			return true, response.Decode(&rotated)
		}
		// The source helper preserves a committed staged version when Apply
		// fails. A repeated mutation with the original CAS would only conflict
		// and must never allocate another incarnation.
		latest, readErr := c.Document(ctx, "node1", "latest")
		if readErr != nil {
			return false, readErr
		}
		if latest.Version > before.Version {
			if latest.Version != before.Version+1 || latest.ParentVersion != before.Version || latest.ChangedBy != "operator" || latest.MemberID("node3") != before.MetaGroup().NextReplicaID || replicaID(latest, "work", "node3") != before.Group("work").NextReplicaID {
				return true, fmt.Errorf("cannot adopt unrelated staged readmission: %+v", latest)
			}
			t.Logf("readmission staged v%d; retry approval after HTTP %d: %s", latest.Version, response.Status, response.Data)
			rotated.Document = latest
			return true, nil
		}
		return false, requireStatus(response, nil, 202)
	}); err != nil {
		t.Fatal("operator readmission while down", err)
	}
	if rotated.OperatorID == "" {
		if err := Poll(ctx, func() (bool, error) {
			response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/approve", rotated.Document.Version), nil, 0)
			if err != nil {
				return false, err
			}
			if response.Status != 202 {
				return false, requireStatus(response, nil, 202)
			}
			var approval struct {
				OperatorID topology.OperatorID `json:"operator_id"`
			}
			if err := response.Decode(&approval); err != nil {
				return true, err
			}
			rotated.OperatorID = approval.OperatorID
			return approval.OperatorID != "", nil
		}); err != nil {
			t.Fatal("approval of exact staged readmission", err)
		}
	}
	if rotated.Document.MemberID("node3") <= previous.Identity.MemberID || replicaID(rotated.Document, "work", "node3") <= previous.ReplicaID || rotated.OperatorID == "" {
		t.Fatal("operator did not commit/approve exact fresh identities", rotated)
	}
	// The old physical files still exist while it is down. Approval alone cannot
	// erase them or replace the disk; quarantine belongs to the returning callback.
	if err := ownDurableFiles(ctx, c, "node3", "work"); err != nil {
		t.Fatal(err)
	}
	operator, err := c.Operator(ctx, "node1", rotated.OperatorID)
	if err != nil || operator.State.Terminal() {
		t.Fatal("down node completed learner admission without its process", operator, err)
	}
	volume := victim.Volume
	if err := victim.StartJoining(ctx); err != nil {
		t.Fatal(err)
	}
	if victim.Volume != volume {
		t.Fatal("rotated return wiped its disk")
	}
	// The returning process binds its _meta observation while it joins.
	var returned LocalObservation
	err = Poll(ctx, func() (bool, error) {
		var err error
		returned, err = c.Local(ctx, "node3", topology.MetaGroupID)
		return err == nil, err
	})
	if err != nil || returned.Identity.InstanceID == previous.Identity.InstanceID {
		t.Fatal("returning process identity missing", returned, err)
	}
	if err := c.WaitOperator(ctx, "node1", rotated.OperatorID); err != nil {
		t.Fatal(err)
	}
	assertRejoinedIdentities(ctx, t, c, before, rotated.Document, previous, returned.Identity.InstanceID, values)
}
