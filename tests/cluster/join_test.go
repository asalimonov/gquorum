//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"

	"github.com/asalimonov/gquorum/topology"
)

func joiningNode(spec NodeSpec) topology.Node {
	return topology.Node{Name: spec.Name, Type: spec.Type, Labels: map[string]string{
		"example.meta_address": spec.Name + ":9000",
		"example.load_address": spec.Name + ":9001",
		"example.http_address": spec.Name + ":8080",
	}}
}

func TestMetaAdmissionUsesLearnerBeforeServingAndRetainsIdentityOnRestart(t *testing.T) {
	t.Parallel()
	testMetaAdmission(t, false)
}

func TestAdmissionOperatorResumesAfterMetaLeaderElectionWithoutNewAttempt(t *testing.T) {
	t.Parallel()
	testMetaAdmission(t, true)
}

func testMetaAdmission(t *testing.T, electionDuringLearner bool) {
	t.Helper()
	spec := matrixSpec(3)
	spec.Limits.MaxNodes, spec.Limits.MaxReplicas = 4, 4
	spec.MetaFaultProxy = electionDuringLearner
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "before-join", "before-join", "retained"); err != nil {
		t.Fatal(err)
	}
	// Raising the desired bound does not exceed Limits or invent another cap.
	placement := created.Document.Clone()
	placement.Placement.MaxReplicas = 4
	if electionDuringLearner {
		// Exercise an explicitly requested four-voter admission, including the
		// existing promotion recovery barrier. Default admission stays learner.
		placement.MetaGroup().VoterPolicy.TargetVoters = 4
		placement.Group("work").VoterPolicy.TargetVoters = 4
	}
	staged, err := c.Stage(ctx, "node2", placement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	node, err := c.PrepareJoinNode(ctx, NodeSpec{Name: "node4", Type: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	barrierKind := topology.StepAddLearner
	barrierQuery := "?after=true"
	if electionDuringLearner {
		barrierKind, barrierQuery = topology.StepPromoteVoter, ""
	}
	for _, seed := range c.Spec.Nodes {
		response, err := c.Request(ctx, seed.Name, "PUT", "/internal/test/barriers/work/"+string(barrierKind)+barrierQuery, nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/nodes", map[string]any{
		"node": joiningNode(node.Spec), "mutation": gquorum.MutationOptions{Actor: "cluster-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var admitted gquorum.MutationResult
	if err := response.Decode(&admitted); err != nil {
		t.Fatal(err)
	}
	for _, group := range admitted.Document.Groups {
		for _, replica := range group.Replicas {
			if replica.NodeName == "node4" && replica.Role != topology.ReplicaLearner {
				t.Fatal("new admission did not default to a stable learner", group.ID, replica)
			}
		}
	}
	if electionDuringLearner {
		desired := admitted.Document.Clone()
		for gi := range desired.Groups {
			for ri := range desired.Groups[gi].Replicas {
				if desired.Groups[gi].Replicas[ri].NodeName == "node4" {
					desired.Groups[gi].Replicas[ri].Role = topology.ReplicaVoter
				}
			}
		}
		admitted, err = c.Stage(ctx, "node2", desired)
		if err != nil {
			t.Fatal(err)
		}
	}
	admitted.OperatorID = startApply(t, ctx, c, "node2", admitted.Document.Version)
	member := admitted.Document.MemberID("node4")
	loadID := replicaID(admitted.Document, "work", "node4")
	if member == 0 || loadID == 0 {
		t.Fatal("admission did not allocate exact meta/load identities", admitted)
	}
	started := make(chan error, 1)
	go func() { started <- node.Start(ctx) }()
	defer func() { cancel(); <-started }()
	learnerSeen := false
	elected := false
	var replacementMember uint64
	observer := "node1"
	var healController func()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := c.Status(ctx, observer)
		if err == nil {
			if status.Operator != nil && status.Operator.ID == admitted.OperatorID && status.Operator.State.Terminal() && status.Operator.State != topology.OperatorSucceeded {
				t.Fatalf("meta admission terminated before load growth: %+v", status.Operator)
			}
			for _, group := range status.Groups {
				if group.GroupID == topology.MetaGroupID && slices.Contains(group.ConfState.Learners, member) {
					learnerSeen = true
					if electionDuringLearner && !elected {
						oldLeader := ""
						for _, r := range admitted.Document.MetaGroup().Replicas {
							if r.ReplicaID == status.MetaLeaderID {
								oldLeader = r.NodeName
								break
							}
						}
						if oldLeader == "" {
							t.Fatal("cannot map exact metadata leader during admission", status)
						}
						if observer == oldLeader {
							observer = "node2"
						}
						local, err := c.Local(ctx, oldLeader, topology.MetaGroupID)
						if err != nil {
							t.Fatal(err)
						}
						// Pending topology owns administration. Use a real election
						// while preserving the former controller's process and loads.
						healController = holdMetaRaft(t, ctx, c, oldLeader, local.Identity)
						if err := Poll(ctx, func() (bool, error) {
							changed, err := c.Status(ctx, observer)
							if err != nil || changed.MetaLeaderID == 0 || changed.MetaLeaderID == local.Identity.MemberID {
								return false, err
							}
							if changed.Operator == nil || changed.Operator.ID != admitted.OperatorID || changed.Operator.Attempt != 1 {
								return true, fmt.Errorf("election changed exact admission execution: %+v", changed.Operator)
							}
							replacementMember = changed.MetaLeaderID
							return true, nil
						}); err != nil {
							t.Fatal("election during admitted learner execution", err)
						}
						elected = true
					}
				}
			}
			if status.Operator != nil && status.Operator.ID == admitted.OperatorID && status.Operator.GroupID == "work" && status.Operator.CurrentStep > 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("meta admission did not reach load growth", err)
		case <-ticker.C:
		}
	}
	if healController != nil {
		healController()
	}
	if electionDuringLearner && !elected {
		t.Fatal("execution never replaced its leader while the new meta member was a learner")
	}
	if !learnerSeen {
		t.Fatal("new meta member was never observed as a learner")
	}
	select {
	case err := <-started:
		started <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("joining process did not start")
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node4", "work")
		return err == nil && local.Found && local.ReplicaID == loadID && slices.Contains(local.Status.ConfState.Learners, loadID) && local.Values["before-join"] == "retained", err
	}); err != nil {
		t.Fatal("joining node never became a caught-up load learner", err)
	}
	local, err := c.Local(ctx, "node4", "work")
	if err != nil || local.Identity.MemberID != member {
		t.Fatal("joining process owns another meta identity", local.Identity, err)
	}
	instance := local.Identity.InstanceID
	if electionDuringLearner {
		operator, err := c.Operator(ctx, observer, admitted.OperatorID)
		if err != nil || operator.ID != admitted.OperatorID || operator.Attempt != 1 || operator.Version != admitted.Document.Version || operator.State.Terminal() {
			t.Fatal("leader replacement changed or finished the paused execution", operator, err)
		}
		status, err := c.Status(ctx, observer)
		if err != nil || status.MetaLeaderID != replacementMember {
			t.Fatal("replacement meta leader did not retain the ongoing execution", status.MetaLeaderID, err)
		}
		// The stored execution's current exact step is exposed by the authorized
		// receiver, even if planning against already changed membership differs.
		if err := Poll(ctx, func() (bool, error) {
			current, err := c.Operator(ctx, observer, admitted.OperatorID)
			if err != nil {
				return false, err
			}
			if current.ID != admitted.OperatorID || current.Attempt != 1 || current.Version != admitted.Document.Version || current.State.Terminal() {
				return true, fmt.Errorf("replacement changed the paused execution: %+v", current)
			}
			for _, seed := range c.Spec.Nodes {
				controls, err := c.Controls(ctx, seed.Name)
				if err != nil {
					return false, err
				}
				for _, barrier := range controls.Barriers {
					a := barrier.Last
					if barrier.Hits > 0 && a.OperatorID == admitted.OperatorID && a.Attempt == 1 && a.Version == admitted.Document.Version && a.StepSeq == current.CurrentStep && a.Kind == topology.StepPromoteVoter && a.GroupID == "work" && a.NodeName == "node4" && a.ReplicaID == loadID {
						return true, nil
					}
				}
			}
			return false, nil
		}); err != nil {
			t.Fatal("replacement controller did not resume the exact authorized promotion", err)
		}
	}
	response, err = c.Request(ctx, "node4", "GET", "/health/ready", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("joining learner became application-ready before approval completed", err)
	}
	response, err = c.Request(ctx, "node4", "GET", "/api/load/work/keys/before-join", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("joining learner served application state", err)
	}
	for _, seed := range c.Spec.Nodes {
		if err := c.SetBarrier(ctx, seed.Name, "work", barrierKind, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitOperator(ctx, "node2", admitted.OperatorID); err != nil {
		t.Fatal(err)
	}
	if electionDuringLearner {
		operator, err := c.Operator(ctx, "node2", admitted.OperatorID)
		if err != nil || operator.Attempt != 1 || operator.ID != admitted.OperatorID || operator.State != topology.OperatorSucceeded {
			t.Fatal("resumed admission created another attempt", operator, err)
		}
	}
	if err := c.WaitActive(ctx, admitted.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node4", "work", "after-join", "after-join", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	for _, current := range c.Nodes() {
		if err := c.WaitValue(ctx, current.Spec.Name, "work", "after-join", "acknowledged"); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	local, err = c.Local(ctx, "node4", "work")
	if err != nil || !local.Found || local.Identity.InstanceID == instance || local.Identity.MemberID != member || local.ReplicaID != loadID || slices.Contains(local.Status.ConfState.Voters, loadID) != electionDuringLearner || slices.Contains(local.Status.ConfState.Learners, loadID) == electionDuringLearner || local.Values["before-join"] != "retained" || local.Values["after-join"] != "acknowledged" {
		t.Fatal("same-store joining restart lost its exact state/identity", local, err)
	}
	for _, current := range c.Nodes() {
		latest, err := c.Document(ctx, current.Spec.Name, "latest")
		if err != nil || latest.Version != admitted.Document.Version || latest.MemberID("node4") != member {
			t.Fatal("retained restart churned the admission document", current.Spec.Name, latest, err)
		}
		status, err := groupStatus(ctx, c, current.Spec.Name, topology.MetaGroupID)
		wantVoters := 3
		if electionDuringLearner {
			wantVoters = 4
		}
		if err != nil || len(status.ConfState.Voters) != wantVoters || slices.Contains(status.ConfState.Learners, member) == electionDuringLearner || slices.Contains(status.ConfState.Voters, member) != electionDuringLearner {
			t.Fatal(fmt.Sprintf("%s meta member did not retain its approved role", current.Spec.Name), status, err)
		}
	}
}

func TestMetaPinnedAdmissionRestartsBeforeAndAfterReadyWithoutAllocatingIDs(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"pending", "ready"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			spec := matrixSpec(3)
			spec.Limits.MaxNodes = 4
			c := acquire(t, spec).Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Second)
			defer cancel()
			// Only the joining container uses the protocol-driver boot mode. Its next
			// process uses the ordinary SDK; both processes retain the same private volume.
			node, err := c.PrepareJoinNode(ctx, NodeSpec{Name: "node4", Type: "worker", AdmissionPinPhase: phase})
			if err != nil {
				t.Fatal(err)
			}
			base, err := c.Document(ctx, "node1", "latest")
			if err != nil {
				t.Fatal(err)
			}
			response, err := c.Request(ctx, "node1", "POST", "/api/nodes", map[string]any{"node": joiningNode(node.Spec), "mutation": gquorum.MutationOptions{Approve: true}}, base.Version)
			if err := requireStatus(response, err, 202); err != nil {
				t.Fatal(err)
			}
			var admission gquorum.MutationResult
			if err := response.Decode(&admission); err != nil {
				t.Fatal(err)
			}
			if err := node.Start(ctx); err != nil {
				op, readErr := c.Operator(ctx, "node1", admission.OperatorID)
				t.Logf("approved pin-driver admission: %+v (read error: %v)", op, readErr)
				t.Fatal(err)
			}
			response, err = c.Request(ctx, "node4", "GET", "/internal/test/admission-pin", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				t.Fatal(err)
			}
			var pin struct {
				Identity          topology.Identity `json:"identity"`
				AuthorityIdentity topology.Identity `json:"authority_identity"`
				MetadataAcquired  bool              `json:"metadata_acquired"`
				Found             bool              `json:"found"`
				Instance          string            `json:"instance_id"`
				Phase             string            `json:"phase"`
			}
			if err := response.Decode(&pin); err != nil {
				t.Fatal(err)
			}
			if !pin.Found || pin.Identity.MemberID != admission.Document.MemberID("node4") || pin.Identity.ClusterID != c.ID || pin.Identity.NodeName != "node4" || pin.Instance == "" || pin.Phase != phase {
				t.Fatal("pending grant was not durably pinned", pin)
			}
			if pin.MetadataAcquired || pin.AuthorityIdentity.ClusterID != pin.Identity.ClusterID || pin.AuthorityIdentity.NodeName != pin.Identity.NodeName || pin.AuthorityIdentity.MemberID != pin.Identity.MemberID || pin.AuthorityIdentity.InstanceID != pin.Instance {
				t.Fatal("pin driver did not retain the independent unacquired authority boundary", pin)
			}
			if err := node.Kill(ctx); err != nil {
				t.Fatal(err)
			}
			if err := node.StartJoining(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitOperator(ctx, "node1", admission.OperatorID); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitActive(ctx, admission.Document.Version); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			own, err := c.Local(ctx, "node4", topology.MetaGroupID)
			if err != nil || own.Identity.MemberID != pin.Identity.MemberID || own.Identity.InstanceID == pin.Instance {
				t.Fatal("restart allocated another identity or retained process incarnation", own, err)
			}
			for _, participant := range c.Nodes() {
				latest, err := c.Document(ctx, participant.Spec.Name, "latest")
				if err != nil || latest.Version != admission.Document.Version || latest.MemberID("node4") != pin.Identity.MemberID {
					t.Fatal("pinned admission restart churned topology", latest, err)
				}
			}
			operator, err := c.Operator(ctx, "node1", admission.OperatorID)
			if err != nil || operator.Attempt != 1 || operator.State != topology.OperatorSucceeded {
				t.Fatal("restart replaced the exact admission attempt", operator, err)
			}
		})
	}
}
