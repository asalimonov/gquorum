//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

const receiptGroup = "receipts"

type processSubmission struct {
	Submission load.Submission `json:"submission"`
	ErrorCode  string          `json:"error_code,omitempty"`
	Error      string          `json:"error,omitempty"`
}

func submissionCluster(t *testing.T) (context.Context, *Cluster, topology.Document) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	c := acquire(t, matrixSpec(3)).Cluster
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: receiptGroup})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, c, created.Document
}

func submitAt(ctx context.Context, c *Cluster, node string, replica uint64, id, value string) (processSubmission, error) {
	var out processSubmission
	response, err := c.Request(ctx, node, http.MethodPost,
		fmt.Sprintf("/internal/test/submission/%s/%d", receiptGroup, replica),
		map[string]string{"request_id": id, "key": "key", "value": value}, 0)
	if err := requireStatus(response, err, http.StatusOK); err != nil {
		return out, err
	}
	return out, response.Decode(&out)
}

func requireAppliedSubmission(t *testing.T, c *Cluster, node string, replica uint64, id string, out processSubmission) {
	t.Helper()
	s := out.Submission
	if out.Error != "" || s.Disposition != load.SubmissionApplied || !s.ResultAvailable || s.AttemptID == "" ||
		s.RequestID != id || s.RunID == "" || s.Index == 0 || s.Term == 0 ||
		s.Identity.ClusterID != c.expectedCluster(node) || s.Identity.NodeName != node ||
		s.Identity.GroupID != receiptGroup || s.Identity.ReplicaID != replica {
		t.Fatal("invalid origin applied receipt", out)
	}
}

func TestSubmissionFollowerAndLearnerAppliedThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, doc := submissionCluster(t)
	demoted := doc.Clone()
	for i := range demoted.Group(receiptGroup).Replicas {
		r := &demoted.Group(receiptGroup).Replicas[i]
		if r.NodeName == "node3" {
			r.Role, r.PinnedLearner = topology.ReplicaLearner, true
		}
	}
	staged, err := c.Stage(ctx, "node2", demoted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := requestLeaderTransfer(ctx, c, "node2", receiptGroup, "node1"); err != nil {
		t.Fatal(err)
	}
	leader := replicaID(doc, receiptGroup, "node1")
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range []string{"node2", "node3"} {
			local, err := c.Local(ctx, node, receiptGroup)
			if err != nil || !local.Found || local.Status.LeaderID != leader {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{"node2", "node3"} {
		id := "from/" + node
		replica := replicaID(doc, receiptGroup, node)
		out, err := submitAt(ctx, c, node, replica, id, node)
		if err != nil {
			t.Fatal(err)
		}
		requireAppliedSubmission(t, c, node, replica, id, out)
		if out.Submission.Result.Code != "ok" {
			t.Fatal(out)
		}
	}
	if err := exactOwnedValues(ctx, c, staged.Document, receiptGroup, map[string]string{"key": "node3"}); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionBusinessRejectionThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, doc := submissionCluster(t)
	for i, value := range []string{"first", "conflicting"} {
		node := fmt.Sprintf("node%d", i+1)
		replica := replicaID(doc, receiptGroup, node)
		out, err := submitAt(ctx, c, node, replica, "logical", value)
		if err != nil {
			t.Fatal(err)
		}
		requireAppliedSubmission(t, c, node, replica, "logical", out)
		want := "ok"
		if i == 1 {
			want = string(fault.VersionConflict)
		}
		if out.Submission.Result.Code != want {
			t.Fatal("business result changed", out)
		}
	}
	if err := exactOwnedValues(ctx, c, doc, receiptGroup, map[string]string{"key": "first"}); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionConcurrentLogicalIDThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, doc := submissionCluster(t)
	var wg sync.WaitGroup
	out := make([]processSubmission, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range out {
		wg.Go(func() {
			<-start
			node := fmt.Sprintf("node%d", i+1)
			out[i], errs[i] = submitAt(ctx, c, node, replicaID(doc, receiptGroup, node), "same-logical", "same")
		})
	}
	close(start)
	wg.Wait()
	for i := range out {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		node := fmt.Sprintf("node%d", i+1)
		requireAppliedSubmission(t, c, node, replicaID(doc, receiptGroup, node), "same-logical", out[i])
		if out[i].Submission.Result.Code != "ok" {
			t.Fatal(out[i])
		}
	}
	if out[0].Submission.AttemptID == out[1].Submission.AttemptID || out[0].Submission.Index == out[1].Submission.Index {
		t.Fatal("library suppressed separate log attempts", out)
	}
	if err := exactOwnedValues(ctx, c, doc, receiptGroup, map[string]string{"key": "same"}); err != nil {
		t.Fatal(err)
	}
}
