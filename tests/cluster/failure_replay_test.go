//go:build clustertest

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

type lostReplyTransport struct {
	base     http.RoundTripper
	accepted Response
}

func (r *lostReplyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	reply, err := r.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer reply.Body.Close()
	r.accepted.Status = reply.StatusCode
	r.accepted.Data, err = io.ReadAll(io.LimitReader(reply.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF // No status or body reaches the simulated caller.
}

// Retain the intercepted response only as the test oracle. Replay below uses
// the original expected version/predecessor, never fields from this response.
func discardMutationReply(ctx context.Context, t *testing.T, c *Cluster, method, path string, body any, expected uint64, status int) Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.Node("node1").Endpoint()+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if expected != 0 {
		request.Header.Set("If-Match", strconv.FormatUint(expected, 10))
	}
	base := c.http.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	transport := &lostReplyTransport{base: base}
	client := *c.http // Do not mutate the pool's concurrently usable client.
	client.Transport = transport
	if _, err := client.Do(request); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("caller did not lose its reply", err)
	}
	if err := requireStatus(transport.accepted, nil, status); err != nil {
		t.Fatal("fault did not follow acceptance", err)
	}
	return transport.accepted
}

func TestFailureIsolation_ExactMutationReplay(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"group": gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}}, "mutation": gquorum.MutationOptions{Actor: "uncertain-caller"}}
	lost := discardMutationReply(ctx, t, c, "POST", "/api/groups", body, base.Version, 200)
	response, err := c.Request(ctx, "node1", "POST", "/api/groups", body, base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var first, replayed gquorum.MutationResult
	if err := lost.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&replayed); err != nil || replayed.Document.Version != first.Document.Version || replayed.Document.Version != base.Version+1 || replayed.Document.Group("work").NextReplicaID != first.Document.Group("work").NextReplicaID {
		t.Fatal("lost Stage reply allocated twice", first, replayed, err)
	}
	approvePath := fmt.Sprintf("/api/topology/%d/approve", replayed.Document.Version)
	lost = discardMutationReply(ctx, t, c, "POST", approvePath, struct{}{}, 0, 202)
	operation := startApply(ctx, t, c, "node1", replayed.Document.Version)
	var original struct {
		ID topology.OperationID `json:"operation_id"`
	}
	if err := lost.Decode(&original); err != nil || original.ID != operation {
		t.Fatal("approval created another attempt", original, operation, err)
	}
	if err := c.WaitOperation(ctx, "node1", operation); err != nil {
		t.Fatal(err)
	}
	old := replicaID(replayed.Document, "work", "node3")
	body = map[string]any{"replica_id": old, "actor": "uncertain-caller", "comment": "one replacement"}
	path := "/api/groups/work/replicas/node3/reseat"
	lost = discardMutationReply(ctx, t, c, "POST", path, body, replayed.Document.Version, 202)
	response, err = c.Request(ctx, "node1", "POST", path, body, replayed.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var replacement, replacementReplay gquorum.MutationResult
	if err := lost.Decode(&replacement); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&replacementReplay); err != nil || replacementReplay.Document.Version != replacement.Document.Version || replacementReplay.OperationID != replacement.OperationID ||
		replicaID(replacementReplay.Document, "work", "node3") != replicaID(replacement.Document, "work", "node3") ||
		replacementReplay.Document.Group("work").NextReplicaID != replacement.Document.Group("work").NextReplicaID {
		t.Fatal("lost replacement reply consumed another replica ID", replacement, replacementReplay, err)
	}
	if err := c.WaitOperation(ctx, "node1", replacementReplay.OperationID); err != nil {
		t.Fatal(err)
	}
	for _, n := range c.Nodes() {
		response, err := c.Request(ctx, n.Spec.Name, "PUT", "/internal/test/barriers/work/stop_replica?refuse=true", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	response, err = c.Request(ctx, "node1", "DELETE", "/api/groups/work", gquorum.MutationOptions{Actor: "uncertain-caller", Approve: true}, replacementReplay.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var deletion gquorum.MutationResult
	if err := response.Decode(&deletion); err != nil {
		t.Fatal(err)
	}
	var failed topology.OperationStatus
	if err := Poll(ctx, func() (bool, error) {
		var err error
		failed, err = c.Operation(ctx, "node1", deletion.OperationID)
		return failed.State.Terminal(), err
	}); err != nil || failed.State != topology.OperationFailed {
		t.Fatal("fixture did not create a retryable failed attempt", failed, err)
	}
	for _, n := range c.Nodes() {
		if err := c.SetBarrier(ctx, n.Spec.Name, "work", topology.StepStopReplica, false); err != nil {
			t.Fatal(err)
		}
	}
	path = "/api/operations/" + string(failed.ID) + "/retry"
	lost = discardMutationReply(ctx, t, c, "POST", path, struct{}{}, 0, 202)
	response, err = c.Request(ctx, "node1", "POST", path, nil, 0)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var retry struct {
		ID topology.OperationID `json:"operation_id"`
	}
	if err := lost.Decode(&original); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&retry); err != nil || retry.ID == "" || retry.ID != original.ID || retry.ID == failed.ID {
		t.Fatal("lost retry reply created another edge", retry, original, err)
	}
	if err := c.WaitOperation(ctx, "node1", retry.ID); err != nil {
		t.Fatal(err)
	}
	op, err := c.Operation(ctx, "node1", retry.ID)
	if err != nil || op.Version != failed.Version || op.Attempt != failed.Attempt+1 {
		t.Fatal("retry changed its approved version or attempt", op, err)
	}
	if got := startApply(ctx, t, c, "node1", deletion.Document.Version); got != failed.ID {
		t.Fatal("approval replay silently selected retry", got, failed.ID)
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil || latest.Version != deletion.Document.Version {
		t.Fatal("replay allocated another topology version", latest, err)
	}
}
