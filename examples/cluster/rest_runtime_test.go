package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

type restCluster struct {
	apps   []*application
	peers  []peer
	ctx    context.Context
	client *http.Client
}

func newRESTCluster(t *testing.T) *restCluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	c := &restCluster{ctx: ctx, client: &http.Client{Timeout: 5 * time.Second}}
	var all []listeners
	for i := range 3 {
		var ls listeners
		for _, dest := range []*net.Listener{&ls.meta, &ls.load, &ls.http} {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			*dest = listener
			t.Cleanup(func() { _ = listener.Close() })
		}
		all = append(all, ls)
		typ := topology.NodeType("worker")
		if i == 2 {
			typ = "observer"
		}
		c.peers = append(c.peers, peer{Name: fmt.Sprintf("node%d", i+1), Type: typ, MemberID: uint64(i + 1), MetaAddress: ls.meta.Addr().String(), LoadAddress: ls.load.Addr().String(), HTTPAddress: ls.http.Addr().String()})
	}
	t.Cleanup(func() {
		for _, app := range c.apps {
			app.RequestClose()
		}
		for _, app := range c.apps {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := app.Close(closeCtx)
			cancel()
			if err != nil {
				t.Errorf("close example: %v", err)
			}
		}
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i, p := range c.peers {
		cfg := config{NodeName: p.Name, NodeType: p.Type, ClusterID: t.Name(), InstanceID: fmt.Sprintf("process-%d", i), DataDir: filepath.Join(t.TempDir(), p.Name), Credential: "example-test", Peers: c.peers, Limits: (topology.Limits{}).WithDefaults(), SessionTTL: 3 * time.Second, TickInterval: 50 * time.Millisecond, SnapshotThreshold: 8, Bootstrap: true, Reconciliation: meta.ReconciliationOptions{Disabled: true}}
		app, err := newApplication(cfg, logger, &all[i])
		if err != nil {
			t.Fatal(err)
		}
		c.apps = append(c.apps, app)
	}
	for _, app := range c.apps {
		if err := app.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	c.wait(t, "bootstrap and serving", 20*time.Second, func() bool {
		for i := range c.apps {
			code, _ := c.request(t, i, "GET", "/health/ready", nil, 0)
			if code != 200 {
				return false
			}
			doc := c.document(t, i, "active")
			if doc.Group("demo") == nil || doc.Group("workers") == nil {
				return false
			}
		}
		return true
	})
	return c
}
func (c *restCluster) request(t *testing.T, node int, method, path string, body any, expected uint64) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(c.ctx, method, "http://"+c.peers[node].HTTPAddress+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if expected > 0 {
		req.Header.Set("If-Match", fmt.Sprintf("\"%d\"", expected))
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		if c.ctx.Err() != nil {
			t.Fatal(c.ctx.Err())
		}
		return 0, []byte(err.Error())
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}
func (c *restCluster) wait(t *testing.T, what string, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && c.ctx.Err() == nil {
		if check() {
			return
		}
		timer := time.NewTimer(40 * time.Millisecond)
		select {
		case <-c.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	for i := range c.apps {
		code, data := c.request(t, i, "GET", "/api/cluster/status", nil, 0)
		t.Logf("node%d status(%d): %s", i+1, code, data)
	}
	t.Fatalf("timed out waiting for %s: %v", what, c.ctx.Err())
}
func (c *restCluster) document(t *testing.T, node int, view string) topology.Document {
	t.Helper()
	code, data := c.request(t, node, "GET", "/api/topology/"+view, nil, 0)
	if code != 200 {
		t.Fatalf("document: %d %s", code, data)
	}
	var doc topology.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}
func (c *restCluster) mutate(t *testing.T, path, method string, body any) meta.MutationResult {
	t.Helper()
	doc := c.document(t, 0, "latest")
	code, data := c.request(t, 0, method, path, body, doc.Version)
	if code != 202 {
		t.Fatalf("%s %s: %d %s", method, path, code, data)
	}
	var result meta.MutationResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	c.waitExecution(t, result.ExecutionID)
	c.wait(t, "active document on all nodes", 5*time.Second, func() bool {
		for i := range c.apps {
			if c.document(t, i, "active").Version != result.Document.Version {
				return false
			}
		}
		return true
	})
	return result
}
func (c *restCluster) waitExecution(t *testing.T, id topology.ExecutionID) {
	t.Helper()
	c.wait(t, "execution "+string(id), 25*time.Second, func() bool {
		code, data := c.request(t, 0, "GET", "/api/executions/"+string(id), nil, 0)
		if code != 200 {
			return false
		}
		var status topology.ExecutionStatus
		if err := json.Unmarshal(data, &status); err != nil {
			t.Fatal(err)
		}
		if status.State.Terminal() && status.State != topology.ExecutionSucceeded {
			t.Fatalf("failed execution: %s", data)
		}
		return status.State == topology.ExecutionSucceeded
	})
}
func TestExampleRESTDrainActivateAndRetirement(t *testing.T) {
	if testing.Short() {
		t.Skip("real three-node HTTP/meta/load functional test")
	}
	c := newRESTCluster(t)
	doc := c.document(t, 0, "active")
	if len(doc.MetaGroup().Replicas) != 3 || len(doc.Group("demo").Replicas) != 3 || len(doc.Group("workers").Replicas) != 2 {
		t.Fatal("node-type placement", doc)
	}
	oldDemoID := doc.Group("demo").Replicas[2].ReplicaID

	// Cross the snapshot threshold before removing a demo replica, then verify
	// the rejoining node itself, including a retry of an old request identity.
	for i := range 20 {
		code, data := c.request(t, 1, "PUT", "/api/load/demo/keys/recover", map[string]string{"request_id": fmt.Sprintf("demo-%d", i), "value": fmt.Sprintf("value-%d", i)}, 0)
		if code != 200 {
			t.Fatal("demo proposal", code, string(data))
		}
	}
	c.mutate(t, "/api/groups/demo/placement", "PUT", map[string]any{
		"placement": topology.PlacementRule{MinReplicas: 2, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}},
		"mutation":  meta.MutationOptions{Apply: true},
	})
	c.mutate(t, "/api/groups/demo/placement", "PUT", map[string]any{
		"placement": topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3},
		"mutation":  meta.MutationOptions{Apply: true},
	})
	doc = c.document(t, 0, "active")
	for _, replica := range doc.Group("demo").Replicas {
		if replica.NodeName == "node3" {
			oldDemoID = replica.ReplicaID
		}
	}
	c.wait(t, "worker group leader", 5*time.Second, func() bool {
		code, _ := c.request(t, 2, "PUT", "/api/load/workers/keys/key", map[string]string{"request_id": "write-1", "value": "first"}, 0)
		return code == 200
	})
	for i := range 2 {
		node := i
		c.wait(t, "replicated worker value", 5*time.Second, func() bool {
			code, data := c.request(t, node, "GET", "/api/load/workers/keys/key", nil, 0)
			return code == 200 && bytes.Contains(data, []byte(`"value":"first"`))
		})
	}
	if code, data := c.request(t, 2, "GET", "/api/load/workers/keys/key", nil, 0); code != 503 {
		t.Fatalf("observer served worker data: %d %s", code, data)
	}
	if code, data := c.request(t, 2, "PUT", "/api/load/workers/keys/key", map[string]string{"request_id": "write-1", "value": "first"}, 0); code != 200 {
		t.Fatal(code, string(data))
	}
	if code, data := c.request(t, 2, "PUT", "/api/load/workers/keys/key", map[string]string{"request_id": "write-1", "value": "changed"}, 0); code != 409 {
		t.Fatal("conflicting request identity", code, string(data))
	}
	if code, data := c.request(t, 0, "POST", "/api/nodes/node1/drain", meta.MutationOptions{Apply: true}, 1); code != 409 {
		t.Fatal("CAS guard", code, string(data))
	}
	c.mutate(t, "/api/nodes/node3/drain", "POST", meta.MutationOptions{Actor: "operator", Apply: true})
	doc = c.document(t, 0, "active")
	if len(doc.MetaGroup().Replicas) != 3 || len(doc.Group("demo").Replicas) != 2 || len(doc.Group("workers").Replicas) != 2 {
		t.Fatal("drain changed wrong memberships", doc)
	}
	if code, data := c.request(t, 2, "GET", "/health/ready", nil, 0); code != 503 {
		t.Fatal("drained node still ready", code, string(data))
	}

	c.mutate(t, "/api/nodes/node3/activate", "POST", meta.MutationOptions{Apply: true})
	doc = c.document(t, 0, "active")
	for _, replica := range doc.Group("demo").Replicas {
		if replica.NodeName == "node3" && replica.ReplicaID <= oldDemoID {
			t.Fatal("retired load ID reused", replica)
		}
	}
	c.wait(t, "reactivated replica serving", 5*time.Second, func() bool { code, _ := c.request(t, 2, "GET", "/health/ready", nil, 0); return code == 200 })
	// The fixed policy refuses a two-voter group's shrink below its current
	// quorum. A staged worker drain can be inspected but cannot be approved.
	code, data := c.request(t, 0, "POST", "/api/nodes/node1/drain", meta.MutationOptions{Apply: true}, doc.Version)
	if code != 422 {
		t.Fatal("two-voter shrink should be refused", code, string(data))
	}
	// The ingress may be a follower: its document endpoint is locally applied,
	// while the staging response was acknowledged by the leader.
	c.wait(t, "failed approval's staged edit applied locally", 5*time.Second, func() bool { return c.document(t, 0, "latest").Version == doc.Version+1 })
	staged := c.document(t, 0, "latest")
	body := map[string]any{"placement": topology.PlacementRule{MinReplicas: 2, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}}, "mutation": meta.MutationOptions{Apply: true}}
	if code, data := c.request(t, 0, "PUT", "/api/groups/workers/placement", body, staged.Version); code != 422 {
		t.Fatal("unsatisfiable placement", code, string(data))
	}
	// Restore the still-approved identities through the document API. Failed
	// approval leaves Latest staged; it never changes Target or Active.
	code, data = c.request(t, 0, "PUT", "/api/topology", doc, staged.Version)
	if code != 200 {
		t.Fatal("restore staged document", code, string(data))
	}
	var restored meta.MutationResult
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	code, data = c.request(t, 0, "POST", fmt.Sprintf("/api/topology/%d/apply", restored.Document.Version), nil, 0)
	if code != 202 {
		t.Fatal("apply restored document", code, string(data))
	}
	var apply struct {
		ID topology.ExecutionID `json:"execution_id"`
	}
	if err := json.Unmarshal(data, &apply); err != nil {
		t.Fatal(err)
	}
	c.waitExecution(t, apply.ID)
	c.wait(t, "restored approved document on all nodes", 5*time.Second, func() bool {
		for i := range c.apps {
			if c.document(t, i, "active").Version != restored.Document.Version {
				return false
			}
		}
		return true
	})

	c.wait(t, "rejoined node recovered demo data", 5*time.Second, func() bool {
		code, data := c.request(t, 2, "GET", "/api/load/demo/keys/recover", nil, 0)
		return code == 200 && bytes.Contains(data, []byte(`"value":"value-19"`))
	})
	if code, data := c.request(t, 2, "PUT", "/api/load/demo/keys/recover", map[string]string{"request_id": "demo-0", "value": "value-0"}, 0); code != 200 {
		t.Fatal("recovered request retry", code, string(data))
	}
	if code, data := c.request(t, 2, "GET", "/api/load/demo/keys/recover", nil, 0); code != 200 || !bytes.Contains(data, []byte(`"value":"value-19"`)) {
		t.Fatal("retry overwrote recovered data", code, string(data))
	}
	c.mutate(t, "/api/groups", "POST", map[string]any{"group": meta.GroupSpec{ID: "temporary", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}}}, "mutation": meta.MutationOptions{Apply: true}})
	c.mutate(t, "/api/groups/temporary", "DELETE", meta.MutationOptions{Apply: true})
	doc = c.document(t, 0, "latest")
	if code, data := c.request(t, 0, "POST", "/api/groups", map[string]any{"group": meta.GroupSpec{ID: "temporary"}, "mutation": meta.MutationOptions{Apply: true}}, doc.Version); code != 409 || !bytes.Contains(data, []byte(`"group_retired"`)) {
		t.Fatal("group ID reused", code, string(data))
	}

	streamCtx, stopStream := context.WithCancel(c.ctx)
	defer stopStream()
	streamReq, err := http.NewRequestWithContext(streamCtx, "GET", "http://"+c.peers[0].HTTPAddress+"/api/events/watch?group=demo&type=leader_change", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := c.client.Do(streamReq)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != 200 || stream.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal("SSE response", stream.StatusCode)
	}
	status, err := c.apps[0].Status(c.ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	destination := "node2"
	for _, replica := range doc.Group("demo").Replicas {
		if replica.NodeName == destination && replica.ReplicaID == status.LeaderID {
			destination = "node1"
		}
	}
	if code, data := c.request(t, 0, "POST", "/api/groups/demo/leader/"+destination, nil, 0); code != 200 {
		t.Fatal("transfer", code, string(data))
	}
	scanner := bufio.NewScanner(stream.Body)
	var observed events.Event
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var delivery struct {
			Event events.Event `json:"event"`
			Lost  uint64       `json:"lost_before"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &delivery); err != nil {
			t.Fatal(err)
		}
		if delivery.Event.Type != events.LeaderChange || delivery.Event.GroupID != "demo" || delivery.Event.Seq == 0 || delivery.Lost != 0 {
			t.Fatal("filtered persisted stream event", line)
		}
		observed = delivery.Event
		break
	}
	if observed.Seq == 0 {
		t.Fatal("missing stream event", scanner.Err())
	}
	stopStream()
	_ = stream.Body.Close()
	code, data = c.request(t, 0, "GET", fmt.Sprintf("/api/events?group=demo&type=leader_change&after=%d", observed.Seq-1), nil, 0)
	if code != 200 || !bytes.Contains(data, []byte(fmt.Sprintf(`"seq":%d`, observed.Seq))) {
		t.Fatal("journal did not precede stream delivery", code, string(data))
	}

	for _, path := range []string{"/api/topology/history?limit=5", "/api/events?limit=5", "/api/cluster/status"} {
		if code, data := c.request(t, 0, "GET", path, nil, 0); code != 200 {
			t.Fatal(path, code, string(data))
		}
	}
	c.wait(t, "load leader settled after transfer and planning available", 5*time.Second, func() bool {
		code, _ := c.request(t, 0, "POST", fmt.Sprintf("/api/topology/%d/plan", doc.Version), nil, 0)
		return code == 200
	})

	// Restart a persisted process with a fresh InstanceID; the member ID and
	// exact acknowledged values must survive on this node's retained WAL.
	old := c.apps[0]
	cfg := old.cfg
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = old.Close(closeCtx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	cfg.InstanceID += "-restart"
	cfg.MetaListen = c.peers[0].MetaAddress
	cfg.LoadListen = c.peers[0].LoadAddress
	cfg.HTTPListen = c.peers[0].HTTPAddress
	restarted, err := newApplication(cfg, old.logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.apps[0] = restarted
	if err := restarted.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	c.wait(t, "persisted process readiness", 10*time.Second, func() bool { code, _ := c.request(t, 0, "GET", "/health/ready", nil, 0); return code == 200 })
	c.wait(t, "persisted worker data", 5*time.Second, func() bool {
		code, data := c.request(t, 0, "GET", "/api/load/workers/keys/key", nil, 0)
		return code == 200 && bytes.Contains(data, []byte(`"value":"first"`))
	})
	if restarted.meta.Identity().MemberID != old.meta.Identity().MemberID {
		t.Fatal("restart changed durable meta ID")
	}

}

func TestExampleRESTExcludeAndReadmitRetainedNode(t *testing.T) {
	if testing.Short() {
		t.Skip("real REST admission and physical readmission test")
	}
	c := newRESTCluster(t)
	for i := range 20 {
		code, data := c.request(t, 0, "PUT", "/api/load/demo/keys/readmit", map[string]string{"request_id": fmt.Sprintf("readmit-%d", i), "value": fmt.Sprintf("value-%d", i)}, 0)
		if code != 200 {
			t.Fatal(code, string(data))
		}
	}
	old := c.apps[2]
	oldIdentity := old.meta.Identity()
	// Drain load replicas first; the excluded process shuts down when its actual
	// meta membership is removed, so it cannot receive later cleanup callbacks.
	c.mutate(t, "/api/nodes/node3/drain", "POST", meta.MutationOptions{Apply: true})
	doc := c.document(t, 0, "latest")
	code, data := c.request(t, 0, "POST", "/api/nodes/node3/exclude", meta.MutationOptions{Actor: "operator", Apply: true}, doc.Version)
	if code != 202 {
		t.Fatal("exclude", code, string(data))
	}
	var excluded meta.MutationResult
	if err := json.Unmarshal(data, &excluded); err != nil {
		t.Fatal(err)
	}
	c.waitExecution(t, excluded.ExecutionID)
	c.wait(t, "excluded process joined shutdown", 10*time.Second, func() bool {
		select {
		case <-old.closed:
			return true
		default:
			return false
		}
	})
	c.wait(t, "exclusion on surviving nodes", 5*time.Second, func() bool {
		return c.document(t, 0, "active").Version == excluded.Document.Version && c.document(t, 1, "active").Version == excluded.Document.Version
	})
	oldPin, found, err := meta.LoadStoredIdentity(filepath.Join(old.cfg.DataDir, "meta.bolt"))
	if err != nil || !found || oldPin.MemberID != oldIdentity.MemberID {
		t.Fatal("closed retained pin", oldPin, found, err)
	}
	doc = c.document(t, 0, "latest")
	body := map[string]any{"node": nodeForPeer(c.peers[2]), "mutation": meta.MutationOptions{Actor: "operator", Apply: true}}
	code, data = c.request(t, 0, "POST", "/api/nodes", body, doc.Version)
	if code != 202 {
		t.Fatal("readmit declaration", code, string(data))
	}
	var added meta.MutationResult
	if err := json.Unmarshal(data, &added); err != nil {
		t.Fatal(err)
	}
	cfg := old.cfg
	cfg.Join = true
	cfg.InstanceID += "-readmitted"
	cfg.MetaListen = c.peers[2].MetaAddress
	cfg.LoadListen = c.peers[2].LoadAddress
	cfg.HTTPListen = c.peers[2].HTTPAddress
	replacement, err := newApplication(cfg, old.logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.apps[2] = replacement
	if err := replacement.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	c.waitExecution(t, added.ExecutionID)
	c.wait(t, "readmitted serving with recovered data", 15*time.Second, func() bool {
		code, data := c.request(t, 2, "GET", "/api/load/demo/keys/readmit", nil, 0)
		return code == 200 && bytes.Contains(data, []byte(`"value":"value-19"`))
	})
	if replacement.meta.Identity().MemberID <= oldIdentity.MemberID || replacement.meta.Identity().InstanceID == oldIdentity.InstanceID {
		t.Fatal("readmission reused old identity", replacement.meta.Identity())
	}
	backups, err := filepath.Glob(filepath.Join(cfg.DataDir, "meta.bolt.retired-*"))
	if err != nil || len(backups) != 1 {
		t.Fatal("exact old pin was not quarantined", backups, err)
	}
	retired, found, err := meta.LoadStoredIdentity(backups[0])
	if err != nil || !found || retired != oldPin {
		t.Fatal("wrong physical pin quarantined", retired, oldPin, err)
	}
	// The new pin belongs to the granted member and must not be quarantined.
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = replacement.Close(closeCtx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := meta.LoadStoredIdentity(filepath.Join(cfg.DataDir, "meta.bolt"))
	if err != nil || !found || current.MemberID != replacement.meta.Identity().MemberID {
		t.Fatal("replacement pin", current, found, err)
	}
}
