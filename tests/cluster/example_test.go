//go:build example && !clustertest

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

// The demo is tested from the test tree using its actual Compose application.
// A unique project/image, random host ports and private volumes keep this test
// independent of a developer's running make run-example deployment.
func TestComposeExampleTopologyDrainActivate(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	project := fmt.Sprintf("gquorum-demo-test-%d", time.Now().UnixNano())
	override := filepath.Join(t.TempDir(), "compose.yaml")
	var yaml strings.Builder
	yaml.WriteString("services:\n")
	for _, name := range []string{"node1", "node2", "node3"} {
		fmt.Fprintf(&yaml, "  %s:\n    image: %s:local\n    ports: !override [\"127.0.0.1::8080\"]\n", name, project)
	}
	if err := os.WriteFile(override, []byte(yaml.String()), 0600); err != nil {
		t.Fatal(err)
	}
	compose := func(ctx context.Context, args ...string) ([]byte, error) {
		argv := append([]string{"compose", "-p", project, "-f", filepath.Join(root, "examples/cluster/compose.yaml"), "-f", override}, args...)
		cmd := exec.CommandContext(ctx, "docker", argv...)
		cmd.Dir = root
		return cmd.CombinedOutput()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if t.Failed() {
			if output, err := compose(ctx, "logs", "--tail", "80"); err == nil {
				t.Log(string(output))
			}
		}
		if output, err := compose(ctx, "down", "--volumes", "--rmi", "all", "--remove-orphans"); err != nil {
			t.Errorf("release demo: %v\n%s", err, output)
		}
	})
	boot, cancelBoot := context.WithTimeout(t.Context(), 150*time.Second)
	output, err := compose(boot, "up", "--build", "-d", "--wait", "--wait-timeout", "60")
	cancelBoot()
	if err != nil {
		t.Fatalf("start demo: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	endpoints := map[string]string{}
	discoverEndpoints := func() {
		t.Helper()
		for _, name := range []string{"node1", "node2", "node3"} {
			port, err := compose(ctx, "port", name, "8080")
			if err != nil {
				t.Fatal(err, string(port))
			}
			endpoints[name] = "http://" + strings.TrimSpace(string(port))
		}
	}
	discoverEndpoints()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	request := func(node, method, path string, body any, version uint64, want int, target any) int {
		t.Helper()
		var encoded []byte
		if body != nil {
			var err error
			encoded, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoints[node]+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if version > 0 {
			req.Header.Set("If-Match", fmt.Sprint(version))
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if want != 0 && res.StatusCode != want {
			t.Fatalf("%s %s: HTTP %d, wanted %d: %s", method, path, res.StatusCode, want, data)
		}
		if target != nil && res.StatusCode < 300 {
			if err := json.Unmarshal(data, target); err != nil {
				t.Fatal(err)
			}
		}
		if method == "GET" && strings.HasPrefix(path, "/api/topology/") && res.StatusCode == 200 {
			var document topology.Document
			if err := json.Unmarshal(data, &document); err != nil || res.Header.Get("ETag") != fmt.Sprintf(`"%d"`, document.Version) {
				t.Fatalf("document ETag: %s, header %v, error %v", data, res.Header, err)
			}
		}
		return res.StatusCode
	}
	var initial topology.Document
	request("node1", "GET", "/api/topology/active", nil, 0, 200, &initial)
	if len(initial.Nodes) != 3 || len(initial.LoadGroups()) != 0 || initial.Node("node3").Type != "observer" {
		t.Fatalf("unexpected demo: %+v", initial)
	}
	for _, n := range []string{"node1", "node2", "node3"} {
		request(n, "GET", "/health/ready", nil, 0, 200, nil)
		request(n, "GET", "/internal/test/controls", nil, 0, 404, nil)
		var status topology.ClusterStatus
		request(n, "GET", "/api/cluster/status", nil, 0, 200, &status)
		if status.MetaLeaderNodeName == "" || initial.MemberID(status.MetaLeaderNodeName) != status.MetaLeaderID || len(status.Groups) != 1 || status.Groups[0].LeaderNodeName != status.MetaLeaderNodeName || status.Groups[0].LeaderID != status.MetaLeaderID {
			t.Fatalf("leader name and Raft ID disagree: %+v", status)
		}
	}
	request("node1", "POST", "/api/topology/1/apply", nil, 0, 404, nil)
	request("node1", "GET", "/api/executions/old", nil, 0, 404, nil)
	request("node1", "POST", "/api/nodes/node3/drain", map[string]any{"apply": true}, initial.Version, 400, nil)
	request("node1", "PUT", "/api/nodes/node3/drain", nil, 0, 405, nil)
	var preview gquorum.MutationResult
	request("node1", "POST", "/api/nodes/node3/drain", map[string]any{"preview": true}, initial.Version, 200, &preview)
	var afterPreview topology.Document
	request("node1", "GET", "/api/topology/latest", nil, 0, 200, &afterPreview)
	if afterPreview.Version != initial.Version || preview.OperatorID != "" || topology.EffectiveNodeState(preview.Document, "node3") != topology.NodeDrained {
		t.Fatalf("preview changed cluster state or failed to preview drain: %+v, %+v", afterPreview, preview)
	}
	for _, action := range []string{"drain", "activate"} {
		var latest topology.Document
		request("node2", "GET", "/api/topology/latest", nil, 0, 200, &latest)
		var mutation gquorum.MutationResult
		if action == "drain" {
			request("node2", "POST", "/api/nodes/node3/drain", map[string]any{"actor": "demo-test", "approve": true}, latest.Version, 202, &mutation)
		} else {
			request("node2", "POST", "/api/nodes/node3/activate", map[string]any{"actor": "demo-test", "approve": false}, latest.Version, 200, &mutation)
			if mutation.OperatorID != "" {
				t.Fatal("staged edit unexpectedly approved", mutation)
			}
			path := fmt.Sprintf("/api/topology/%d/approve", mutation.Document.Version)
			request("node2", "POST", path+"?dry_run=true", nil, 0, 400, nil)
			var target topology.Document
			request("node2", "GET", "/api/topology/target", nil, 0, 200, &target)
			if target.Version != latest.Version {
				t.Fatalf("staging or refused approval advanced Target: %+v", target)
			}
			var approved struct {
				ID topology.OperatorID `json:"operator_id"`
			}
			request("node2", "POST", path, nil, mutation.Document.Version, 202, &approved)
			mutation.OperatorID = approved.ID
		}
		if mutation.OperatorID == "" {
			t.Fatal("approval did not return operator_id")
		}
		for {
			var operator topology.OperatorStatus
			status := request("node1", "GET", "/api/operators/"+string(mutation.OperatorID), nil, 0, 0, &operator)
			if status != 200 && status != 404 {
				t.Fatalf("operator observation returned HTTP %d", status)
			}
			if operator.State == topology.OperatorSucceeded {
				break
			}
			if operator.State.Terminal() {
				t.Fatalf("operator failed: %+v", operator)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		for _, n := range []string{"node1", "node2", "node3"} {
			for {
				var active topology.Document
				request(n, "GET", "/api/topology/active", nil, 0, 200, &active)
				if active.Version == mutation.Document.Version {
					want := topology.NodeActive
					if action == "drain" {
						want = topology.NodeDrained
					}
					if topology.EffectiveNodeState(active, "node3") != want || len(active.Group(topology.MetaGroupID).Replicas) != 3 {
						t.Fatalf("incorrect node state or meta membership: %+v", active)
					}
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		status := 200
		if action == "drain" {
			status = 503
		}
		request("node3", "GET", "/health/ready", nil, 0, status, nil)
		request("node1", "POST", "/api/nodes/node3/drain", map[string]any{"approve": true}, latest.Version, 409, nil)
	}
	var beforeRestart topology.Document
	request("node1", "GET", "/api/topology/active", nil, 0, 200, &beforeRestart)
	if output, err := compose(ctx, "restart"); err != nil {
		t.Fatalf("restart retained demo: %v\n%s", err, output)
	}
	if output, err := compose(ctx, "up", "-d", "--wait", "--wait-timeout", "60"); err != nil {
		t.Fatalf("retained demo readiness: %v\n%s", err, output)
	}
	// Docker may assign new ephemeral host ports when containers restart.
	discoverEndpoints()
	client.CloseIdleConnections()
	for _, name := range []string{"node1", "node2", "node3"} {
		var retained topology.Document
		request(name, "GET", "/api/topology/active", nil, 0, 200, &retained)
		if !reflect.DeepEqual(retained, beforeRestart) {
			t.Fatalf("retained topology changed on %s: before %+v, after %+v", name, beforeRestart, retained)
		}
	}
}
