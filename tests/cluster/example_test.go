//go:build example && !cluster

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
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/meta"
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
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	endpoints := map[string]string{}
	for _, name := range []string{"node1", "node2", "node3"} {
		port, err := compose(ctx, "port", name, "8080")
		if err != nil {
			t.Fatal(err, string(port))
		}
		endpoints[name] = "http://" + strings.TrimSpace(string(port))
	}
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
	}
	for _, action := range []string{"drain", "activate"} {
		var latest topology.Document
		request("node2", "GET", "/api/topology/latest", nil, 0, 200, &latest)
		var mutation meta.MutationResult
		request("node2", "POST", "/api/nodes/node3/"+action, map[string]any{"actor": "demo-test", "apply": true}, latest.Version, 202, &mutation)
		for {
			var execution topology.ExecutionStatus
			status := request("node1", "GET", "/api/executions/"+string(mutation.ExecutionID), nil, 0, 0, &execution)
			if status != 200 && status != 404 {
				t.Fatalf("execution observation returned HTTP %d", status)
			}
			if execution.State == topology.ExecutionSucceeded {
				break
			}
			if execution.State.Terminal() {
				t.Fatalf("execution failed: %+v", execution)
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
		request("node1", "POST", "/api/nodes/node3/drain", map[string]any{"apply": true}, latest.Version, 409, nil)
	}
}
