//go:build example && !clustertest

package cluster

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Exercise the actual cache application with private volumes and ports. A
// process restart preserves the OS page cache; this does not model power loss.
func TestCacheExampleWALRestart(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	project := fmt.Sprintf("gq-cache-test-%d", time.Now().UnixNano())
	override := filepath.Join(t.TempDir(), "compose.yaml")
	nodes := []string{"node1", "node2", "node3"}
	var yaml strings.Builder
	yaml.WriteString("services:\n")
	for _, name := range nodes {
		fmt.Fprintf(&yaml, "  %s:\n    image: %s:local\n    ports: !override [\"127.0.0.1::6379\"]\n    environment:\n      STORAGE_DISABLE_FSYNC: \"true\"\n", name, project)
	}
	fmt.Fprintf(&yaml, "  loadgen:\n    image: %s:local\n", project)
	if err := os.WriteFile(override, []byte(yaml.String()), 0600); err != nil {
		t.Fatal(err)
	}
	compose := func(ctx context.Context, args ...string) ([]byte, error) {
		argv := append([]string{"compose", "-p", project, "-f", filepath.Join(root, "examples/inmemory-cache/compose.yaml"), "-f", override}, args...)
		cmd := exec.CommandContext(ctx, "docker", argv...)
		cmd.Dir = root
		return cmd.CombinedOutput()
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if t.Failed() {
			if out, err := compose(cleanup, "logs", "--tail", "100"); err == nil {
				t.Log(string(out))
			}
		}
		if out, err := compose(cleanup, "down", "--volumes", "--rmi", "local", "--remove-orphans"); err != nil {
			t.Errorf("cleanup cache: %v\n%s", err, out)
		}
		// A custom image tag is retained by --rmi local; this test owns it.
		if out, err := exec.CommandContext(cleanup, "docker", "image", "rm", project+":local").CombinedOutput(); err != nil && !strings.Contains(string(out), "No such image") {
			t.Errorf("remove cache image: %v\n%s", err, out)
		}
	})
	control := func(args ...string) {
		t.Helper()
		if out, err := compose(ctx, args...); err != nil {
			t.Fatalf("compose %v: %v\n%s", args, err, out)
		}
	}
	// Do not start loadgen: eviction would obscure the retained-data assertion.
	control("up", "--build", "-d", "--wait", "--wait-timeout", "90", "node1", "node2", "node3")
	endpoints := map[string]string{}
	discover := func() {
		t.Helper()
		for _, name := range nodes {
			out, err := compose(ctx, "port", name, "6379")
			if err != nil {
				t.Fatal(err, string(out))
			}
			endpoints[name] = strings.TrimSpace(string(out))
		}
	}
	discover()
	command := func(node string, args ...string) string {
		t.Helper()
		var result string
		deadline, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		for {
			var err error
			result, err = cacheCommand(deadline, endpoints[node], args...)
			if err == nil {
				return result
			}
			select {
			case <-deadline.Done():
				t.Fatalf("%s %v: %v", node, args, err)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	replicaID := func(node string) string {
		t.Helper()
		for line := range strings.SplitSeq(command(node, "INFO"), "\r\n") {
			if id, ok := strings.CutPrefix(line, "replica_id:"); ok && id != "0" {
				return id
			}
		}
		t.Fatal("missing replica identity", node)
		return ""
	}
	ids := map[string]string{}
	for _, node := range nodes {
		ids[node] = replicaID(node)
	}
	if got := command("node1", "SET", "restart-key", "retained"); got != "OK" {
		t.Fatal(got)
	}
	verify := func() {
		t.Helper()
		for _, node := range nodes {
			if got := command(node, "GET", "restart-key"); got != "retained" {
				t.Fatalf("%s lost acknowledged value: %q", node, got)
			}
			if got := replicaID(node); got != ids[node] {
				t.Fatalf("%s reseated instead of recovering: %s -> %s", node, ids[node], got)
			}
		}
	}
	verify() // all replicas have applied the value before stopping any process
	control("kill", "-s", "SIGKILL", "node3")
	// Keep the key set tiny so this failover workload cannot evict the
	// retained-data assertion below. Node3 must be excluded by health polling.
	out, err := compose(ctx, "run", "--rm", "--no-deps", "-e", "CACHE_WORKERS=1",
		"-e", "CACHE_KEYS=1", "-e", "CACHE_DURATION=5s", "loadgen")
	if err != nil || !strings.Contains(string(out), "healthy=node1:6379,node2:6379 ") ||
		!strings.Contains(string(out), "stale=0 corrupt=0") {
		t.Fatalf("loader did not exclude unavailable node3: %v\n%s", err, out)
	}
	control("start", "node3")
	discover()
	verify()
	control("kill", "-s", "SIGKILL", "node1", "node2", "node3")
	control("start", "node1", "node2", "node3")
	discover()
	verify()
	if got := command("node3", "SET", "after-restart", "ok"); got != "OK" {
		t.Fatal(got)
	}
	if got := command("node1", "GET", "after-restart"); got != "ok" {
		t.Fatal("restarted cluster did not commit new writes", got)
	}
	// Only run load after retention assertions; the workload forces eviction.
	for _, workload := range []struct {
		workers, duration string
	}{{"1", "5s"}, {"10", "75s"}, {"256", "5s"}} {
		out, err := compose(ctx, "run", "--rm", "--no-deps", "-e", "CACHE_WORKERS="+workload.workers,
			"-e", "CACHE_DURATION="+workload.duration, "loadgen")
		if err != nil {
			t.Fatalf("balanced load with %s workers: %v\n%s", workload.workers, err, out)
		}
		var previousWrites int64
		final := false
		for line := range strings.SplitSeq(string(out), "\n") {
			if !strings.HasPrefix(line, "progress ") && !strings.HasPrefix(line, "final ") {
				continue
			}
			fields := map[string]string{}
			for field := range strings.FieldsSeq(line) {
				if key, value, ok := strings.Cut(field, "="); ok {
					fields[key] = value
				}
			}
			for _, key := range []string{"stale", "corrupt", "write_errors", "read_errors"} {
				if fields[key] != "0" {
					t.Fatalf("%s workers: %s is not zero: %s", workload.workers, key, line)
				}
			}
			writes, err := strconv.ParseInt(fields["total_writes"], 10, 64)
			if err != nil || writes == 0 || (strings.HasPrefix(line, "progress ") && writes <= previousWrites) {
				t.Fatalf("%s workers stopped making progress: %s", workload.workers, line)
			}
			previousWrites = writes
			if !strings.HasPrefix(line, "final ") && fields["healthy"] != "node1:6379,node2:6379,node3:6379" {
				t.Fatalf("loader did not discover all healthy nodes: %s", line)
			}
			dials, err := strconv.Atoi(fields["dials"])
			workers, _ := strconv.Atoi(workload.workers)
			limit := len(nodes) * max(1, (workers+len(nodes)-1)/len(nodes))
			// Shutdown cancels borrowed connections; the final DBSIZE may
			// need one replacement per node after all workers have joined.
			if strings.HasPrefix(line, "final ") {
				limit += len(nodes)
			}
			if err != nil || dials > limit {
				t.Fatalf("%s workers failed to reuse connections: %s", workload.workers, line)
			}
			if strings.HasPrefix(line, "final ") {
				final = true
			}
		}
		if !final || (workload.workers == "10" && previousWrites < 20000) {
			t.Fatalf("workload did not exercise sustained connection reuse:\n%s", out)
		}
		t.Logf("balanced load with %s workers:\n%s", workload.workers, out)
	}

}

// Only status and bulk replies are needed by this process test.
func cacheCommand(ctx context.Context, addr string, args ...string) (string, error) {
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return "", err
	}
	var request strings.Builder
	fmt.Fprintf(&request, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&request, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := io.WriteString(conn, request.String()); err != nil {
		return "", err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if strings.HasPrefix(line, "+") {
		return line[1:], nil
	}
	if strings.HasPrefix(line, "$") {
		n, err := strconv.Atoi(line[1:])
		if err != nil || n < 0 || n > 1<<20 {
			return "", fmt.Errorf("invalid or absent bulk reply: %q", line)
		}
		data := make([]byte, n+2)
		_, err = io.ReadFull(r, data)
		return string(data[:n]), err
	}
	return "", fmt.Errorf("cache reply: %s", line)
}
