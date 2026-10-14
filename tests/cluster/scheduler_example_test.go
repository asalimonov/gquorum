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
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
)

type schedulerKey struct {
	TaskID  int64 `json:"task_id"`
	Attempt int64 `json:"attempt"`
}
type schedulerTask struct {
	TaskID   int64  `json:"task_id"`
	State    string `json:"state"`
	Attempt  int64  `json:"attempt"`
	Executor string `json:"executor_node"`
	Result   string `json:"result"`
}
type schedulerStatus struct {
	Node        gquorum.NodeStatus `json:"node"`
	Application struct {
		Observation load.Observation `json:"observation"`
		Executor    struct {
			Active *schedulerKey  `json:"active"`
			Phase  string         `json:"phase"`
			Queued []schedulerKey `json:"queued"`
		} `json:"executor"`
		Replicas []load.ReplicaProgress `json:"replicas"`
	} `json:"application"`
}
type schedulerFixture struct {
	t                       *testing.T
	ctx                     context.Context
	root, project, override string
	urls                    map[string]string
	client                  *http.Client
}

func (f *schedulerFixture) compose(ctx context.Context, args ...string) ([]byte, error) {
	argv := append([]string{"compose", "-p", f.project, "-f", filepath.Join(f.root, "examples/scheduler/compose.yaml"), "-f", f.override}, args...)
	cmd := exec.CommandContext(ctx, "docker", argv...)
	cmd.Dir = f.root
	return cmd.CombinedOutput()
}
func (f *schedulerFixture) control(args ...string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 35*time.Second)
	defer cancel()
	if output, err := f.compose(ctx, args...); err != nil {
		f.t.Fatalf("compose %v: %v\n%s", args, err, output)
	}
	// Docker allocates new ephemeral host ports when a stopped container starts.
	if len(args) > 0 && args[0] == "start" {
		for _, node := range args[1:] {
			data, err := f.compose(ctx, "port", node, "8080")
			if err != nil {
				f.t.Fatal(err, string(data))
			}
			f.urls[node] = "http://" + strings.TrimSpace(string(data))
		}
	}
}

// isolate keeps the process alive while removing all of its peer connectivity.
// Reconnection is registered for cleanup before the test observes any outcomes.
func (f *schedulerFixture) isolate(node string) func() {
	f.t.Helper()
	data, err := f.compose(f.ctx, "ps", "-q", node)
	if err != nil {
		f.t.Fatal(err, string(data))
	}
	id, network := strings.TrimSpace(string(data)), f.project+"_scheduler"
	networkCommand := func(args ...string) {
		f.t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", append([]string{"network"}, args...)...).CombinedOutput()
		if err != nil {
			f.t.Fatalf("network %v: %v\n%s", args, err, output)
		}
	}
	networkCommand("disconnect", network, id)
	connected := false
	reconnect := func() {
		if !connected {
			networkCommand("connect", "--alias", node, network, id)
			connected = true
		}
	}
	f.t.Cleanup(reconnect)
	return reconnect
}
func newSchedulerFixture(t *testing.T) *schedulerFixture {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	f := &schedulerFixture{t: t, ctx: ctx, root: root, project: fmt.Sprintf("gq-scheduler-test-%d", time.Now().UnixNano()), override: filepath.Join(t.TempDir(), "compose.yaml"), urls: map[string]string{}, client: &http.Client{Timeout: 5 * time.Second}}
	image := os.Getenv("SCHEDULER_TEST_IMAGE")
	owned := image == ""
	if owned {
		image = f.project + ":local"
	}
	var override strings.Builder
	override.WriteString("services:\n")
	for _, name := range []string{"node1", "node2", "node3"} {
		fmt.Fprintf(&override, "  %s:\n    image: %s\n    ports: !override [\"127.0.0.1::8080\"]\n", name, image)
	}
	if err := os.WriteFile(f.override, []byte(override.String()), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		f.client.CloseIdleConnections()
		if t.Failed() {
			if data, err := f.compose(cleanup, "logs", "--tail", "120"); err == nil {
				t.Log(string(data))
			}
		}
		args := []string{"down", "--volumes", "--remove-orphans"}
		if owned {
			args = append(args, "--rmi", "all")
		}
		if data, err := f.compose(cleanup, args...); err != nil {
			t.Errorf("cleanup scheduler: %v\n%s", err, data)
		}
	})
	boot, cancelBoot := context.WithTimeout(ctx, 150*time.Second)
	defer cancelBoot()
	build := "--no-build"
	if owned {
		build = "--build"
	}
	data, err := f.compose(boot, "up", build, "-d", "--wait", "--wait-timeout", "90")
	if err != nil {
		t.Fatalf("boot scheduler: %v\n%s", err, data)
	}
	for _, name := range []string{"node1", "node2", "node3"} {
		data, err := f.compose(ctx, "port", name, "8080")
		if err != nil {
			t.Fatal(err, string(data))
		}
		f.urls[name] = "http://" + strings.TrimSpace(string(data))
	}
	return f
}
func (f *schedulerFixture) request(node, method, path string, body any, out any) (int, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequestWithContext(f.ctx, method, f.urls[node]+path, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer scheduler-local-demo-secret")
	req.Header.Set("Content-Type", "application/json")
	reply, err := f.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer reply.Body.Close()
	data, err = io.ReadAll(io.LimitReader(reply.Body, 1<<20))
	if err != nil {
		return reply.StatusCode, err
	}
	if reply.StatusCode >= 300 {
		return reply.StatusCode, fmt.Errorf("HTTP %d: %s", reply.StatusCode, data)
	}
	if out != nil {
		err = json.Unmarshal(data, out)
	}
	return reply.StatusCode, err
}
func (f *schedulerFixture) wait(label string, predicate func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal(label, f.ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	f.t.Fatal("timed out:", label)
}
func (f *schedulerFixture) status(node string) (schedulerStatus, bool) {
	var s schedulerStatus
	_, err := f.request(node, "GET", "/status", nil, &s)
	return s, err == nil && s.Application.Observation.Ready
}
func (f *schedulerFixture) leader(nodes ...string) string {
	for _, node := range nodes {
		if s, ok := f.status(node); ok && s.Application.Observation.IsLeader {
			return node
		}
	}
	return ""
}
func (f *schedulerFixture) task(node string, id int64) (schedulerTask, bool) {
	var task schedulerTask
	_, err := f.request(node, "GET", fmt.Sprintf("/tasks/%d", id), nil, &task)
	return task, err == nil
}
func (f *schedulerFixture) tasks(node string) ([]schedulerTask, bool) {
	var all []schedulerTask
	var after int64
	for range 11 {
		var page struct {
			Tasks []schedulerTask `json:"tasks"`
			Next  int64           `json:"next"`
		}
		_, err := f.request(node, "GET", fmt.Sprintf("/tasks?after=%d&limit=100", after), nil, &page)
		if err != nil {
			return nil, false
		}
		all = append(all, page.Tasks...)
		if page.Next == 0 {
			return all, true
		}
		after = page.Next
	}
	return nil, false
}
func (f *schedulerFixture) busyFollower(nodes ...string) (string, schedulerStatus) {
	var name string
	var selected schedulerStatus
	f.wait("follower with running and queued tasks", func() bool {
		for _, node := range nodes {
			if s, ok := f.status(node); ok && !s.Application.Observation.IsLeader && s.Application.Executor.Active != nil && s.Application.Executor.Phase == "calculating" && len(s.Application.Executor.Queued) > 0 {
				name, selected = node, s
				return true
			}
		}
		return false
	})
	return name, selected
}
func otherSchedulerNodes(except string) []string {
	var names []string
	for _, name := range []string{"node1", "node2", "node3"} {
		if name != except {
			names = append(names, name)
		}
	}
	return names
}

func TestSchedulerExampleLifecycle(t *testing.T) {
	t.Parallel()
	f := newSchedulerFixture(t)
	t.Log("verify quorum-backed task execution and bounded capacity")
	var baseline schedulerTask
	f.wait("initial completed task", func() bool {
		tasks, ok := f.tasks("node1")
		if !ok {
			return false
		}
		unfinished := 0
		counts := map[string]int{}
		for _, task := range tasks {
			if task.State == "succeeded" {
				baseline = task
			} else if task.State != "failed" {
				unfinished++
				if task.Executor != "" {
					counts[task.Executor]++
				}
			}
		}
		if unfinished > 10 {
			t.Fatal("unbounded unfinished tasks", unfinished)
		}
		for name, n := range counts {
			if n > 5 {
				t.Fatal("executor capacity exceeded", name, n)
			}
		}
		return baseline.TaskID != 0
	})
	for _, node := range []string{"node1", "node2", "node3"} {
		f.wait("same committed result on "+node, func() bool { got, ok := f.task(node, baseline.TaskID); return ok && got == baseline })
	}

	f.wait("leader reports named replica progress", func() bool {
		s, ok := f.status(f.leader("node1", "node2", "node3"))
		if !ok || len(s.Application.Replicas) != 3 {
			return false
		}
		for _, r := range s.Application.Replicas {
			if r.NodeName == "" || !r.Voter {
				return false
			}
		}
		return true
	})

	t.Log("transfer leadership with active and queued execution")
	target, before := f.busyFollower("node1", "node2", "node3")
	active := *before.Application.Executor.Active
	queued := before.Application.Executor.Queued[0]
	leader := f.leader("node1", "node2", "node3")
	if leader == "" {
		t.Fatal("missing leader")
	}
	// TransferLeader may refuse transiently (unavailable) while replication
	// is in flight; the example maps such refusals to 503, so retry them.
	f.wait("transfer accepted", func() bool {
		code, err := f.request(leader, "POST", "/transfer", map[string]string{"node": target}, nil)
		if err == nil && code == http.StatusServiceUnavailable {
			return false
		}
		if err != nil || code != http.StatusAccepted {
			t.Fatal("transfer", code, err)
		}
		return true
	})
	f.wait("requested load leader", func() bool { return f.leader("node1", "node2", "node3") == target })
	f.wait("promoted executor drains its task", func() bool {
		task, ok := f.task(target, active.TaskID)
		if ok && task.Attempt != active.Attempt {
			t.Fatal("running task reassigned on promotion", task)
		}
		return ok && task.State == "succeeded" && task.Executor == target
	})
	f.wait("promoted executor queue reassigned", func() bool {
		task, ok := f.task(target, queued.TaskID)
		return ok && task.Attempt > queued.Attempt && task.Executor != "" && task.Executor != target
	})

	t.Log("crash an executor, recover its queued work, retain identity on restart")
	victim, before := f.busyFollower("node1", "node2", "node3")
	queued = before.Application.Executor.Queued[0]
	survivors := otherSchedulerNodes(victim)
	f.control("kill", "-s", "SIGKILL", victim)
	f.wait("failed executor task reassigned and completed", func() bool {
		task, ok := f.task(survivors[0], queued.TaskID)
		return ok && task.Attempt > queued.Attempt && task.State == "succeeded" && task.Executor != victim
	})
	f.control("start", victim)
	f.wait("retained executor restart", func() bool {
		s, ok := f.status(victim)
		return ok && s.Application.Observation.InstanceID != before.Application.Observation.InstanceID && s.Application.Observation.Identity == before.Application.Observation.Identity
	})

	t.Log("snapshot and crash the scheduler leader")
	leader = f.leader("node1", "node2", "node3")
	if leader == "" {
		t.Fatal("missing leader")
	}
	if _, err := f.request(leader, "POST", "/snapshot", nil, nil); err != nil {
		t.Fatal(err)
	}
	prior, _ := f.tasks(leader)
	var lastID int64
	for _, task := range prior {
		lastID = max(lastID, task.TaskID)
	}
	f.control("kill", "-s", "SIGKILL", leader)
	survivors = otherSchedulerNodes(leader)
	f.wait("new elected scheduler emits and completes", func() bool {
		next := f.leader(survivors...)
		if next == "" {
			return false
		}
		tasks, ok := f.tasks(next)
		if !ok {
			return false
		}
		for _, task := range tasks {
			if task.TaskID > lastID && task.State == "succeeded" {
				return true
			}
		}
		return false
	})
	f.control("start", leader)
	f.wait("former leader recovers snapshot and tail", func() bool {
		tasks, ok := f.tasks(leader)
		if !ok {
			return false
		}
		for _, task := range tasks {
			if task.TaskID > lastID && task.State == "succeeded" {
				return true
			}
		}
		return false
	})

	t.Log("lose quorum, refuse consensus reads, and resume on restoration")
	leader = f.leader("node1", "node2", "node3")
	if leader == "" {
		t.Fatal("missing leader")
	}
	survivors = otherSchedulerNodes(leader)
	f.control("kill", "-s", "SIGKILL", survivors[0], survivors[1])
	f.wait("quorum read refusal", func() bool {
		code, err := f.request(leader, "GET", "/tasks?limit=1", nil, nil)
		return err != nil && code == http.StatusServiceUnavailable
	})
	f.control("start", survivors[0], survivors[1])
	f.wait("restored quorum serves tasks", func() bool { _, ok := f.tasks(leader); return ok })
	for _, node := range []string{"node1", "node2", "node3"} {
		f.wait("final health "+node, func() bool {
			code, err := f.request(node, "GET", "/healthz", nil, nil)
			return err == nil && code == http.StatusOK
		})
	}

	t.Log("partition a live executor; preserve the replacement result on return")
	victim, before = f.busyFollower("node1", "node2", "node3")
	queued = before.Application.Executor.Queued[0]
	survivors = otherSchedulerNodes(victim)
	reconnect := f.isolate(victim)
	var accepted schedulerTask
	f.wait("partitioned executor work reassigned", func() bool {
		current, ok := f.task(survivors[0], queued.TaskID)
		if ok && current.Attempt > queued.Attempt && current.State == "succeeded" && current.Executor != victim {
			accepted = current
			return true
		}
		return false
	})
	reconnect()
	f.wait("returning executor sees only the accepted attempt", func() bool {
		current, ok := f.task(victim, queued.TaskID)
		return ok && current == accepted
	})
}
