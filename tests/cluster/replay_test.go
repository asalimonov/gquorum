//go:build clustertest

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// startReplay observes only existing read endpoints. The sampler owns its file
// and joins every request before lease cleanup can reclaim the fixture.
func startReplay(t *testing.T, c *Cluster) func() {
	t.Helper()
	root := os.Getenv("GQUORUM_TEST_REPLAY_DIR")
	if root == "" {
		return func() {}
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal("create replay directory:", err)
	}
	file, err := os.CreateTemp(root, "capture-*.jsonl")
	if err != nil {
		t.Fatal("create replay:", err)
	}
	id := filepath.Base(file.Name())
	started := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var mu sync.Mutex
	var writeErr error
	encoder := json.NewEncoder(file)
	write := func(value map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		if writeErr != nil {
			return
		}
		value["replay"], value["id"] = "gquorum/v1", id
		writeErr = encoder.Encode(value)
	}
	write(map[string]any{"event": "start", "test": t.Name(), "time": started.UTC(),
		"cluster_id": c.ID, "clock": "wall", "interval_ms": 500,
		"scope": "after lease acquisition until test cleanup; metadata leadership and node startup readiness"})
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			var wg sync.WaitGroup
			for _, node := range c.Nodes() {
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					observation := replayObservation(ctx, c, name)
					if ctx.Err() == nil {
						write(observation)
					}
				}(node.Spec.Name)
			}
			wg.Wait()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	t.Logf("Cluster replay: %s", file.Name())
	return func() {
		cancel()
		<-done
		write(map[string]any{"event": "end", "time": time.Now().UTC(),
			"duration": time.Since(started).Seconds()})
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Errorf("save cluster replay: write=%v close=%v", writeErr, closeErr)
		}
	}
}

func replayObservation(ctx context.Context, c *Cluster, name string) map[string]any {
	started := time.Now().UTC()
	readCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	runtime, runtimeErr := c.runtime(readCtx, name)
	cancel()
	readCtx, cancel = context.WithTimeout(ctx, 400*time.Millisecond)
	cluster, clusterErr := c.Status(readCtx, name)
	cancel()
	role, health := "unknown", "unreachable"
	details := map[string]any{"requested_at": started, "received_at": time.Now().UTC()}
	var term uint64
	message := "Runtime status unavailable"
	if runtimeErr == nil {
		details["runtime"] = runtime.Status
		meta := runtime.Status.Meta
		term = meta.Term
		if meta.Found && meta.LeaderID != 0 {
			role = "nonleader"
			if meta.LeaderID == runtime.Status.Identity.MemberID {
				role = "leader"
			}
		}
		health = "not-ready"
		if runtime.Status.Ready {
			health = "startup-ready"
		}
		message = fmt.Sprintf("_meta %s term=%d; state=%s startup_ready=%t session_live=%t",
			role, term, runtime.Status.State, runtime.Status.Ready, runtime.Status.SessionLive)
	} else {
		details["runtime_error"] = runtimeErr.Error()
	}
	if clusterErr == nil {
		details["cluster"] = cluster
		message += fmt.Sprintf("; topology latest=%d target=%d converged=%d",
			cluster.LatestVersion, cluster.TargetVersion, cluster.FullyConverged.Version)
	} else {
		details["cluster_error"] = clusterErr.Error()
	}
	// Timestamp completion, so the player never displays a response before it
	// was available. Original observation timestamps remain in the payload.
	observation := map[string]any{"event": "observation", "time": time.Now().UTC(),
		"machine": name, "msg": message, "kind": "status", "role": role,
		"health": health, "details": details}
	if runtimeErr == nil && runtime.Status.Meta.Found {
		observation["term"] = term
	}
	return observation
}
