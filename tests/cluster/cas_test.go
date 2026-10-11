//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCASHasOneCommitAtEveryClusterSize(t *testing.T) {
	for _, size := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			c := acquire(t, matrixSpec(size)).Cluster
			ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
			defer cancel()
			base, err := c.Document(ctx, "node1", "latest")
			if err != nil {
				t.Fatal(err)
			}
			const contenders = 15
			type attempt struct {
				response Response
				err      error
				value    string
			}
			results := make([]attempt, contenders)
			start := make(chan struct{})
			var workers sync.WaitGroup
			for i := range contenders {
				workers.Go(func() {
					candidate := base.Clone()
					candidate.ParentVersion = base.Version
					value := fmt.Sprintf("candidate-%d", i)
					candidate.Node("node1").Labels["test.cas"] = value
					<-start
					response, err := c.Request(ctx, c.Spec.Nodes[i%size].Name, "PUT", "/api/topology", candidate, base.Version)
					results[i] = attempt{response: response, err: err, value: value}
				})
			}
			close(start)
			workers.Wait()
			var winner string
			commits, conflicts := 0, 0
			for _, result := range results {
				if result.err != nil {
					t.Fatal("CAS request failed before an authoritative result", result.err)
				}
				switch result.response.Status {
				case 200:
					var committed gquorum.MutationResult
					if err := result.response.Decode(&committed); err != nil || committed.Document.Version != base.Version+1 || committed.Document.ParentVersion != base.Version || committed.Document.Node("node1").Labels["test.cas"] != result.value {
						t.Fatal("commit returned another operation's result", committed, err)
					}
					commits++
					winner = result.value
				case 409:
					var refusal struct {
						Code            string `json:"code"`
						ExpectedVersion uint64 `json:"expected_version"`
						CurrentVersion  uint64 `json:"current_version"`
					}
					if err := result.response.Decode(&refusal); err != nil || refusal.Code != "version_conflict" || refusal.ExpectedVersion != base.Version || refusal.CurrentVersion != base.Version+1 {
						t.Fatal("conflict lost exact versions", refusal, err)
					}
					conflicts++
				default:
					t.Fatal("unexpected CAS result", result.response.Status, string(result.response.Data))
				}
			}
			if commits != 1 || conflicts != contenders-1 {
				t.Fatal("CAS did not serialize one winner", commits, conflicts)
			}
			if err := Poll(ctx, func() (bool, error) {
				for _, node := range c.Nodes() {
					latest, err := c.Document(ctx, node.Spec.Name, "latest")
					if err != nil || latest.Version != base.Version+1 || latest.Node("node1").Labels["test.cas"] != winner {
						return false, err
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal("winning commit did not propagate to every meta member", err)
			}
			// A follower can approve the exact winning version after contention.
			if _, err := c.Approve(ctx, c.Spec.Nodes[size-1].Name, base.Version+1); err != nil {
				t.Fatal(err)
			}
			for _, node := range c.Nodes() {
				active, err := c.Document(ctx, node.Spec.Name, "active")
				if err != nil || active.Node("node1").Labels["test.cas"] != winner {
					t.Fatal("approval changed the winner", node.Spec.Name, active, err)
				}
			}
		})
	}
}
