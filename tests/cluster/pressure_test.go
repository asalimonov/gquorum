//go:build cluster

package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/asalimonov/gquorum"
	"maps"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestPendingPressureBoundAndIndependentApplyThreeNodes(t *testing.T) { pendingPressure(t, 3) }
func TestPendingPressureBoundAndIndependentApplyFiveNodes(t *testing.T)  { pendingPressure(t, 5) }
func TestPendingPressureBoundAndIndependentApplySevenNodes(t *testing.T) { pendingPressure(t, 7) }

type pressureReply struct {
	duration time.Duration
	response Response
	err      error
}

func nativeProposal(ctx context.Context, c *Cluster, replica topology.Replica, group, id string) pressureReply {
	started := time.Now()
	r, err := c.Request(ctx, replica.NodeName, "POST", fmt.Sprintf("/internal/test/propose/%s/%d", group, replica.ReplicaID), map[string]string{"request_id": id, "key": id, "value": id}, 0)
	return pressureReply{time.Since(started), r, err}
}

// The source's application reservation set is replaced by bounded engine
// waiters: 4,000 excess attempts against the observed default 256-slot limit.
// A single-voter load group makes the persistence fault deterministic without
// coupling waiter cleanup to leadership replacement. Meta and the independent
// three-voter group remain distributed in every cluster-size fixture.
func pendingPressure(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.SnapshotThreshold = 100000
	ctx, c := performanceClusterSpec(t, spec)
	c.http.Timeout = 65 * time.Second
	transport := c.http.Transport.(*http.Transport)
	transport.MaxIdleConnsPerHost, transport.MaxIdleConns = 512, 1024
	work, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "held", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "healthy", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	r := work.Document.Group("held").Replicas[0]
	if err := c.Put(ctx, r.NodeName, "held", "warm", "warm", "warm"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, healthy.Document, "held", map[string]string{"warm": "warm"}); err != nil {
		t.Fatal(err)
	}
	expectedHealthy := map[string]string{}
	baseline := pressureHealthyWorkload(ctx, c, size, "baseline", 10*time.Second)
	for _, w := range baseline {
		for id := range w.values {
			expectedHealthy[id] = id
		}
		if w.err != nil {
			t.Fatal(w.err)
		}
	}
	before, err := c.Local(ctx, r.NodeName, "held")
	if err != nil {
		t.Fatal(err)
	}
	limit := before.Proposals.Limit
	if limit != 256 {
		t.Fatalf("unexpected constructor default: %+v", before.Proposals)
	}
	holderCtx, cancelHolders := context.WithCancel(ctx)
	var holders sync.WaitGroup
	t.Cleanup(func() { cancelHolders(); holders.Wait() })
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.WALFault(cleanup, r.NodeName, "held", r.ReplicaID, false, 0, true); err != nil {
			t.Error("release held writer", err)
		}
	})
	if err := c.WALFault(ctx, r.NodeName, "held", r.ReplicaID, true, 0, false); err != nil {
		t.Fatal(err)
	}
	heldReplies := make([]pressureReply, limit)
	// Acquire every HTTP connection before releasing the initial wave. The
	// engine intentionally sheds new work after one second of WAL stall; slow
	// client connection setup must not consume that admission window before
	// this test has filled the bounded waiter set.
	connected := make(chan struct{})
	var connections atomic.Int32
	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConnect()
	for i := range limit {
		holders.Go(func() {
			var arrival sync.Once
			trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) {
				arrival.Do(func() {
					if connections.Add(1) == int32(limit) {
						close(connected)
					}
				})
				select {
				case <-connected:
				case <-connectCtx.Done():
				}
			}}
			heldReplies[i] = nativeProposal(httptrace.WithClientTrace(holderCtx, trace), c, r, "held", fmt.Sprintf("holder-%d", i))
		})
	}
	select {
	case <-connected:
	case <-connectCtx.Done():
		t.Fatalf("could not prepare holder connections: %d/%d: %v", connections.Load(), limit, connectCtx.Err())
	}
	var peak int
	if err := Poll(ctx, func() (bool, error) {
		v, err := c.Local(ctx, r.NodeName, "held")
		peak = max(peak, v.Proposals.AwaitingCommit)
		return v.WAL.Active > 0 && v.Proposals.AwaitingCommit >= limit*8/10 && v.WALStallNS >= int64(time.Second), err
	}); err != nil {
		t.Fatalf("did not observe bounded pending plateau: peak=%d: %v", peak, err)
	}
	const excess = 4000
	refusals := make([]pressureReply, excess)
	var burst sync.WaitGroup
	t.Cleanup(func() { cancelHolders(); burst.Wait() })
	for worker := range 40 {
		burst.Go(func() {
			for i := worker; i < excess; i += 40 {
				refusals[i] = nativeProposal(holderCtx, c, r, "held", fmt.Sprintf("shed-%d", i))
			}
		})
	}
	var measured []pressureWorkload
	var paced [][]pressureReply
	var measure sync.WaitGroup
	t.Cleanup(func() { cancelHolders(); measure.Wait() })
	measure.Go(func() { measured = pressureHealthyWorkload(holderCtx, c, size, "pressure", 30*time.Second) })
	measure.Go(func() {
		paced = make([][]pressureReply, 8)
		var wg sync.WaitGroup
		deadline := time.Now().Add(30 * time.Second)
		for worker := range paced {
			wg.Go(func() {
				for i := 0; time.Now().Before(deadline) && holderCtx.Err() == nil; i++ {
					paced[worker] = append(paced[worker], nativeProposal(holderCtx, c, r, "held", fmt.Sprintf("paced-%d-%d", worker, i)))
					select {
					case <-holderCtx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			})
		}
		wg.Wait()
	})
	started := time.Now()
	samples := 0
	for time.Since(started) < 30*time.Second {
		v, err := c.Local(ctx, r.NodeName, "held")
		if err != nil {
			t.Fatal(err)
		}
		peak = max(peak, v.Proposals.AwaitingCommit)
		if !v.Found || v.Status.LeaderID != r.ReplicaID || !v.WAL.Blocked || v.Proposals.InFlight > limit || v.Proposals.AwaitingCommit > limit || v.Proposals.AwaitingCommit < limit*8/10 || !maps.Equal(v.Values, map[string]string{"warm": "warm"}) {
			t.Fatalf("pending plateau or durability changed: %+v", v)
		}
		for _, n := range c.Spec.Nodes {
			meta, err := c.Local(ctx, n.Name, topology.MetaGroupID)
			if err != nil || !meta.Found || meta.Status.LeaderID == 0 {
				t.Fatalf("meta responsiveness: %s %v %+v", n.Name, err, meta)
			}
		}
		samples++
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	burst.Wait()
	measure.Wait()
	if samples < 100 {
		t.Fatalf("insufficient continuous pending samples: %d", samples)
	}
	for worker, replies := range paced {
		if len(replies) < 500 {
			t.Fatalf("insufficient paced overload worker %d: %d", worker, len(replies))
		}
		refusals = append(refusals, replies...)
	}
	latencies := make([]time.Duration, len(refusals))
	for i, reply := range refusals {
		var body struct {
			Code string `json:"code"`
		}
		if reply.err != nil || reply.response.Status != 503 || reply.response.Decode(&body) != nil || body.Code != "overloaded" {
			t.Fatalf("excess request %d: %+v body=%s", i, reply, reply.response.Data)
		}
		latencies[i] = reply.duration
	}
	slices.Sort(latencies)
	if p95 := latencies[len(latencies)*95/100]; p95 >= time.Second {
		t.Fatalf("overload waited behind persistence: p95=%s", p95)
	}
	for _, w := range measured {
		for id := range w.values {
			expectedHealthy[id] = id
		}
		if w.err != nil {
			t.Fatal(w.err)
		}
	}
	baseFraction := pressureFraction(t, baseline)
	measuredFraction := pressureFraction(t, measured)
	if measuredFraction < .3*baseFraction {
		t.Fatalf("independent apply latency collapsed: base=%f held=%f", baseFraction, measuredFraction)
	}
	if err := exactOwnedValues(ctx, c, healthy.Document, "healthy", expectedHealthy); err != nil {
		t.Fatal(err)
	}
	cancelHolders()
	holders.Wait()
	canceled := 0
	for i, reply := range heldReplies {
		if errors.Is(reply.err, context.Canceled) {
			canceled++
			continue
		}
		var refusal struct {
			Code string `json:"code"`
		}
		if reply.err != nil || reply.response.Status != 503 || reply.response.Decode(&refusal) != nil || refusal.Code != "overloaded" {
			t.Fatalf("held request %d: err=%v status=%d body=%s", i, reply.err, reply.response.Status, reply.response.Data)
		}
	}
	if canceled < limit*8/10 {
		t.Fatalf("only %d canceled waiters", canceled)
	}
	if err := Poll(ctx, func() (bool, error) {
		v, err := c.Local(ctx, r.NodeName, "held")
		return v.WAL.Blocked && v.WAL.Active > 0 && v.Proposals.InFlight == 0 && v.Proposals.Queued == 0 && v.Proposals.AwaitingCommit == 0, err
	}); err != nil {
		t.Fatal("canceled waiters retained by blocked writer", err)
	}
	if err := c.WALFault(ctx, r.NodeName, "held", r.ReplicaID, false, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		v, err := c.Local(ctx, r.NodeName, "held")
		return v.WALStallNS == 0 && v.WAL.Active == 0 && v.Status.LeaderID == r.ReplicaID, err
	}); err != nil {
		t.Fatal(err)
	}
	// Cancellation can leave a committed command. Retry every exact identity;
	// the caller FSM must preserve its fingerprint and materialize each once.
	expectedHeld := map[string]string{"warm": "warm"}
	for i := range limit {
		id := fmt.Sprintf("holder-%d", i)
		expectedHeld[id] = id
		reply := nativeProposal(ctx, c, r, "held", id)
		if err := requireStatus(reply.response, reply.err, 200); err != nil {
			t.Fatal(err)
		}
	}
	if err := exactOwnedValues(ctx, c, healthy.Document, "held", expectedHeld); err != nil {
		t.Fatal(err)
	}
	t.Logf("METRIC pressure_attempts=%d pending_peak=%d pending_limit=%d samples=%d overload_p95_ms=%.3f baseline_le100ms=%.3f held_le100ms=%.3f healthy_values=%d held_values=%d", len(refusals), peak, limit, samples, float64(latencies[len(latencies)*95/100])/float64(time.Millisecond), baseFraction, measuredFraction, len(expectedHealthy), len(expectedHeld))
}

type pressureWorkload struct {
	values    map[string]bool
	durations []time.Duration
	err       error
}

func pressureHealthyWorkload(ctx context.Context, c *Cluster, size int, prefix string, duration time.Duration) []pressureWorkload {
	results := make([]pressureWorkload, 40)
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for worker := range results {
		wg.Go(func() {
			w := &results[worker]
			w.values = map[string]bool{}
			for i := 0; time.Now().Before(deadline) && ctx.Err() == nil; i++ {
				id := fmt.Sprintf("%s-%d-%d", prefix, worker, i)
				start := time.Now()
				err := c.Put(ctx, c.Spec.Nodes[worker%size].Name, "healthy", id, id, id)
				w.durations = append(w.durations, time.Since(start))
				if err != nil {
					w.err = err
					return
				}
				w.values[id] = true
				select {
				case <-ctx.Done():
					w.err = ctx.Err()
					return
				case <-time.After(200 * time.Millisecond):
				}
			}
			if len(w.values) == 0 {
				w.err = fmt.Errorf("empty pressure worker %d", worker)
			}
		})
	}
	wg.Wait()
	return results
}
func pressureFraction(t *testing.T, results []pressureWorkload) float64 {
	t.Helper()
	count, fast := 0, 0
	for worker, w := range results {
		if len(w.values) < 20 {
			t.Fatalf("insufficient sustained worker %d: %d", worker, len(w.values))
		}
		for _, d := range w.durations {
			count++
			if d <= 100*time.Millisecond {
				fast++
			}
		}
	}
	if count == 0 {
		t.Fatal("empty latency measurement")
	}
	return float64(fast) / float64(count)
}
