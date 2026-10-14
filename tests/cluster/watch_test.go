//go:build clustertest

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/topology"
)

// Each reader owns its request and decoder. Closing joins the decoder before
// the pool lease can be released, including a failure while awaiting an update.
type topologyStream struct {
	updates chan topology.Update
	errors  chan error
	cancel  context.CancelFunc
	body    io.ReadCloser
	done    chan struct{}
}

func watchTopology(ctx context.Context, t *testing.T, c *Cluster, node string, view topology.View) *topologyStream {
	t.Helper()
	owned, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(owned, "GET", c.Node(node).Endpoint()+"/internal/test/watch/"+string(view), nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	req.Header.Set("X-Gquorum-Test-Credential", c.Credential)
	// Streams use the lease's transport and the case deadline, not its six-second
	// unary-client timeout. The pool owns closing this transport's idle clients.
	response, err := (&http.Client{Transport: c.http.Transport}).Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		cancel()
		response.Body.Close()
		t.Fatal("watch status", response.StatusCode)
	}
	stream := &topologyStream{updates: make(chan topology.Update, 16), errors: make(chan error, 1), cancel: cancel, body: response.Body, done: make(chan struct{})}
	go func() {
		defer close(stream.done)
		decoder := json.NewDecoder(response.Body)
		for {
			var update topology.Update
			if err := decoder.Decode(&update); err != nil {
				stream.errors <- err
				return
			}
			select {
			case stream.updates <- update:
			case <-owned.Done():
				return
			}
		}
	}()
	t.Cleanup(stream.close)
	return stream
}
func (s *topologyStream) close() { s.cancel(); s.body.Close(); <-s.done }
func (s *topologyStream) next(t *testing.T, view topology.View, version uint64) topology.Update {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case update := <-s.updates:
		if update.View != view || update.Document.Version != version {
			t.Fatal("watch scope/version", update, view, version)
		}
		return update
	case err := <-s.errors:
		t.Fatal("watch ended before expected update", err)
	case <-timer.C:
		t.Fatal("watch update timed out", view, version)
	}
	return topology.Update{}
}
func TestMetaWatchFanoutIsOwnedAndDurablyAppliedOnFollowersThreeNodes(t *testing.T) {
	t.Parallel()
	const size = 3
	c := acquire(t, matrixSpec(size)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	streams := map[string]map[topology.View]*topologyStream{}
	views := []topology.View{topology.Latest, topology.Target, topology.Active}
	for _, node := range c.Nodes() {
		streams[node.Spec.Name] = map[topology.View]*topologyStream{}
		for _, view := range views {
			stream := watchTopology(ctx, t, c, node.Spec.Name, view)
			stream.next(t, view, 1)
			streams[node.Spec.Name][view] = stream
		}
	}
	base, err := c.Document(ctx, "node2", "latest")
	if err != nil {
		t.Fatal(err)
	}
	if base.Nodes[0].Labels == nil {
		base.Nodes[0].Labels = map[string]string{}
	}
	base.Nodes[0].Labels["rack"] = "one"
	staged, err := c.Stage(ctx, "node2", base)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		update := streams[node.Spec.Name][topology.Latest].next(t, topology.Latest, staged.Document.Version)
		if update.Document.Nodes[0].Labels["rack"] != "one" {
			t.Fatal("watch lost content", update)
		}
		update.Document.Nodes[0].Labels["rack"] = "client-mutation"
		current, err := c.Document(ctx, node.Spec.Name, "latest")
		if err != nil || current.Nodes[0].Labels["rack"] != "one" {
			t.Fatal("subscriber changed live topology", current, err)
		}
	}
	// One shared observation window, not a separate sleep per node/view.
	until := time.After(150 * time.Millisecond)
quiet:
	for {
		for _, node := range c.Nodes() {
			for _, view := range []topology.View{topology.Target, topology.Active} {
				select {
				case update := <-streams[node.Spec.Name][view].updates:
					t.Fatal("staging notified approved view", update)
				case err := <-streams[node.Spec.Name][view].errors:
					t.Fatal(err)
				default:
				}
			}
		}
		select {
		case <-until:
			break quiet
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		for _, view := range []topology.View{topology.Target, topology.Active} {
			update := streams[node.Spec.Name][view].next(t, view, staged.Document.Version)
			if update.Document.Nodes[0].Labels["rack"] != "one" {
				t.Fatal("approved watch lost content", update)
			}
		}
	}
	for _, byView := range streams {
		for _, stream := range byView {
			stream.close()
		}
	}
	// Abrupt restart after delivery proves each follower's observed application
	// was durable, without reading a live bbolt file through a second owner.
	status, err := c.Status(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if node.Spec.Name == status.MetaLeaderNodeName {
			continue
		}
		before, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Kill(ctx); err != nil {
			t.Fatal(err)
		}
		if err := node.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := c.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		after, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil || after.Identity.MemberID != before.Identity.MemberID || after.Identity.InstanceID == before.Identity.InstanceID {
			t.Fatal("watch restart lost identity", after, err)
		}
		for _, view := range views {
			doc, err := c.Document(ctx, node.Spec.Name, string(view))
			if err != nil || doc.Version != staged.Document.Version || doc.Nodes[0].Labels["rack"] != "one" {
				t.Fatal("watch preceded durable follower application", node.Spec.Name, view, doc, err)
			}
		}
	}
}

func TestFixtureLeaderEventIsJournaledBeforeStreamDelivery(t *testing.T) {
	t.Parallel()
	ctx, c := performanceCluster(t, 3)
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "events"})
	if err != nil {
		t.Fatal(err)
	}
	local, err := c.Local(ctx, "node1", "events")
	if err != nil {
		t.Fatal(err)
	}
	target := "node1"
	if local.Status.LeaderID == local.ReplicaID {
		target = "node2"
	}
	watch, cancel := context.WithCancel(ctx)
	defer cancel()
	request, err := http.NewRequestWithContext(watch, "GET", c.Node("node1").Endpoint()+"/api/events/watch?group=events&type=group.leader_changed", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Use the actual event constant, including its compatibility spelling.
	query := request.URL.Query()
	query.Set("type", string(events.LeaderChange))
	request.URL.RawQuery = query.Encode()
	response, err := (&http.Client{Transport: c.http.Transport}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("stream status", response.StatusCode)
	}
	result := make(chan struct {
		Event events.Event
		Lost  uint64
		Err   error
	}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var delivery struct {
				Event events.Event `json:"event"`
				Lost  uint64       `json:"lost_before"`
			}
			err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &delivery)
			if err == nil && delivery.Event.NewLeader != fmt.Sprint(replicaID(created.Document, "events", target)) {
				continue
			}
			result <- struct {
				Event events.Event
				Lost  uint64
				Err   error
			}{delivery.Event, delivery.Lost, err}
			return
		}
		result <- struct {
			Event events.Event
			Lost  uint64
			Err   error
		}{Err: scanner.Err()}
	}()
	defer func() { cancel(); response.Body.Close(); <-done }()
	if err := requestLeaderTransfer(ctx, c, "node2", "events", target); err != nil {
		t.Fatal(err)
	}
	var event events.Event
	select {
	case delivery := <-result:
		if delivery.Err != nil || delivery.Lost != 0 || delivery.Event.Seq == 0 || delivery.Event.Type != events.LeaderChange || delivery.Event.GroupID != "events" {
			t.Fatal("invalid streamed leader event", delivery)
		}
		event = delivery.Event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	journal, err := c.Request(ctx, "node1", "GET", fmt.Sprintf("/api/events?group=events&type=%s&after=%d", events.LeaderChange, event.Seq-1), nil, 0)
	if err := requireStatus(journal, err, 200); err != nil {
		t.Fatal(err)
	}
	var page struct {
		Events []events.Event `json:"events"`
	}
	if err := journal.Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Events) == 0 || page.Events[0].Seq != event.Seq || page.Events[0].Type != event.Type || page.Events[0].GroupID != event.GroupID {
		t.Fatal("live delivery preceded journal append", event, page)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Local(ctx, "node1", "events")
		return status.Status.LeaderID == replicaID(created.Document, "events", target), err
	}); err != nil {
		t.Fatal(err)
	}
}
