//go:build functional

package metaservice

import (
	"encoding/json"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/metrics"
	"testing"
	"time"
)

func TestMetaReplicaMetricsAuthenticatedLocalRead(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) { o.Metrics.SampleInterval = 10 * time.Millisecond }))
	waitPublic(t, func() bool { return g.engine.Load().Metrics().State == metrics.Current })
	header := g.transport.Header()
	reply, err := g.transport.CallAddress(t.Context(), g.opts.Network.Listener.Addr().String(), "MetaReplicaMetrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := metatransport.ReplyError(reply); err != nil {
		t.Fatal(err)
	}
	var s metrics.ReplicaSample
	if err := json.Unmarshal(reply.Data, &s); err != nil {
		t.Fatal(err)
	}
	if s.Identity.NodeName != header.NodeName || s.Identity.InstanceID != header.InstanceId || s.Identity.ReplicaID != header.MemberId || s.Identity.RunID == "" || !s.Ready {
		t.Fatal(s)
	}
	header.InstanceId = "stale-process"
	if err := metatransport.ReplyError(g.Call(t.Context(), "MetaReplicaMetrics", header, nil)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	header = g.transport.Header()
	if err := metatransport.ReplyError(g.Call(t.Context(), "MetaReplicaMetrics", header, []byte("unexpected"))); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
