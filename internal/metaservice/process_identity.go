package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

// Admission ownership lasts through metadata retirement and replacement. A
// process probe establishes only that the exact admitted process is still alive;
// it cannot prove readiness, member ownership or consumption of any effect.
type processIdentityRequest struct {
	NodeName   string `json:"node_name"`
	InstanceID string `json:"instance_id"`
}

// Call has already authenticated the cluster and acquired protocol lifetime.
func (g *Group) processIdentityReply(request processIdentityRequest) *metapb.Reply {
	id := g.Identity()
	if request.NodeName == "" || request.InstanceID == "" || request.NodeName != id.NodeName || request.InstanceID != id.InstanceID {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	data, err := json.Marshal(id)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: data}
}

func (g *Group) probeAdmission(parent context.Context, admission metaengine.AdmissionRecord) bool {
	ctx, cancel := context.WithTimeout(parent, g.opts.Timing.DialTimeout)
	defer cancel()
	address, err := g.resolveNode(ctx, admission.NodeName)
	if err != nil {
		return false
	}
	data, err := json.Marshal(authorityIdentityRequest{Process: &processIdentityRequest{NodeName: admission.NodeName, InstanceID: admission.InstanceID}})
	if err != nil {
		return false
	}
	reply, err := g.transport.CallAddress(ctx, address, "IdentityPing", data)
	if err != nil || metatransport.ReplyError(reply) != nil {
		return false
	}
	var id topology.Identity
	if json.Unmarshal(reply.Data, &id) != nil {
		return false
	}
	return id.ClusterID == g.Identity().ClusterID && id.NodeName == admission.NodeName && id.InstanceID == admission.InstanceID
}
