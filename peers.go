package gquorum

import (
	"context"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// StaticPeer holds the reachable endpoints of one node in a fixed roster.
type StaticPeer struct {
	// MetaAddress is the host:port of the node's _meta listener.
	MetaAddress string `json:"meta_address"`
	// LoadAddress is the host:port of the node's load transport server.
	LoadAddress string `json:"load_address"`
}

// StaticPeers is a fixed roster keyed by node name. It routes _meta and load
// traffic; a route never proves admission, which Node checks against approved
// topology for every incoming load batch.
//
// TODO(not planned): dynamic service discovery (DNS records, gossip) can
// implement the same two resolver shapes as Resolve and ResolveLoad.
type StaticPeers map[string]StaticPeer

var _ topology.AddressResolver = StaticPeers(nil)

// Resolve returns the _meta address of name; use it as
// MetaNetworkOptions.Resolver.
func (p StaticPeers) Resolve(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if peer, ok := p[name]; ok && peer.MetaAddress != "" {
		return peer.MetaAddress, nil
	}
	return "", fault.New(fault.Unavailable, fmt.Sprintf("no meta address for node %q", name), nil)
}

// ResolveLoad returns the load address for id.NodeName; it matches
// loadgrpc.Options.Resolve.
func (p StaticPeers) ResolveLoad(ctx context.Context, id topology.Identity) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if peer, ok := p[id.NodeName]; ok && peer.LoadAddress != "" {
		return peer.LoadAddress, nil
	}
	return "", fault.New(fault.Unavailable, fmt.Sprintf("no load address for node %q", id.NodeName), nil)
}

// Founders returns every peer as a founding member of nodeType, sorted by name,
// with member IDs derived by FoundingIDs.
func (p StaticPeers) Founders(clusterID string, nodeType topology.NodeType) ([]FoundingMember, error) {
	names := make([]string, 0, len(p))
	for name := range p {
		names = append(names, name)
	}
	slices.Sort(names)
	ids, err := FoundingIDs(clusterID, names)
	if err != nil {
		return nil, err
	}
	founders := make([]FoundingMember, 0, len(names))
	for _, name := range names {
		founders = append(founders, FoundingMember{
			Node:     topology.Node{Name: name, Type: nodeType, State: topology.NodeActive},
			MemberID: ids[name],
			Address:  p[name].MetaAddress,
		})
	}
	return founders, nil
}
