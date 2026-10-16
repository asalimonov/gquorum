package main

import (
	"context"
	"net"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/util"
)

type addresses map[string]string

func (a addresses) Resolve(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if address := a[name]; address != "" {
		return address, nil
	}
	return "", fault.ErrIdentityMismatch
}

type nodeResources struct {
	node     gquorum.Node
	listener net.Listener
}

// newNode creates the application listener and validates a Node. The caller
// joins Node before closing the listener; Start transfers it to the library.
func newNode(cfg config) (nodeResources, error) {
	// A restart keeps its durable member identity but gets a new process
	// identity.
	identity := topology.Identity{NodeName: cfg.Node, ClusterID: cfg.Cluster, InstanceID: util.NewInstanceID()}
	// The manifest describes the founding voters and how to reach each peer.
	// Every node uses the same manifest; this loop also finds our local node
	// type.
	founders := collection.Slice[gquorum.FoundingMember](len(cfg.Peers.Members))
	var local topology.Node
	resolve := addresses{}
	for _, p := range cfg.Peers.Members {
		member := topology.Node{Name: p.Name, Type: p.Type, State: topology.NodeActive}
		founders = append(founders, gquorum.FoundingMember{Node: member, MemberID: p.MemberID, Address: p.MetaAddress})
		resolve[p.Name] = p.MetaAddress
		if p.Name == cfg.Node {
			local = member
			identity.MemberID = p.MemberID
		}
	}
	// The application binds the address. Node takes ownership when Start
	// acquires
	// the metadata service; construction errors leave cleanup with this caller.
	listener, err := net.Listen("tcp", cfg.MetaListen)
	if err != nil {
		return nodeResources{}, err
	}
	node, err := gquorum.NewNode(gquorum.NodeOptions{
		// The library owns _meta.bolt and _authority.bolt inside this
		// directory.
		// This example is meta-only, so it needs no application load storage.
		StorageDir: cfg.Data,
		Identity:   identity,
		Node:       local,
		Bootstrap:  gquorum.BootstrapOptions{Founders: founders},
		Meta: gquorum.MetaOptions{
			// The resolver maps readable node names to protocol addresses. All
			// peers use the same credential to authenticate metadata traffic in
			// this demo.
			Network: gquorum.MetaNetworkOptions{
				Listener: listener, AdvertiseAddress: resolve[cfg.Node], Resolver: resolve,
				AllowPlaintext: true, ClusterCredential: []byte(cfg.Credential),
			},
		},
	})
	if err != nil {
		_ = listener.Close()
		return nodeResources{}, err
	}
	return nodeResources{node: node, listener: listener}, nil
}

type readiness struct {
	Node          string `json:"node"`
	SessionLive   bool   `json:"session_live"`
	TargetVersion uint64 `json:"target_version"`
}

func readReadiness(ctx context.Context, node gquorum.Node) (readiness, error) {
	// Startup returns before quorum readiness. Serve ready only after
	// admission, a live session and approved node intent agree that this
	// node may serve.
	status := node.Status()
	if !status.Ready || !status.SessionLive {
		return readiness{}, fault.ErrReplicaNotReady
	}
	name := node.Identity().NodeName
	query := topology.ProgressQuery{NodeName: name}
	for {
		progress, err := node.Cluster().ReadProgress(ctx, query)
		if err != nil {
			return readiness{}, err
		}
		for _, row := range progress.Nodes {
			if row.NodeName != name {
				continue
			}
			if row.Approved == nil || row.Approved.State != topology.NodeActive {
				return readiness{}, fault.ErrReplicaNotReady
			}
			return readiness{Node: name, SessionLive: true, TargetVersion: progress.TargetVersion}, nil
		}
		if progress.Next == "" {
			return readiness{}, fault.ErrReplicaNotReady
		}
		query.After = progress.Next
	}
}
