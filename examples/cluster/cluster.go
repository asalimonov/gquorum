package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"path/filepath"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
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

// newNode creates the application listener and validates a Node. The caller
// joins Node before closing the listener; Start transfers it to the library.
func newNode(cfg config) (gquorum.Node, net.Listener, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, nil, err
	}
	identity := topology.Identity{NodeName: cfg.Node, ClusterID: cfg.Cluster, InstanceID: hex.EncodeToString(nonce[:])}
	var founders []gquorum.FoundingMember
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
	listener, err := net.Listen("tcp", cfg.MetaListen)
	if err != nil {
		return nil, nil, err
	}
	node, err := gquorum.NewNode(gquorum.NodeOptions{
		Identity:  identity,
		Node:      local,
		Bootstrap: gquorum.BootstrapOptions{Founders: founders},
		Meta: gquorum.MetaOptions{
			DBPath: filepath.Join(cfg.Data, "meta.bolt"),
			Network: gquorum.MetaNetworkOptions{
				Listener: listener, AdvertiseAddress: resolve[cfg.Node], Resolver: resolve,
				AllowPlaintext: true, ClusterCredential: []byte(cfg.Credential),
			},
		},
	})
	if err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	return node, listener, nil
}

type readiness struct {
	Node          string `json:"node"`
	SessionLive   bool   `json:"session_live"`
	ActiveVersion uint64 `json:"active_version"`
}

func readReadiness(ctx context.Context, node gquorum.Node) (readiness, error) {
	status := node.Status()
	if !status.Ready || !status.SessionLive {
		return readiness{}, fault.ErrReplicaNotReady
	}
	active, err := node.Cluster().Active(ctx)
	if err != nil {
		return readiness{}, err
	}
	name := node.Identity().NodeName
	member := active.Node(name)
	if member == nil || member.State != topology.NodeActive {
		return readiness{}, fault.ErrReplicaNotReady
	}
	return readiness{Node: name, SessionLive: true, ActiveVersion: active.Version}, nil
}
