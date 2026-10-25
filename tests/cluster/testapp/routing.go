package main

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Routes are local hints, never quorum reads or effect authorization. Historical
// endpoints remain usable for cleanup, but staged-only documents cannot supply them.
func (a *application) approvedRoute(ctx context.Context, match func(topology.Document) bool) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	if a.currentCluster() == nil {
		return topology.Document{}, fault.ErrUnavailable
	}
	target, err := a.currentCluster().Target(ctx)
	if err != nil {
		return topology.Document{}, err
	}
	if match(target) {
		return target, nil
	}
	history, err := a.currentCluster().History(ctx, topology.Page{})
	if err != nil {
		return topology.Document{}, err
	}
	for _, doc := range history.Documents {
		if doc.Version == target.Version || !match(doc) {
			continue
		}
		id := topology.MakeOperationID(doc.Version, 1)
		op, err := a.currentCluster().Operation(ctx, id)
		if err == nil && op.ID == id && op.Version == doc.Version {
			return doc, nil
		}
	}
	return topology.Document{}, fault.New(fault.Unavailable, "no retained approved route", nil)
}

func replicaOwner(group *topology.Group, id uint64) string {
	if group != nil {
		for _, replica := range group.Replicas {
			if replica.ReplicaID == id {
				return replica.NodeName
			}
		}
	}
	return ""
}

func (a *application) loadAddress(ctx context.Context, name string, member uint64) (string, error) {
	if member == 0 {
		return a.address(ctx, name, loadAddressLabel)
	}
	doc, err := a.approvedRoute(ctx, func(doc topology.Document) bool {
		node := doc.Node(name)
		return doc.MemberID(name) == member && node != nil
	})
	if err == nil {
		if address := doc.Node(name).Labels[loadAddressLabel]; address != "" {
			return address, nil
		}
		for _, seed := range a.cfg.Peers {
			if seed.Name == name && seed.LoadAddress != "" {
				return seed.LoadAddress, nil
			}
		}
		err = fault.ErrUnavailable
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	// The cache is pinned to the exact member. A receiver still authenticates the
	// full destination and grant, including when a successor now uses this address.
	a.mu.RLock()
	defer a.mu.RUnlock()
	if old := a.clients[name]; old != nil && old.member == member {
		return old.address, nil
	}
	return "", err
}
