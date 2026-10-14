package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

// ReadBarrier confirms a meta quorum round and waits for local application.
func (g *Group) ReadBarrier(parent context.Context) error {
	ctxHandle, err := g.acquireEffectContext(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer release()
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrReplicaNotReady
	}
	return engine.ReadBarrier(ctx)
}

type queryMode uint8

const (
	applicationQuery queryMode = iota
	controlQuery
)

func (g *Group) read(ctx context.Context, q metaengine.QueryRequest, result any) error {
	return g.readQuery(ctx, q, result, applicationQuery)
}

func (g *Group) readControl(ctx context.Context, q metaengine.QueryRequest, result any) error {
	return g.readQuery(ctx, q, result, controlQuery)
}

func (g *Group) readQuery(parent context.Context, q metaengine.QueryRequest, result any, mode queryMode) error {
	ctxHandle, err := g.acquireEffectContext(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer release()
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrReplicaNotReady
	}
	data, err := json.Marshal(q)
	if err != nil {
		return err
	}
	// Capture the engine once. Readmission cannot redirect a barrier into a
	// successor's machine between quorum confirmation and query execution.
	var r raftengine.Result
	if mode == controlQuery {
		r, err = engine.ReadControl(ctx, data)
	} else {
		r, err = engine.Read(ctx, data)
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(r.Data, result); err != nil {
		return fault.New(fault.Unreadable, "invalid metadata query result", err)
	}
	return nil
}

// ReadTopology returns topology views from a quorum-confirmed serialized
// machine query.
func (g *Group) ReadTopology(ctx context.Context) (topology.ReadView, error) {
	var result topology.ReadView
	err := g.read(ctx, metaengine.QueryRequest{Kind: "topology"}, &result)
	return result, err
}

// ReadLockHolder confirms current lock ownership through a serialized meta
// query.
func (g *Group) ReadLockHolder(ctx context.Context, key string) (topology.LockHolder, error) {
	if key == "" {
		return topology.LockHolder{}, fault.ErrInvalid
	}
	var result metaengine.LockRead
	err := g.read(ctx, metaengine.QueryRequest{Kind: "lock_holder", Key: key}, &result)
	return topology.LockHolder(result), err
}
