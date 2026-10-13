package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) ReadBarrier(ctx context.Context) error {
	ctx, release, err := g.beginEffect(ctx)
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
func (g *Group) read(ctx context.Context, q metaengine.QueryRequest, result any) error {
	return g.readQuery(ctx, q, result, false)
}
func (g *Group) readControl(ctx context.Context, q metaengine.QueryRequest, result any) error {
	return g.readQuery(ctx, q, result, true)
}
func (g *Group) readQuery(ctx context.Context, q metaengine.QueryRequest, result any, control bool) error {
	ctx, release, err := g.beginEffect(ctx)
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
	if control {
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
func (g *Group) ReadTopology(ctx context.Context) (topology.ReadView, error) {
	var result topology.ReadView
	err := g.read(ctx, metaengine.QueryRequest{Kind: "topology"}, &result)
	return result, err
}
func (g *Group) ReadLockHolder(ctx context.Context, key string) (topology.Session, bool, error) {
	if key == "" {
		return topology.Session{}, false, fault.ErrInvalid
	}
	var result metaengine.LockRead
	err := g.read(ctx, metaengine.QueryRequest{Kind: "lock_holder", Key: key}, &result)
	return result.Session, result.Held, err
}
