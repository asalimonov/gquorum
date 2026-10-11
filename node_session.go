package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) ActivateSession(ctx context.Context) (topology.Session, error) {
	m, err := n.service(ctx, true)
	if err != nil {
		return topology.Session{}, err
	}
	return m.Activate(ctx)
}
func (n *node) KeepAlive(ctx context.Context, id string) error {
	m, err := n.service(ctx, true)
	if err != nil {
		return err
	}
	return m.KeepAlive(ctx, id)
}
func (n *node) DeactivateSession(ctx context.Context, id string) error {
	m, err := n.service(ctx, true)
	if err != nil {
		return err
	}
	return m.Deactivate(ctx, id)
}
func (n *node) OwnSessionLive() bool {
	m, err := n.service(context.Background(), true)
	return err == nil && m.OwnSessionLive()
}
func (n *node) ServeOverride() int {
	m, err := n.service(context.Background(), false)
	if err != nil {
		return 0
	}
	return m.ServeOverride()
}
func (n *node) SetServeOverride(ctx context.Context, value int) error {
	m, err := n.service(ctx, true)
	if err != nil {
		return err
	}
	return m.SetServeOverride(ctx, value)
}
func (n *node) SchedulingStatus() topology.SchedulingStatus {
	m, err := n.service(context.Background(), false)
	if err != nil {
		return topology.SchedulingStatus{}
	}
	return m.SchedulingStatus()
}
func (n *node) SetSchedulingEnabled(ctx context.Context, enabled bool) error {
	m, err := n.service(ctx, true)
	if err != nil {
		return err
	}
	return m.SetSchedulingEnabled(ctx, enabled)
}
