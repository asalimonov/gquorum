package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/load"
)

func (n *node) ReceiveLoadRaft(ctx context.Context, batch load.RaftBatch) error {
	h, err := n.loadHost(ctx)
	if err != nil {
		return err
	}
	return h.ReceiveRaft(ctx, batch)
}

func (n *node) ReadLoadReplica(ctx context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	h, err := n.loadHost(ctx)
	if err != nil {
		return load.ReplicaStatus{}, err
	}
	return h.ReadReplica(ctx, query)
}

func (n *node) ApplyLoadAction(ctx context.Context, request load.ActionRequest) (load.ActionResult, error) {
	h, err := n.authorityHost(ctx)
	if err != nil {
		return load.ActionResult{}, err
	}
	return h.ApplyAction(ctx, request)
}

func (n *node) ReportLoadDelivery(ctx context.Context, report load.DeliveryReport) error {
	h, err := n.loadHost(ctx)
	if err != nil {
		return err
	}
	return h.ReportDelivery(ctx, report)
}

func (n *node) InstallLoadFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if n.needsReadmissionFence(request) {
		return n.installReadmissionFence(ctx, request)
	}
	h, err := n.authorityHost(ctx)
	if err != nil {
		return load.FenceResult{}, err
	}
	return h.InstallFence(ctx, request)
}
