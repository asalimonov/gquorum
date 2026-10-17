package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

func (c *clusterView) ReadProgress(ctx context.Context, query topology.ProgressQuery) (topology.ProgressPage, error) {
	if err := query.Validate(); err != nil {
		return topology.ProgressPage{}, err
	}
	service, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.ProgressPage{}, err
	}
	return service.ReadProgress(ctx, query)
}
