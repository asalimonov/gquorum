//go:build !clustertest

package main

import (
	"context"
	"net/http"

	"github.com/asalimonov/gquorum/topology"
)

type clusterHooks struct{}

func newClusterHooks() *clusterHooks                                                    { return nil }
func (*application) registerClusterRoutes(*http.ServeMux)                               {}
func (*application) beforeClusterEffect(context.Context, topology.AuthorizedStep) error { return nil }
func (*application) afterClusterEffect(topology.AuthorizedStep, error)                  {}
func (*application) receivedClusterSnapshot(string)                                     {}
