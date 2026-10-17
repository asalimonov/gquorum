package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

// requestPool shares a worker-sized connection budget across endpoints, with
// at least one connection per node. Each channel slot is an idle connection or
// permission to create one. Only its borrower uses the buffers and deadline.
type requestPool struct {
	endpoints   map[string]chan *resp.Client
	connections atomic.Int64
	dials       atomic.Int64
}

func newRequestPool(endpoints []string, workers int) *requestPool {
	p := &requestPool{endpoints: make(map[string]chan *resp.Client, len(endpoints))}
	perNode := max(1, (workers+len(endpoints)-1)/max(1, len(endpoints)))
	for _, endpoint := range endpoints {
		clients := make(chan *resp.Client, perNode)
		for range cap(clients) {
			clients <- nil
		}
		p.endpoints[endpoint] = clients
	}
	return p
}

// request reuses a pooled connection. Failed attempts discard their connection
// before retrying the same endpoint. Balanced requests select a node separately.
func (p *requestPool) request(ctx context.Context, endpoint string, args ...string) (resp.Reply, error) {
	return retryRequest(ctx, func(call context.Context) (resp.Reply, error) {
		return p.exchange(call, endpoint, args)
	})
}

func (p *requestPool) exchange(ctx context.Context, endpoint string, args []string) (resp.Reply, error) {
	if err := contextError(ctx); err != nil {
		return resp.Reply{}, err
	}
	clients := p.endpoints[endpoint]
	var client *resp.Client
	select {
	case <-ctx.Done():
		return resp.Reply{}, ctx.Err()
	case client = <-clients:
	}
	defer func() { clients <- client }()
	if err := contextError(ctx); err != nil {
		return resp.Reply{}, err
	}
	if client == nil {
		var err error
		p.dials.Add(1)
		client, err = resp.Dial(ctx, endpoint)
		if err != nil {
			return resp.Reply{}, err
		}
		p.connections.Add(1)
	}
	reply, err := exchange(ctx, client, args)
	if err != nil || retryableReply(reply, err) {
		_ = client.Close()
		p.connections.Add(-1)
		client = nil
	}
	return reply, err
}

// Join cancellation before returning the connection to the pool. A delayed
// cancellation callback must never close a later borrower's connection.
func exchange(ctx context.Context, client *resp.Client, args []string) (resp.Reply, error) {
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = client.Close(); close(closed) })
	deadline, _ := ctx.Deadline()
	reply, err := client.Do(time.Until(deadline), args...)
	if !stop() {
		<-closed
	}
	if ctx.Err() != nil {
		return resp.Reply{}, ctx.Err()
	}
	return reply, err
}

// close runs after workers have joined and the final report has finished.
func (p *requestPool) close() {
	for _, clients := range p.endpoints {
		for range cap(clients) {
			if client := <-clients; client != nil {
				_ = client.Close()
				p.connections.Add(-1)
			}
		}
	}
}
