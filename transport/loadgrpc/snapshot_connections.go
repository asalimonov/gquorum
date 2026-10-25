package loadgrpc

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
)

// Bulk connections deliberately use the same resolver and address, but never
// the ordinary Raft/control ClientConn or its flow-control windows.
func (t *Transport) snapshotClient(ctx context.Context, to topology.Identity) (clientRef, error) {
	address, err := t.resolve(ctx, to)
	if err != nil {
		return clientRef{}, err
	}
	t.mu.Lock()
	var evicted *grpc.ClientConn
	defer func() {
		t.mu.Unlock()
		if evicted != nil {
			_ = evicted.Close()
		}
	}()
	if t.closed {
		return clientRef{}, fault.ErrStopped
	}
	c := t.snapshotClients[address]
	if c == nil {
		if len(t.snapshotClients) >= t.snapshotOptions.MaxOutgoing {
			evicted = t.evictSnapshotConnection()
			if len(t.snapshotClients) >= t.snapshotOptions.MaxOutgoing {
				return clientRef{}, fault.ErrOverloaded
			}
		}
		options := append([]grpc.DialOption(nil), t.options.DialOptions...)
		options = append(options, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(t.options.MaxMessageBytes), grpc.MaxCallSendMsgSize(t.options.MaxMessageBytes)))
		conn, err := grpc.NewClient(address, options...)
		if err != nil {
			return clientRef{}, fault.New(fault.Unavailable, "open snapshot connection", err)
		}
		c = &connection{conn: conn}
		t.snapshotClients[address] = c
	}
	c.refs++
	return clientRef{Conn: c.conn, Release: func() { t.mu.Lock(); c.refs--; t.mu.Unlock() }}, nil
}
func (t *Transport) closeSnapshotConnections() error {
	t.mu.Lock()
	clients := t.snapshotClients
	t.snapshotClients = nil
	t.mu.Unlock()
	var result error
	for _, client := range clients {
		result = errors.Join(result, client.conn.Close())
	}
	return result
}

func (t *Transport) evictSnapshotConnection() *grpc.ClientConn {
	for endpoint, idle := range t.snapshotClients {
		if idle.refs == 0 {
			delete(t.snapshotClients, endpoint)
			return idle.conn
		}
	}
	return nil
}
