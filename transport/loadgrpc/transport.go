// Package loadgrpc provides an optional authenticated gRPC implementation of
// load.Transport. Applications own the server, listeners, TLS configuration and
// endpoint discovery. Node remains responsible for authorization of effects.
package loadgrpc

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxConnections     = 64
	defaultMaxConcurrentCalls = 128
	defaultMaxMessageBytes    = 16 << 20
	minimumMessageBytes       = 1024
	defaultCallTimeout        = 10 * time.Second
)

// Ingress is the portion of gquorum.Node used by this adapter. Bind one Node
// after constructing it with this Transport, before starting it or serving
// RPCs.
type Ingress interface {
	Identity() topology.Identity
	ReceiveLoadRaft(context.Context, load.RaftBatch) error
	ReadLoadReplica(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
	ApplyLoadAction(context.Context, load.ActionRequest) (load.ActionResult, error)
	InstallLoadFence(context.Context, load.FenceRequest) (load.FenceResult, error)
	ReportLoadDelivery(context.Context, load.DeliveryReport) error
}

// Options supplies application-owned networking and authentication. Callbacks
// may run concurrently; they must respect cancellation. They must not call
// Close
// synchronously because Close joins their calls.
type Options struct {
	// Resolve returns a route for the full recipient identity, including
	// retained
	// cleanup destinations. A route is not proof of admission or authorization.
	Resolve func(context.Context, topology.Identity) (string, error)
	// Authenticate verifies the incoming credentials and, for node-specific
	// credentials, binds them to the declared sender. It runs before Node
	// ingress.
	Authenticate func(context.Context, topology.Identity) error
	// DialOptions must explicitly select transport credentials, including
	// insecure credentials for a deliberate plaintext deployment. No
	// credentials are inferred.
	DialOptions []grpc.DialOption
	// MaxConnections bounds cached connections. Idle connections can be
	// evicted;
	// when every connection is busy, opening another fails with Overloaded.
	MaxConnections int
	// MaxConcurrentCalls bounds total incoming and outgoing operations.
	MaxConcurrentCalls int
	// MaxMessageBytes bounds protobuf requests and replies; default 16 MiB.
	// Configure the caller's grpc.Server with the same receive/send limits.
	MaxMessageBytes int
	// CallTimeout bounds each complete operation, including resolution and Node
	// ingress; a shorter caller deadline wins. Default 10 seconds.
	CallTimeout time.Duration
}

// Transport owns its connections and in-flight calls, but borrows the Node and
// server. Stop Node while this adapter and the server are still available, then
// Close the adapter and stop the server. Close timeouts retain join ownership.
type Transport struct {
	options Options
	mu      sync.Mutex
	node    Ingress
	closed  bool
	active  int
	clients map[string]*connection
	// Outgoing Raft streams by exact replica route; addresses whose recipient
	// lacks StreamRaft map to the time streaming is retried.
	streams    map[streamKey]*raftStream
	janitor    bool
	streamIdle time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	done       chan struct{}
}
type connection struct {
	conn *grpc.ClientConn
	refs int
}

var _ load.Transport = (*Transport)(nil)

// New validates and copies options without I/O, callbacks or goroutines.
func New(options Options) (*Transport, error) {
	if options.Resolve == nil || options.Authenticate == nil || len(options.DialOptions) == 0 ||
		options.MaxConnections < 0 ||
		options.MaxConcurrentCalls < 0 || options.MaxMessageBytes < 0 || options.CallTimeout < 0 {
		return nil, fault.New(fault.Invalid,
			"loadgrpc requires resolution, authentication, explicit dial options and nonnegative limits", nil)
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = defaultMaxConnections
	}
	if options.MaxConcurrentCalls == 0 {
		options.MaxConcurrentCalls = defaultMaxConcurrentCalls
	}
	if options.MaxMessageBytes == 0 {
		options.MaxMessageBytes = defaultMaxMessageBytes
	}
	if options.MaxMessageBytes < minimumMessageBytes {
		return nil, fault.New(fault.Invalid, "loadgrpc message limit must be at least 1024 bytes", nil)
	}
	if options.CallTimeout == 0 {
		options.CallTimeout = defaultCallTimeout
	}
	options.DialOptions = append([]grpc.DialOption(nil), options.DialOptions...)
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		options:    options,
		clients:    make(map[string]*connection),
		streams:    make(map[streamKey]*raftStream),
		streamIdle: defaultStreamIdle,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}, nil
}

// Bind installs one exact Node lifetime. It refuses rebinding, even to the same
// Node, so an old transport cannot silently begin serving a replacement owner.
func (t *Transport) Bind(node Ingress) error {
	if node == nil || (reflect.ValueOf(node).Kind() == reflect.Pointer && reflect.ValueOf(node).IsNil()) {
		return fault.ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fault.ErrStopped
	}
	if t.node != nil {
		return fault.New(fault.Invalid, "loadgrpc already bound", nil)
	}
	t.node = node
	return nil
}

// Register installs the load service on the caller's server. Call once before
// serving; the server's gRPC registration rules apply. It does not start it.
func (t *Transport) Register(server grpc.ServiceRegistrar) {
	loadpb.RegisterLoadServer(server, &serverAdapter{transport: t})
}

type ingressCall struct {
	Context context.Context
	Node    Ingress
	Release func()
}

type clientRef struct {
	Conn    *grpc.ClientConn
	Release func()
}

func (t *Transport) begin(parent context.Context) (ingressCall, error) {
	if err := parent.Err(); err != nil {
		return ingressCall{}, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ingressCall{}, fault.ErrStopped
	}
	if t.node == nil {
		t.mu.Unlock()
		return ingressCall{}, fault.ErrReplicaNotReady
	}
	if t.active >= t.options.MaxConcurrentCalls {
		t.mu.Unlock()
		return ingressCall{}, fault.ErrOverloaded
	}
	t.active++
	t.wg.Add(1)
	node := t.node
	t.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, t.options.CallTimeout)
	joined := make(chan struct{})
	stop := context.AfterFunc(t.ctx, func() { cancel(); close(joined) })
	return ingressCall{Context: ctx, Node: node, Release: func() {
		cancel()
		if !stop() {
			<-joined
		}
		t.mu.Lock()
		t.active--
		t.mu.Unlock()
		t.wg.Done()
	}}, nil
}

func (t *Transport) client(ctx context.Context, to topology.Identity) (clientRef, error) {
	address, err := t.resolve(ctx, to)
	if err != nil {
		return clientRef{}, err
	}
	return t.connect(address)
}

func (t *Transport) resolve(ctx context.Context, to topology.Identity) (string, error) {
	address, err := t.options.Resolve(ctx, to)
	if err != nil {
		return "", err
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	if address == "" {
		return "", fault.New(fault.Unavailable, "empty load endpoint", nil)
	}
	return address, nil
}

// connect returns a referenced cached connection; release drops the reference.
func (t *Transport) connect(address string) (clientRef, error) {
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
	c := t.clients[address]
	if c == nil {
		if len(t.clients) >= t.options.MaxConnections {
			evicted = t.evictIdleConnection()
			if len(t.clients) >= t.options.MaxConnections {
				return clientRef{}, fault.ErrOverloaded
			}
		}
		opts := append([]grpc.DialOption(nil), t.options.DialOptions...)
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(t.options.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(t.options.MaxMessageBytes)))
		conn, err := grpc.NewClient(address, opts...)
		if err != nil {
			return clientRef{}, fault.New(fault.Unavailable, "open load connection", err)
		}
		c = &connection{conn: conn}
		t.clients[address] = c
	}
	c.refs++
	return clientRef{Conn: c.conn, Release: func() { t.mu.Lock(); c.refs--; t.mu.Unlock() }}, nil
}

// evictIdleConnection runs while the connection cache lock is held. Its caller
// closes the detached connection after releasing that lock.
func (t *Transport) evictIdleConnection() *grpc.ClientConn {
	for endpoint, idle := range t.clients {
		if idle.refs == 0 {
			evicted := idle.conn
			delete(t.clients, endpoint)
			return evicted
		}
	}
	return nil
}

// Close fences new calls, cancels and joins accepted work, then releases owned
// connections. A timeout only ends this wait; later Close calls join the same
// teardown. The application must also stop its borrowed gRPC server.
func (t *Transport) Close(ctx context.Context) error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		t.cancel()
		go func() {
			t.wg.Wait()
			t.mu.Lock()
			clients := t.clients
			t.clients, t.streams = nil, nil
			t.mu.Unlock()
			for _, c := range clients {
				_ = c.conn.Close()
			}
			close(t.done)
		}()
	}
	t.mu.Unlock()
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validIdentity(id topology.Identity) bool {
	return id.ClusterID != "" && id.NodeName != "" && id.MemberID != 0 && id.InstanceID != ""
}
func checkRoute(local, from, to topology.Identity) error {
	if from.ClusterID != to.ClusterID || local != from || !validIdentity(from) || !validIdentity(to) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func transportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := fault.Unavailable
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		code = fault.IdentityMismatch
	case codes.InvalidArgument:
		code = fault.Invalid
	case codes.ResourceExhausted:
		code = fault.Overloaded
	default: // Keep the default fault classification for other RPC codes.
	}
	return fault.New(code, "load RPC failed", err)
}
