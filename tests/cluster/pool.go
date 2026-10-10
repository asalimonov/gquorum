//go:build cluster

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
)

// A unit's lifecycle goroutine is its only resource owner. Tests borrow it with
// a generation-checked lease; canceled acquisitions never strand a lease.
type unitState int32

const (
	unitCold unitState = iota
	unitWarming
	unitReady
	unitInUse
	unitReleasing
	unitDead
)

type unitCommand struct {
	spec       Spec
	ctx        context.Context
	acquire    chan acquireResult
	release    chan error
	generation uint64
}
type acquireResult struct {
	lease *Lease
	err   error
}
type deploymentUnit struct {
	id         int
	state      atomic.Int32
	commands   chan unitCommand
	network    *testcontainers.DockerNetwork
	cluster    *Cluster
	generation uint64
}
type PoolOptions struct {
	Image                            string
	Size, CookParallel               int
	OperationTimeout, CleanupTimeout time.Duration
}
type Pool struct {
	opts      PoolOptions
	runID     string
	units     []*deploymentUnit
	available chan *deploymentUnit
	cooked    chan struct{}
	cooking   chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	startMu   sync.Mutex
	started   bool
	closing   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	errorsMu  sync.Mutex
	failures  []error
}
type Lease struct {
	Cluster     *Cluster
	pool        *Pool
	unit        *deploymentUnit
	generation  uint64
	releaseOnce sync.Once
	released    chan struct{}
	releaseErr  error
}

func NewPool(opts PoolOptions) (*Pool, error) {
	if opts.Image == "" || opts.Size < 1 || opts.CookParallel < 1 || opts.CookParallel > opts.Size {
		return nil, errors.New("pool requires image and positive bounded unit/cook counts")
	}
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = 45 * time.Second
	}
	if opts.CleanupTimeout == 0 {
		opts.CleanupTimeout = 30 * time.Second
	}
	if opts.OperationTimeout < 0 || opts.CleanupTimeout < 0 {
		return nil, errors.New("negative pool timeout")
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{opts: opts, runID: id, available: make(chan *deploymentUnit, opts.Size), cooked: make(chan struct{}), cooking: make(chan struct{}, opts.CookParallel), ctx: ctx, cancel: cancel, closing: make(chan struct{}), done: make(chan struct{})}, nil
}
func (p *Pool) failure(err error) {
	if err == nil {
		return
	}
	p.errorsMu.Lock()
	p.failures = append(p.failures, err)
	p.errorsMu.Unlock()
}
func (p *Pool) failureResult() error {
	p.errorsMu.Lock()
	defer p.errorsMu.Unlock()
	return errors.Join(p.failures...)
}
func (p *Pool) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.startMu.Lock()
	select {
	case <-p.closing:
		p.startMu.Unlock()
		return errors.New("pool closed")
	default:
	}
	if !p.started {
		p.started = true
		for i := range p.opts.Size {
			u := &deploymentUnit{id: i, commands: make(chan unitCommand)}
			p.units = append(p.units, u)
			p.wg.Go(func() { p.runUnit(u) })
		}
		go func() { p.wg.Wait(); close(p.done) }()
	}
	p.startMu.Unlock()
	for {
		select {
		case <-p.cooked:
			return nil
		case <-p.done:
			return fmt.Errorf("all pool units failed: %w", p.failureResult())
		case <-ctx.Done():
			p.RequestClose()
			return ctx.Err()
		case <-p.closing:
			return errors.New("pool closing")
		}
	}
}
func (p *Pool) Acquire(ctx context.Context, spec Spec) (*Lease, error) {
	spec = spec.normalized()
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.Start(ctx); err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.closing:
			return nil, errors.New("pool closing")
		case <-p.done:
			return nil, fmt.Errorf("no usable units: %w", p.failureResult())
		case u := <-p.available:
			reply := make(chan acquireResult)
			cmd := unitCommand{spec: spec, ctx: ctx, acquire: reply}
			select {
			case u.commands <- cmd:
			case <-ctx.Done():
				select {
				case p.available <- u:
				case <-p.closing:
				}
				return nil, ctx.Err()
			case <-p.closing:
				return nil, errors.New("pool closing")
			}
			select {
			case result := <-reply:
				return result.lease, result.err
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-p.closing:
				return nil, errors.New("pool closing")
			}
		}
	}
}
func (p *Pool) makeAvailable(u *deploymentUnit) {
	u.state.Store(int32(unitReady))
	select {
	case <-p.closing:
		return
	default:
	}
	select {
	case p.available <- u:
	case <-p.closing:
	}
}
func (p *Pool) takeCook(ctx context.Context) error {
	select {
	case p.cooking <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closing:
		return errors.New("pool closing")
	}
}
func (p *Pool) runUnit(u *deploymentUnit) {
	u.state.Store(int32(unitWarming))
	defer u.state.Store(int32(unitDead))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), p.opts.CleanupTimeout)
		defer cancel()
		if u.cluster != nil {
			if err := u.cluster.destroy(ctx); err != nil {
				p.failure(fmt.Errorf("unit %d cluster cleanup: %w", u.id, err))
			}
		}
		if u.network != nil {
			_ = u.network.Remove(ctx)
		}
		if err := p.removeUnitNetworks(ctx, u.id); err != nil {
			p.failure(fmt.Errorf("unit %d network cleanup: %w", u.id, err))
		}
	}()
	ctx, cancel := context.WithTimeout(p.ctx, p.opts.OperationTimeout)
	if err := p.takeCook(ctx); err != nil {
		cancel()
		if p.ctx.Err() == nil {
			p.failure(err)
		}
		return
	}
	// Once creation is admitted, join its bounded acknowledgement even if a
	// caller stops waiting. Canceling the Docker request itself lets the remote
	// network appear after cleanup has already observed an empty namespace.
	createCtx, finishCreate := context.WithTimeout(context.Background(), p.opts.OperationTimeout)
	network, err := tcnetwork.New(createCtx, tcnetwork.WithLabels(map[string]string{"gquorum.test.run": p.runID, "gquorum.test.unit": fmt.Sprint(u.id)}))
	finishCreate()
	<-p.cooking
	cancel()
	if err != nil {
		p.failure(fmt.Errorf("unit %d network: %w", u.id, err))
		return
	}
	u.network = network
	p.closeCooked()
	p.makeAvailable(u)
	for {
		select {
		case <-p.ctx.Done():
			return
		case cmd := <-u.commands:
			if cmd.acquire != nil {
				u.state.Store(int32(unitWarming))
				u.generation++
				bootCtx, stop := context.WithTimeout(cmd.ctx, p.opts.OperationTimeout)
				joined := make(chan struct{})
				stopParent := context.AfterFunc(p.ctx, func() { stop(); close(joined) })
				if err = p.takeCook(bootCtx); err == nil {
					u.cluster, err = newCluster(bootCtx, p, u, cmd.spec)
					<-p.cooking
				}
				stop()
				if !stopParent() {
					<-joined
				}
				if err != nil && u.cluster != nil && cmd.ctx.Err() == nil && p.ctx.Err() == nil {
					diagnostics, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err = fmt.Errorf("%w\n%s", err, u.cluster.diagnostics(diagnostics))
					cancel()
				}
				if err == nil {
					u.state.Store(int32(unitInUse))
					lease := &Lease{Cluster: u.cluster, pool: p, unit: u, generation: u.generation, released: make(chan struct{})}
					select {
					case cmd.acquire <- acquireResult{lease: lease}:
						continue
					case <-cmd.ctx.Done():
					case <-p.ctx.Done():
					}
				} else {
					select {
					case cmd.acquire <- acquireResult{err: err}:
					case <-cmd.ctx.Done():
					case <-p.ctx.Done():
					}
				}
				if err := p.resetUnit(u); err != nil {
					p.failure(err)
					return
				}
				p.makeAvailable(u)
			} else if cmd.release != nil {
				if cmd.generation != u.generation || unitState(u.state.Load()) != unitInUse {
					cmd.release <- errors.New("stale or unowned deployment lease")
					continue
				}
				err := p.resetUnit(u)
				cmd.release <- err
				if err != nil {
					p.failure(err)
					return
				}
				p.makeAvailable(u)
			}
		}
	}
}

// Network creation can also lose its acknowledgement on cancellation. Final
// teardown proves this unit's namespace is empty even without a returned handle.
func (p *Pool) removeUnitNetworks(ctx context.Context, unit int) error {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	filter := make(docker.Filters).Add("label", "gquorum.test.run="+p.runID, "gquorum.test.unit="+fmt.Sprint(unit))
	networks, err := cli.NetworkList(ctx, docker.NetworkListOptions{Filters: filter})
	if err != nil {
		return err
	}
	for _, owned := range networks.Items {
		if owned.Labels["gquorum.test.run"] != p.runID || owned.Labels["gquorum.test.unit"] != fmt.Sprint(unit) {
			return errors.New("network namespace changed during cleanup")
		}
		if _, err := cli.NetworkRemove(ctx, owned.ID, docker.NetworkRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}
	networks, err = cli.NetworkList(ctx, docker.NetworkListOptions{Filters: filter})
	if err != nil {
		return err
	}
	if len(networks.Items) > 0 {
		return errors.New("unit networks remain after cleanup")
	}
	return nil
}
func (p *Pool) closeCooked() {
	p.errorsMu.Lock()
	defer p.errorsMu.Unlock()
	select {
	case <-p.cooked:
	default:
		close(p.cooked)
	}
}
func (p *Pool) resetUnit(u *deploymentUnit) error {
	u.state.Store(int32(unitReleasing))
	if u.cluster == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.CleanupTimeout)
	defer cancel()
	if err := u.cluster.destroy(ctx); err != nil {
		return fmt.Errorf("unit %d reset: %w", u.id, err)
	}
	u.cluster = nil
	return nil
}
func (l *Lease) Release(ctx context.Context) error {
	l.releaseOnce.Do(func() {
		go func() {
			defer close(l.released)
			reply := make(chan error, 1)
			select {
			case l.unit.commands <- unitCommand{release: reply, generation: l.generation}:
				select {
				case l.releaseErr = <-reply:
				case <-l.pool.done:
					l.releaseErr = l.pool.failureResult()
				}
			case <-l.pool.closing:
				<-l.pool.done
				l.releaseErr = l.pool.failureResult()
			}
		}()
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-l.released:
		return l.releaseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *Pool) RequestClose() {
	p.closeOnce.Do(func() {
		p.startMu.Lock()
		close(p.closing)
		p.cancel()
		if !p.started {
			close(p.done)
		}
		p.startMu.Unlock()
	})
}
func (p *Pool) Close(ctx context.Context) error {
	p.RequestClose()
	select {
	case <-p.done:
		return p.failureResult()
	case <-ctx.Done():
		return fmt.Errorf("pool still owns cleanup: %w", ctx.Err())
	}
}
