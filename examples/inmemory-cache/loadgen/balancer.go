package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

const (
	healthInterval = 500 * time.Millisecond
	healthTimeout  = time.Second
	maxPort        = 65535
)

type backendHealth uint8

const (
	backendUnavailable backendHealth = iota
	backendHealthy
)

type routedReply struct {
	resp.Reply
	node string
}

type backend struct {
	address, healthURL string
	health             backendHealth // guarded by balancer.mu
}

// balancer serializes only selection and health publication. Probes run
// independently per node, with reusable HTTP connections and no overlapping
// probes for a slow node. All state starts unhealthy until a probe succeeds.
type balancer struct {
	mu      sync.Mutex
	nodes   []backend
	next    int
	changed chan struct{}
	pool    *requestPool
	client  *http.Client
	cancel  context.CancelFunc
	workers sync.WaitGroup
}

func (c config) validateNodes() error {
	if c.HealthPort < 1 || c.HealthPort > maxPort {
		return errors.New("health port must be in 1..65535")
	}
	seen := make(map[string]bool, len(c.Nodes))
	for _, address := range c.Nodes {
		host, port, err := net.SplitHostPort(address)
		number, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || number < 1 || number > maxPort || seen[address] {
			return errors.New("nodes must be unique host:port Redis endpoints")
		}
		seen[address] = true
	}
	return nil
}

func newBalancer(pool *requestPool, addresses []string, healthPort int) *balancer {
	b := &balancer{pool: pool, changed: make(chan struct{}), client: &http.Client{
		Timeout: healthTimeout,
		Transport: &http.Transport{MaxIdleConns: len(addresses), MaxIdleConnsPerHost: 1,
			IdleConnTimeout: time.Minute},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	for _, address := range addresses {
		host, _, _ := net.SplitHostPort(address) // validated before construction
		url := "http://" + net.JoinHostPort(host, strconv.Itoa(healthPort)) + "/health"
		b.nodes = append(b.nodes, backend{address: address, healthURL: url})
	}
	return b
}

func (b *balancer) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	b.cancel = cancel
	for i := range b.nodes {
		b.workers.Go(func() {
			tick := time.NewTicker(healthInterval)
			defer tick.Stop()
			for {
				b.probe(ctx, i)
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		})
	}
}

func (b *balancer) probe(ctx context.Context, index int) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.nodes[index].healthURL, nil)
	state := backendUnavailable
	if err == nil {
		response, callErr := b.client.Do(req)
		if callErr == nil {
			_, readErr := io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK && readErr == nil {
				state = backendHealthy
			}
		}
	}
	if ctx.Err() == nil {
		b.setHealth(index, state)
	}
}

func (b *balancer) setHealth(index int, state backendHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nodes[index].health != state {
		b.nodes[index].health = state
		close(b.changed)
		b.changed = make(chan struct{})
	}
}

func (b *balancer) selectNode(ctx context.Context) (int, error) {
	for {
		if err := contextError(ctx); err != nil {
			return 0, err
		}
		b.mu.Lock()
		for range len(b.nodes) {
			index := b.next
			b.next = (b.next + 1) % len(b.nodes)
			if b.nodes[index].health == backendHealthy {
				b.mu.Unlock()
				return index, nil
			}
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-changed:
		}
	}
}

// request selects again for every retry, retaining the same command bytes and
// the existing two-second initial / shared 500ms retry budgets.
func (b *balancer) request(ctx context.Context, args ...string) (routedReply, error) {
	address := ""
	reply, err := retryRequest(ctx, func(call context.Context) (resp.Reply, error) {
		index, err := b.selectNode(call)
		if err != nil {
			return resp.Reply{}, err
		}
		address = b.nodes[index].address
		result, err := b.pool.exchange(call, address, args)
		if (err != nil || retryableReply(result, err)) && contextError(ctx) == nil {
			b.setHealth(index, backendUnavailable)
		}
		return result, err
	})
	return routedReply{Reply: reply, node: address}, err
}

func (b *balancer) healthyNodes() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var nodes []string
	for _, node := range b.nodes {
		if node.health == backendHealthy {
			nodes = append(nodes, node.address)
		}
	}
	if len(nodes) == 0 {
		return "none"
	}
	return strings.Join(nodes, ",")
}

func (b *balancer) stop() {
	b.cancel()
	b.workers.Wait()
	b.client.CloseIdleConnections()
}
