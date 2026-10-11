//go:build cluster

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	containerapi "github.com/moby/moby/api/types/container"
	docker "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type NodeSpec struct {
	Name string
	Type topology.NodeType
}
type Spec struct {
	Nodes                                 []NodeSpec
	Limits                                topology.Limits
	Bootstrap                             bool
	Deferred                              []string
	SkipReady                             bool
	AddressFree                           bool
	PublishMeta                           bool
	GateFounders                          bool
	FoundingTimeout                       time.Duration
	SessionTTL, Tick                      time.Duration
	WALWriteWait                          time.Duration
	SnapshotThreshold                     uint64
	Scheduling                            bool
	SchedulingInterval, SchedulingTimeout time.Duration
}

func (s Spec) normalized() Spec {
	s.Nodes = slices.Clone(s.Nodes)
	s.Deferred = slices.Clone(s.Deferred)
	s.Limits = s.Limits.WithDefaults()
	if s.SessionTTL == 0 {
		s.SessionTTL = 5 * time.Second
	}
	if s.Tick == 0 {
		s.Tick = 100 * time.Millisecond
	}
	if s.SnapshotThreshold == 0 {
		s.SnapshotThreshold = 8
	}
	return s
}
func (s Spec) validate() error {
	s = s.normalized()
	if err := s.Limits.Validate(); err != nil {
		return err
	}
	if len(s.Nodes) < 1 || len(s.Nodes) > s.Limits.MaxNodes {
		return errors.New("initial node count exceeds Limits")
	}
	names := map[string]bool{}
	for _, node := range s.Nodes {
		if node.Name == "" || strings.TrimSpace(string(node.Type)) == "" || names[node.Name] {
			return errors.New("invalid initial node identities")
		}
		if err := topology.ValidateID(node.Name); err != nil {
			return err
		}
		names[node.Name] = true
	}
	for _, name := range s.Deferred {
		if !names[name] {
			return errors.New("deferred node is not declared")
		}
	}
	if s.SessionTTL <= 0 || s.Tick <= 0 {
		return errors.New("invalid cluster timing")
	}
	if s.FoundingTimeout < 0 {
		return errors.New("negative founding timeout")
	}
	if s.WALWriteWait < 0 || s.WALWriteWait > 10*time.Millisecond {
		return errors.New("invalid load WAL coalescing wait")
	}
	return (metaservice.SchedulingOptions{Interval: s.SchedulingInterval, TickTimeout: s.SchedulingTimeout}).Validate()
}
func randomID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

type peer struct {
	Name        string            `json:"name"`
	Type        topology.NodeType `json:"type"`
	MemberID    uint64            `json:"member_id"`
	MetaAddress string            `json:"meta_address"`
	LoadAddress string            `json:"load_address"`
	HTTPAddress string            `json:"http_address"`
}
type Node struct {
	Spec      NodeSpec
	Container testcontainers.Container
	Volume    string
	mu        sync.RWMutex
	endpoint  string
	running   atomic.Bool
	cluster   *Cluster
}

func (n *Node) Endpoint() string { n.mu.RLock(); defer n.mu.RUnlock(); return n.endpoint }
func (n *Node) Start(ctx context.Context) error {
	owned, cancel, err := n.cluster.operation(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if err := n.Container.Start(owned); err != nil {
		return err
	}
	endpoint, err := n.Container.PortEndpoint(owned, "8080/tcp", "http")
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.endpoint = endpoint
	n.mu.Unlock()
	n.running.Store(true)
	return nil
}
func (n *Node) Stop(ctx context.Context) error {
	timeout := 5 * time.Second
	err := n.Container.Stop(ctx, &timeout)
	if err == nil {
		n.running.Store(false)
	}
	return err
}
func (n *Node) Kill(ctx context.Context) error {
	_, err := n.cluster.docker.ContainerKill(ctx, n.Container.GetContainerID(), docker.ContainerKillOptions{Signal: "SIGKILL"})
	if err == nil {
		n.running.Store(false)
	}
	return err
}

// StartJoining replaces only the stopped process container, retaining its exact
// private volume, host identity and lease catalog entry. Join is a caller-supplied
// constructor parameter; the library supplies the grant/fencing/quarantine flow.
func (n *Node) StartJoining(ctx context.Context) error {
	c := n.cluster
	c.creationMu.Lock()
	defer c.creationMu.Unlock()
	state, err := n.Container.State(ctx)
	if err != nil {
		return err
	}
	if state.Running {
		return errors.New("cannot change boot mode of a running process")
	}
	owned, cancel, err := c.operation(ctx)
	if err != nil {
		return err
	}
	err = n.Container.Terminate(owned, testcontainers.StopTimeout(2*time.Second))
	cancel()
	if err != nil {
		return err
	}
	n.running.Store(false)
	peers := make([]peer, len(c.Spec.Nodes))
	for i, seed := range c.Spec.Nodes {
		peers[i] = peer{Name: seed.Name, Type: seed.Type, MetaAddress: seed.Name + ":9000", LoadAddress: seed.Name + ":9001", HTTPAddress: seed.Name + ":8080"}
	}
	if err := c.createNodeContainer(ctx, n, c.image, peers, true, n.Spec.Name); err != nil {
		return err
	}
	return n.Start(ctx)
}
func (n *Node) Logs(ctx context.Context) (string, error) {
	reader, err := n.Container.Logs(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	return string(data), err
}

type Cluster struct {
	ID, Credential, Network, RunID string
	UnitID                         int
	Generation                     uint64
	Spec                           Spec
	mu                             sync.RWMutex
	nodes                          []*Node
	http                           *http.Client
	docker                         *testcontainers.DockerClient
	labels                         map[string]string
	resourcePrefix                 string
	operationTimeout               time.Duration
	uncertainCreate                error
	image                          string
	creationMu                     sync.Mutex
}

// Acquisitions can stop waiting promptly, while the lifecycle owner joins an
// admitted Docker mutation's acknowledgement before disposing its resources.
func (c *Cluster) operation(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	owned, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.operationTimeout)
	return owned, cancel, nil
}

func (c *Cluster) Nodes() []*Node { c.mu.RLock(); defer c.mu.RUnlock(); return slices.Clone(c.nodes) }
func (c *Cluster) Node(name string) *Node {
	for _, node := range c.Nodes() {
		if node.Spec.Name == name {
			return node
		}
	}
	return nil
}
func newCluster(ctx context.Context, p *Pool, u *deploymentUnit, spec Spec) (*Cluster, error) {
	spec = spec.normalized()
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	credential, err := randomID()
	if err != nil {
		return nil, err
	}
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("gquorum-test-%s-u%d-g%d", p.runID, u.id, u.generation)
	labels := maps.Clone(testcontainers.GenericLabels())
	labels["gquorum.test.run"] = p.runID
	labels["gquorum.test.unit"] = fmt.Sprint(u.id)
	labels["gquorum.test.generation"] = fmt.Sprint(u.generation)
	c := &Cluster{ID: "test-" + id, Credential: credential, Network: u.network.Name, RunID: p.runID, UnitID: u.id, Generation: u.generation, Spec: spec, http: &http.Client{Timeout: 6 * time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}, docker: cli, labels: labels, resourcePrefix: prefix, operationTimeout: p.opts.OperationTimeout, image: p.opts.Image}
	// Return the partial cluster on failure so its lifecycle owner can clean every
	// resource whose creation was acknowledged, including a failed-start node.
	peers := make([]peer, len(spec.Nodes))
	for i, node := range spec.Nodes {
		peers[i] = peer{Name: node.Name, Type: node.Type, MemberID: uint64(i + 1), MetaAddress: node.Name + ":9000", LoadAddress: node.Name + ":9001", HTTPAddress: node.Name + ":8080"}
	}
	for _, node := range spec.Nodes {
		if _, err := c.createNode(ctx, p.opts.Image, node, peers, false); err != nil {
			return c, err
		}
	}
	var wg sync.WaitGroup
	failures := make(chan error, len(spec.Nodes))
	for _, node := range c.Nodes() {
		if slices.Contains(spec.Deferred, node.Spec.Name) {
			continue
		}
		wg.Go(func() {
			if err := node.Start(ctx); err != nil {
				failures <- fmt.Errorf("start %s: %w", node.Spec.Name, err)
			}
		})
	}
	wg.Wait()
	close(failures)
	var bootErrors []error
	for err := range failures {
		bootErrors = append(bootErrors, err)
	}
	if err := errors.Join(bootErrors...); err != nil {
		return c, err
	}
	if !spec.SkipReady {
		if err := c.WaitReady(ctx); err != nil {
			return c, err
		}
	}
	return c, nil
}
func (c *Cluster) createNode(ctx context.Context, image string, spec NodeSpec, peers []peer, join bool) (*Node, error) {
	return c.createNodeClaiming(ctx, image, spec, peers, join, spec.Name)
}
func (c *Cluster) createNodeClaiming(ctx context.Context, image string, spec NodeSpec, peers []peer, join bool, claim string) (*Node, error) {
	volumeName := c.resourcePrefix + "-" + spec.Name
	owned, cancel, err := c.operation(ctx)
	if err != nil {
		return nil, err
	}
	created, err := c.docker.VolumeCreate(owned, docker.VolumeCreateOptions{Name: volumeName, Labels: c.labels})
	cancel()
	if err != nil {
		c.uncertainCreate = err
		return nil, err
	}
	node := &Node{Spec: spec, Volume: created.Volume.Name, cluster: c}
	c.mu.Lock()
	c.nodes = append(c.nodes, node)
	c.mu.Unlock()
	return node, c.createNodeContainer(ctx, node, image, peers, join, claim)
}
func (c *Cluster) createNodeContainer(ctx context.Context, node *Node, image string, peers []peer, join bool, claim string) error {
	spec := node.Spec
	volumeName := c.resourcePrefix + "-" + spec.Name
	manifest, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	ports := []string{"8080/tcp"}
	if c.Spec.PublishMeta {
		ports = append(ports, "9000/tcp")
	}
	request := testcontainers.ContainerRequest{
		Image: image, Name: volumeName, ExposedPorts: ports, Networks: []string{c.Network}, NetworkAliases: map[string][]string{c.Network: {spec.Name}}, Labels: maps.Clone(c.labels), Mounts: testcontainers.Mounts(testcontainers.VolumeMount(node.Volume, "/data")),
		Env:            map[string]string{"GQUORUM_NODE": claim, "GQUORUM_TYPE": string(spec.Type), "GQUORUM_CLUSTER": c.ID, "GQUORUM_CREDENTIAL": c.Credential, "GQUORUM_DATA": "/data", "GQUORUM_PEERS": string(manifest), "GQUORUM_META_ADDRESS": spec.Name + ":9000", "GQUORUM_LOAD_ADDRESS": spec.Name + ":9001", "GQUORUM_HTTP_ADDRESS": spec.Name + ":8080"},
		Cmd:            []string{"-gate-founders=" + strconv.FormatBool(c.Spec.GateFounders), "-founding-timeout=" + c.Spec.FoundingTimeout.String(), "-address-free=" + strconv.FormatBool(c.Spec.AddressFree), "-bootstrap=" + strconv.FormatBool(c.Spec.Bootstrap), "-join=" + strconv.FormatBool(join), "-max-nodes=" + strconv.Itoa(c.Spec.Limits.MaxNodes), "-max-replicas=" + strconv.Itoa(c.Spec.Limits.MaxReplicas), "-session-ttl=" + c.Spec.SessionTTL.String(), "-tick=" + c.Spec.Tick.String(), "-snapshot-threshold=" + strconv.FormatUint(c.Spec.SnapshotThreshold, 10), "-wal-write-wait=" + c.Spec.WALWriteWait.String(), "-disable-reconciliation=" + strconv.FormatBool(!c.Spec.Scheduling), "-reconciliation-interval=" + c.Spec.SchedulingInterval.String(), "-reconciliation-timeout=" + c.Spec.SchedulingTimeout.String()},
		WaitingFor:     wait.ForListeningPort("8080/tcp").SkipInternalCheck().WithStartupTimeout(20 * time.Second),
		ConfigModifier: func(config *containerapi.Config) { config.Hostname = spec.Name },
	}
	owned, cancel, err := c.operation(ctx)
	if err != nil {
		return err
	}
	node.Container, err = testcontainers.GenericContainer(owned, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: false})
	cancel()
	if err != nil && node.Container == nil {
		c.uncertainCreate = err
	}
	return err
}

// PrepareJoinNode allocates an owned, stopped process with fresh storage. The
// caller first approves its topology admission, then starts the exact process.
// Only the initial processes supply seeds; the joining node is no founder.
func (c *Cluster) PrepareJoinNode(ctx context.Context, spec NodeSpec) (*Node, error) {
	c.creationMu.Lock()
	defer c.creationMu.Unlock()
	if err := topology.ValidateID(spec.Name); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(spec.Type)) == "" || c.Node(spec.Name) != nil {
		return nil, errors.New("invalid or already allocated joining node")
	}
	peers := make([]peer, len(c.Spec.Nodes))
	for i, seed := range c.Spec.Nodes {
		peers[i] = peer{Name: seed.Name, Type: seed.Type, MetaAddress: seed.Name + ":9000", LoadAddress: seed.Name + ":9001", HTTPAddress: seed.Name + ":8080"}
	}
	return c.createNode(ctx, c.image, spec, peers, true)
}

// PrepareNameClaimingNode gives an unauthorized process its own host/volume
// while it claims an existing node identity. It never overwrites the resource
// catalog entry, DNS endpoint or storage of the legitimate holder.
func (c *Cluster) PrepareNameClaimingNode(ctx context.Context, host NodeSpec, claim string) (*Node, error) {
	c.creationMu.Lock()
	defer c.creationMu.Unlock()
	if err := topology.ValidateID(host.Name); err != nil {
		return nil, err
	}
	holder := c.Node(claim)
	if c.Node(host.Name) != nil || holder == nil || host.Type != holder.Spec.Type {
		return nil, errors.New("invalid conflict host or claimed identity")
	}
	peers := make([]peer, len(c.Spec.Nodes))
	for i, seed := range c.Spec.Nodes {
		peers[i] = peer{Name: seed.Name, Type: seed.Type, MetaAddress: seed.Name + ":9000", LoadAddress: seed.Name + ":9001", HTTPAddress: seed.Name + ":8080"}
	}
	return c.createNodeClaiming(ctx, c.image, host, peers, true, claim)
}
func (c *Cluster) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(60 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		ready := true
		for _, node := range c.Nodes() {
			if slices.Contains(c.Spec.Deferred, node.Spec.Name) {
				continue
			}
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
			if err != nil || response.Status != 200 {
				last = fmt.Errorf("node %s readiness: %w", node.Spec.Name, requireStatus(response, err, 200))
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster ready: %w", errors.Join(ctx.Err(), last))
		case <-ticker.C:
		}
	}
}

func (c *Cluster) diagnostics(ctx context.Context) string {
	var result strings.Builder
	for _, node := range c.Nodes() {
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/cluster/status", nil, 0)
		fmt.Fprintf(&result, "\n%s status: %s (%v)", node.Spec.Name, response.Data, err)
		if node.Container != nil {
			logs, err := node.Logs(ctx)
			if len(logs) > 16<<10 {
				logs = logs[len(logs)-(16<<10):]
			}
			fmt.Fprintf(&result, "\n%s logs: %s (%v)", node.Spec.Name, logs, err)
		}
	}
	return result.String()
}
func (c *Cluster) destroy(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.http.CloseIdleConnections()
	var wg sync.WaitGroup
	results := make(chan error, len(c.Nodes()))
	for _, node := range c.Nodes() {
		wg.Go(func() {
			if node.Container != nil {
				id := node.Container.GetContainerID()
				err := node.Container.Terminate(ctx, testcontainers.StopTimeout(2*time.Second))
				_, inspectErr := c.docker.ContainerInspect(ctx, id, docker.ContainerInspectOptions{})
				if !errdefs.IsNotFound(inspectErr) {
					results <- fmt.Errorf("container %s removal not verified: %w", id, errors.Join(err, inspectErr))
					return
				}
			}
			if node.Volume != "" {
				_, err := c.docker.VolumeRemove(ctx, node.Volume, docker.VolumeRemoveOptions{})
				_, inspectErr := c.docker.VolumeInspect(ctx, node.Volume, docker.VolumeInspectOptions{})
				if !errdefs.IsNotFound(inspectErr) {
					results <- fmt.Errorf("volume %s removal not verified: %w", node.Volume, errors.Join(err, inspectErr))
					return
				}
			}
		})
	}
	wg.Wait()
	close(results)
	var failures []error
	for err := range results {
		failures = append(failures, err)
	}
	// A canceled Docker create can succeed remotely without returning its ID.
	// Discover only this acknowledged lease namespace before returning a unit
	// to the pool; no unlabeled or other-generation resources are touched.
	if err := c.removeResidue(ctx); err != nil {
		return errors.Join(append(failures, err)...)
	}
	if c.uncertainCreate != nil {
		return fmt.Errorf("unit cannot be reused after an unacknowledged Docker create: %w", c.uncertainCreate)
	}
	c.mu.Lock()
	c.nodes = nil
	c.mu.Unlock()
	return c.docker.Close()
}

func (c *Cluster) resourceFilter() docker.Filters {
	return make(docker.Filters).Add("label", "gquorum.test.run="+c.RunID, "gquorum.test.unit="+fmt.Sprint(c.UnitID), "gquorum.test.generation="+fmt.Sprint(c.Generation))
}
func (c *Cluster) removeResidue(ctx context.Context) error {
	containers, err := c.docker.ContainerList(ctx, docker.ContainerListOptions{All: true, Filters: c.resourceFilter()})
	if err != nil {
		return err
	}
	for _, owned := range containers.Items {
		if owned.Labels["gquorum.test.run"] != c.RunID || owned.Labels["gquorum.test.unit"] != fmt.Sprint(c.UnitID) || owned.Labels["gquorum.test.generation"] != fmt.Sprint(c.Generation) {
			return errors.New("container namespace changed during cleanup")
		}
		if _, err := c.docker.ContainerRemove(ctx, owned.ID, docker.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}
	volumes, err := c.docker.VolumeList(ctx, docker.VolumeListOptions{Filters: c.resourceFilter()})
	if err != nil {
		return err
	}
	for _, owned := range volumes.Items {
		if owned.Labels["gquorum.test.run"] != c.RunID || owned.Labels["gquorum.test.unit"] != fmt.Sprint(c.UnitID) || owned.Labels["gquorum.test.generation"] != fmt.Sprint(c.Generation) {
			return errors.New("volume namespace changed during cleanup")
		}
		if _, err := c.docker.VolumeRemove(ctx, owned.Name, docker.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}
	containers, err = c.docker.ContainerList(ctx, docker.ContainerListOptions{All: true, Filters: c.resourceFilter()})
	if err != nil {
		return err
	}
	if len(containers.Items) != 0 {
		return errors.New("lease containers remain after cleanup")
	}
	volumes, err = c.docker.VolumeList(ctx, docker.VolumeListOptions{Filters: c.resourceFilter()})
	if err != nil {
		return err
	}
	if len(volumes.Items) != 0 {
		return errors.New("lease volumes remain after cleanup")
	}
	return nil
}
