//revive:disable:max-public-structs Keep Node and subsystem options together for API navigation.
package gquorum

import (
	"bytes"
	"context"
	"crypto/tls"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultMaxWatchers      = 128
	defaultCallbackTimeout  = 10 * time.Second
	defaultEffectTimeout    = 10 * time.Second
	defaultRecoveryInterval = 250 * time.Millisecond
)

// NodeOptions configures one local participant and its managed replicas.
// Paths and networking are supplied by the application. Custom dependencies
// must remain stable for the Node lifetime; the event bus is borrowed. NewNode
// validates and copies these settings without acquiring resources.
type NodeOptions struct {
	// MaxWatchers bounds Node.Watch subscriptions; zero selects 128.
	MaxWatchers int
	// StorageDir contains the library-owned _meta.bolt and _authority.bolt
	// files. Retain it across restarts and readmission, outside all load
	// retirement roots.
	// Applications select load storage separately through Loads.Storage.
	StorageDir string

	// Participant identity, declaration, founding inputs and cluster-wide
	// limits.
	Identity  topology.Identity
	Node      topology.Node
	Bootstrap BootstrapOptions
	Limits    topology.Limits
	Placement topology.PlacementRule

	// Subsystem settings and caller-supplied dependencies.
	Meta       MetaOptions
	Loads      LoadOptions
	Scheduling scheduling.Options
	Lifecycle  lifecycle.Options
	// CallbackTimeout bounds finite application and decision callbacks; zero
	// selects ten seconds. It does not bound the lifetime of Application.Run or
	// leadership-scoped work.
	CallbackTimeout time.Duration
	Metrics         MetricsOptions
	Health          topology.HealthSource
	Events          *events.Bus
	Logger          *slog.Logger
}

// BootstrapOptions selects founding members or join seeds for this participant.
// Join and Founders describe different startup paths; retained identity is
// recovered from StorageDir rather than inferred from these inputs.
type BootstrapOptions struct {
	// Founders identifies the initial _meta members during fresh founding.
	Founders []FoundingMember
	// Join requests admission to an established cluster using JoinSeeds.
	Join      bool
	JoinSeeds []string
}

// FoundingMember declares a fresh meta member's node, Raft identity and
// endpoint.
type FoundingMember struct {
	Node          topology.Node
	MemberID      uint64
	Address       string
	PinnedLearner bool
}

// MetaOptions configures the local _meta replica, reads, sessions and
// observation. Membership changes after founding use Cluster topology edits.
type MetaOptions struct {
	// VoterConfig applies only to fresh founding; nil selects the default.
	// Retained topology is changed through Cluster.SetGroupVoterConfig.
	VoterConfig *topology.VoterConfig

	// Network, read and Raft timing settings.
	Network MetaNetworkOptions
	Reads   load.ReadOptions
	Timing  MetaTiming

	// Session, snapshot, watch and observation settings.
	SessionTTL        time.Duration
	SnapshotThreshold uint64
	MaxWatchers       int
	Observation       ObservationOptions
}

// MetaNetworkOptions configures _meta transport and peer discovery. Listener
// ownership transfers to Node only when startup acquires the meta service.
type MetaNetworkOptions struct {
	// Listener transfers to Node when Start successfully acquires meta.
	Listener net.Listener
	// AdvertiseAddress and Resolver identify reachable _meta peers.
	// Resolver must work from local information before election and during quorum loss.
	AdvertiseAddress string
	Resolver         topology.AddressResolver
	// TLS settings, plaintext opt-in and the shared cluster credential protect
	// meta transport; MaxSnapshotBytes bounds incoming snapshots.
	TLS, ClientTLS    *tls.Config
	AllowPlaintext    bool
	ClusterCredential []byte
	MaxSnapshotBytes  int
}

// MetaTiming configures _meta Raft ticks and peer communication timeouts.
// Zero-valued settings use the library defaults during option resolution.
type MetaTiming struct {
	// TickInterval drives Raft; ElectionTicks and HeartbeatTicks count its
	// ticks.
	TickInterval                  time.Duration
	ElectionTicks, HeartbeatTicks int
	// DialTimeout and SendTimeout bound peer calls; retry bounds reconnects.
	DialTimeout, SendTimeout       time.Duration
	PeerRetryInitial, PeerRetryMax time.Duration
}

// ObservationOptions bounds the interval and timeout of peer observations.
type ObservationOptions struct{ Interval, Timeout time.Duration }

// GroupFactory acquires a machine for the owned, approved replica
// specification.
// On success, Node owns Application.Close. On failure, the factory releases any
// partial acquisitions. Callbacks must honor cancellation; they may inspect
// status or RequestStop, but must not wait for teardown that joins themselves.
type GroupFactory func(context.Context, load.ReplicaSpec) (load.Application, error)

// RetireReplicaFunc idempotently verifies and retires exact application state,
// including retries after prior cleanup. Return the entire matching proof with
// Verified=true. Callbacks must honor cancellation and may read status or call
// RequestStop; do not wait for their own group's lifecycle or Node.Stop.
type RetireReplicaFunc func(context.Context, load.RetirementRequest) (load.RetirementResult, error)

// LoadOptions supplies managed application factories, transport, storage and
// engine settings.
type LoadOptions struct {
	Factory                              GroupFactory
	Retire                               RetireReplicaFunc
	Transport                            load.Transport
	Storage                              LoadStorageOptions
	Engine                               load.EngineOptions
	EffectTimeout                        time.Duration
	RecoveryInterval, RetirementInterval time.Duration
}

// LoadStorageOptions selects Node-owned WAL storage or borrowed storage and
// retirement interfaces.
type LoadStorageOptions struct {
	// Select owned WAL storage, or a borrowed Store with explicit Retirement.
	WAL        *WALOptions
	Store      storage.Store
	Retirement storage.RetirementStore
}

// WALOptions configures the caller-selected load storage root and WAL writes.
type WALOptions struct {
	RootDir      string
	SegmentBytes int64
	Prewrite     *bool
	// DisableFSync skips fsync for WAL and applied-state writes. Writes still
	// reach the OS, but host crashes can lose acknowledged data or corrupt
	// state. Snapshot files, identity pins and directory synchronization are
	// unchanged.
	DisableFSync bool
}

// DefaultMetaTiming returns the library's default meta Raft and transport
// timing.
func DefaultMetaTiming() MetaTiming { return MetaTiming(metaservice.DefaultTiming()) }

// Validate performs no I/O and never invokes caller dependencies or decision
// callbacks.
// It validates all supplied durations even when scheduling is disabled.
func (o NodeOptions) Validate() error {
	_, err := resolveNodeOptions(o)
	return err
}

type resolvedNodeOptions struct {
	node NodeOptions
	meta metaservice.Options
}

func resolveNodeOptions(input NodeOptions) (resolvedNodeOptions, error) {
	o := input
	if o.MaxWatchers < 0 {
		return resolvedNodeOptions{}, fault.New(fault.Invalid, "negative node watcher bound", nil)
	}
	if o.MaxWatchers == 0 {
		o.MaxWatchers = defaultMaxWatchers
	}
	if err := validateNodeSettings(o); err != nil {
		return resolvedNodeOptions{}, err
	}
	var err error
	metricsSettings, err := internalmetrics.Settings(o.Metrics).Resolve()
	if err != nil {
		return resolvedNodeOptions{}, err
	}
	o.Metrics = MetricsOptions(metricsSettings)
	if o.Scheduling.Policy == nil {
		o.Scheduling.Policy, err = scheduling.NewDefaultRepairPolicy(o.Scheduling.DefaultRepair)
		if err != nil {
			return resolvedNodeOptions{}, err
		}
	}
	if o.Lifecycle.Hooks == nil {
		o.Lifecycle.Hooks, err = lifecycle.NewDefaultHooks(o.Lifecycle.Default)
		if err != nil {
			return resolvedNodeOptions{}, err
		}
	}
	if o.CallbackTimeout == 0 {
		o.CallbackTimeout = defaultCallbackTimeout
	}
	if err := validateLoadOptions(o.Loads); err != nil {
		return resolvedNodeOptions{}, err
	}
	o.Loads = resolveLoadDefaults(o.Loads)
	m := metaservice.Options{
		Identity: o.Identity, Node: o.Node, DBPath: o.metaDBPath(),
		Join: o.Bootstrap.Join, JoinSeeds: o.Bootstrap.JoinSeeds,
		Limits: o.Limits, Placement: o.Placement,
		VoterConfig: o.Meta.VoterConfig, Reads: raftengine.ReadOptions(o.Meta.Reads),
		Timing: metaservice.Timing(o.Meta.Timing), SessionTTL: o.Meta.SessionTTL,
		SnapshotThreshold: o.Meta.SnapshotThreshold, MaxWatchers: o.Meta.MaxWatchers,
		Network:     metaservice.NetworkOptions(o.Meta.Network),
		Observation: metaservice.ObservationOptions(o.Meta.Observation),
		Metrics:     metricsSettings,
		Scheduling: metaservice.SchedulingOptions{
			Disabled: o.Scheduling.Disabled, Interval: o.Scheduling.Interval,
			TickTimeout: o.Scheduling.Timeout, Cooldown: o.Scheduling.Cooldown,
		},
		RepairPolicy: o.Scheduling.Policy, CallbackTimeout: o.CallbackTimeout,
		Health: o.Health, Events: o.Events, Logger: o.Logger,
	}
	for _, founder := range o.Bootstrap.Founders {
		m.Founders = append(m.Founders, metaservice.FoundingMember(founder))
	}
	if err := m.Validate(); err != nil {
		return resolvedNodeOptions{}, err
	}
	m = m.WithDefaults()
	o.applyMetaDefaults(m)

	return resolvedNodeOptions{node: o, meta: m}, nil
}

func validateNodeSettings(o NodeOptions) error {
	if err := validateStorageDir(o); err != nil {
		return err
	}
	invalid := func(message string) error {
		return fault.New(fault.Invalid, message, nil)
	}
	if o.CallbackTimeout < 0 || o.Loads.EffectTimeout < 0 || o.Loads.RecoveryInterval < 0 ||
		o.Loads.RetirementInterval < 0 {
		return invalid("negative node callback or maintenance duration")
	}
	if o.Scheduling.Policy != nil && o.Scheduling.DefaultRepair != (scheduling.DefaultRepairOptions{}) {
		return invalid("custom repair policy cannot also configure DefaultRepair")
	}
	if o.Lifecycle.Hooks != nil && o.Lifecycle.Default != (lifecycle.DefaultOptions{}) {
		return invalid("custom lifecycle hooks cannot also configure Default")
	}

	return nil
}

func resolveLoadDefaults(loads LoadOptions) LoadOptions {
	if loads.Storage.WAL != nil {
		owned := *loads.Storage.WAL
		if owned.Prewrite != nil {
			v := *owned.Prewrite
			owned.Prewrite = &v
		}
		loads.Storage.WAL = &owned
	}
	if loads.Factory != nil {
		if loads.EffectTimeout == 0 {
			loads.EffectTimeout = defaultEffectTimeout
		}
		if loads.RecoveryInterval == 0 {
			loads.RecoveryInterval = defaultRecoveryInterval
		}
		if loads.RetirementInterval == 0 {
			loads.RetirementInterval = time.Minute
		}
	}

	return loads
}

func (o *NodeOptions) applyMetaDefaults(m metaservice.Options) {
	o.Identity, o.Node, o.Limits, o.Placement, o.Logger = m.Identity, m.Node, m.Limits, m.Placement, m.Logger
	o.Bootstrap.Founders = make([]FoundingMember, len(m.Founders))
	for i, founder := range m.Founders {
		o.Bootstrap.Founders[i] = FoundingMember(founder)
	}
	o.Bootstrap.JoinSeeds = m.JoinSeeds
	o.Meta.Network, o.Meta.Timing = MetaNetworkOptions(m.Network), MetaTiming(m.Timing)
	o.Meta.SessionTTL, o.Meta.SnapshotThreshold, o.Meta.MaxWatchers = m.SessionTTL, m.SnapshotThreshold, m.MaxWatchers
	o.Meta.Observation = ObservationOptions(m.Observation)
	o.Meta.VoterConfig = m.VoterConfig
	o.Scheduling.Disabled = m.Scheduling.Disabled
	o.Scheduling.Interval = m.Scheduling.Interval
	o.Scheduling.Timeout = m.Scheduling.TickTimeout
	o.Scheduling.Cooldown = m.Scheduling.Cooldown
}

func validateLoadOptions(o LoadOptions) error {
	invalid := func(message string) error { return fault.New(fault.Invalid, message, nil) }
	hasLoads := o.Factory != nil || o.Retire != nil || o.Transport != nil || o.Storage.WAL != nil ||
		o.Storage.Store != nil || o.Storage.Retirement != nil || o.Engine != (load.EngineOptions{}) ||
		o.EffectTimeout != 0 || o.RecoveryInterval != 0 || o.RetirementInterval != 0
	if !hasLoads {
		return nil
	}
	if o.Factory == nil || o.Transport == nil {
		return invalid("managed loads require Factory and Transport")
	}
	if o.Storage.WAL != nil {
		if o.Storage.Store != nil || o.Storage.Retirement != nil {
			return invalid("select WAL or borrowed Store with Retirement")
		}
		if o.Storage.WAL.SegmentBytes < 0 || strings.TrimSpace(o.Storage.WAL.RootDir) == "" ||
			strings.ContainsRune(o.Storage.WAL.RootDir, 0) {
			return invalid("WAL requires a caller path and a nonnegative segment size")
		}
	} else if o.Storage.Store == nil || o.Storage.Retirement == nil {
		return invalid("borrowed load storage requires both Store and Retirement")
	}
	return (raftengine.Options{MaxWatchers: o.Engine.MaxWatchers, Reads: raftengine.ReadOptions(
		o.Engine.Reads), Timing: raftengine.Timing(
		o.Engine.Timing), MaxPendingProposals: o.Engine.MaxPendingProposals,
		ProposalTimeout: o.Engine.ProposalTimeout}).ValidateSettings()
}

// MetricsOptions bounds local sampling and cluster observation. Zero fields
// select defaults. Raw history is process-local and is lost on restart.
type MetricsOptions struct {
	SampleInterval   time.Duration // default 1s
	CollectInterval  time.Duration // default 5s
	CollectTimeout   time.Duration // default 2s per metrics round or native load status read
	MaxSampleAge     time.Duration // default 15s
	HistoryRetention time.Duration // default 10m
	MaxHistoryPoints int           // default 600 per series
	MaxParallelReads int           // default 8 per pool; independent collector and native load status pools
}

func cloneNodeOptions(opts NodeOptions) NodeOptions {
	out := opts
	out.Node.Labels = maps.Clone(opts.Node.Labels)
	out.Placement = opts.Placement.Clone()
	out.Bootstrap.JoinSeeds = slices.Clone(opts.Bootstrap.JoinSeeds)
	out.Bootstrap.Founders = slices.Clone(opts.Bootstrap.Founders)
	for i := range out.Bootstrap.Founders {
		out.Bootstrap.Founders[i].Node.Labels = maps.Clone(opts.Bootstrap.Founders[i].Node.Labels)
	}
	if opts.Loads.Storage.WAL != nil {
		wal := *opts.Loads.Storage.WAL
		if wal.Prewrite != nil {
			value := *wal.Prewrite
			wal.Prewrite = &value
		}
		out.Loads.Storage.WAL = &wal
	}
	if opts.Meta.VoterConfig != nil {
		config := *opts.Meta.VoterConfig
		out.Meta.VoterConfig = &config
	}
	out.Meta.Network.ClusterCredential = bytes.Clone(opts.Meta.Network.ClusterCredential)
	if opts.Meta.Network.TLS != nil {
		out.Meta.Network.TLS = opts.Meta.Network.TLS.Clone()
	}
	if opts.Meta.Network.ClientTLS != nil {
		out.Meta.Network.ClientTLS = opts.Meta.Network.ClientTLS.Clone()
	}
	return out
}
