package gquorum

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// NodeOptions configures one local participant and its managed replicas.
// Paths and networking are supplied by the application. Custom dependencies
// must remain stable for the Node lifetime; the event bus is borrowed.
type NodeOptions struct {
	// StorageDir contains the library-owned _meta.bolt and _authority.bolt files.
	// Retain it across restarts and readmission, outside all load retirement roots.
	// Applications select load storage separately through Loads.Storage.
	StorageDir string
	Identity   topology.Identity
	Node       topology.Node
	Bootstrap  BootstrapOptions
	Limits     topology.Limits
	Placement  topology.PlacementRule
	Meta       MetaOptions
	Loads      LoadOptions
	Policies   PolicyOptions
	Metrics    MetricsOptions
	Health     topology.HealthSource
	Events     *events.Bus
	Logger     *slog.Logger
}

type BootstrapOptions struct {
	Founders  []FoundingMember
	Join      bool
	JoinSeeds []string
}
type FoundingMember struct {
	Node          topology.Node
	MemberID      uint64
	Address       string
	PinnedLearner bool
}
type MetaOptions struct {
	// VoterPolicy applies only to fresh founding; nil selects the default.
	// Retained topology is changed through Cluster.SetGroupVoterPolicy.
	VoterPolicy       *topology.VoterPolicy
	Network           MetaNetworkOptions
	Reads             load.ReadOptions
	Timing            MetaTiming
	SessionTTL        time.Duration
	SnapshotThreshold uint64
	MaxWatchers       int
	Observation       ObservationOptions
}
type MetaNetworkOptions struct {
	// Listener transfers to Node when Start successfully acquires meta.
	Listener          net.Listener
	AdvertiseAddress  string
	Resolver          topology.AddressResolver
	TLS, ClientTLS    *tls.Config
	AllowPlaintext    bool
	ClusterCredential []byte
	MaxSnapshotBytes  int
}
type MetaTiming struct {
	TickInterval                   time.Duration
	ElectionTicks, HeartbeatTicks  int
	DialTimeout, SendTimeout       time.Duration
	PeerRetryInitial, PeerRetryMax time.Duration
}
type ObservationOptions struct{ Interval, Timeout time.Duration }

// GroupFactory acquires a machine for the owned, approved replica specification.
// On success, Node owns Application.Close. On failure, the factory releases any
// partial acquisitions. Callbacks must honor cancellation; they may inspect
// status or RequestStop, but must not wait for teardown that joins themselves.
type GroupFactory func(context.Context, load.ReplicaSpec) (load.Application, error)

// RetireReplicaFunc idempotently verifies and retires exact application state,
// including retries after prior cleanup. Return the entire matching proof with
// Verified=true. Callbacks must honor cancellation and may read status or call
// RequestStop; do not wait for their own group's lifecycle or Node.Stop.
type RetireReplicaFunc func(context.Context, load.RetirementRequest) (load.RetirementResult, error)

type LoadOptions struct {
	Factory                              GroupFactory
	Retire                               RetireReplicaFunc
	Transport                            load.Transport
	Storage                              LoadStorageOptions
	Engine                               load.EngineOptions
	EffectTimeout                        time.Duration
	RecoveryInterval, RetirementInterval time.Duration
}
type LoadStorageOptions struct {
	// Select owned WAL storage, or a borrowed Store with explicit Retirement.
	WAL        *WALOptions
	Store      storage.Store
	Retirement storage.RetirementStore
}
type WALOptions struct {
	RootDir      string
	SegmentBytes int64
	Prewrite     *bool
}
type PolicyOptions struct {
	Scheduler        policy.Scheduler
	RestoreDegraded  policy.RestoreDegradedOptions
	Scheduling       SchedulingOptions
	Lifecycle        policy.LifecyclePolicy
	DefaultLifecycle policy.DefaultLifecycleOptions
	CallbackTimeout  time.Duration
}
type SchedulingOptions struct {
	Disabled                    bool
	Interval, Timeout, Cooldown time.Duration
}

func DefaultMetaTiming() MetaTiming { return MetaTiming(metaservice.DefaultTiming()) }

// Validate performs no I/O and never invokes a caller dependency or policy.
// It validates all supplied durations even when scheduling is disabled.
func (o NodeOptions) Validate() error {
	_, _, err := resolveNodeOptions(o)
	return err
}

func resolveNodeOptions(input NodeOptions) (NodeOptions, metaservice.Options, error) {
	o := input
	if err := validateStorageDir(o); err != nil {
		return NodeOptions{}, metaservice.Options{}, err
	}
	invalid := func(message string) (NodeOptions, metaservice.Options, error) {
		return NodeOptions{}, metaservice.Options{}, fault.New(fault.Invalid, message, nil)
	}
	if o.Policies.CallbackTimeout < 0 || o.Loads.EffectTimeout < 0 || o.Loads.RecoveryInterval < 0 || o.Loads.RetirementInterval < 0 {
		return invalid("negative node callback or maintenance duration")
	}
	if o.Policies.Scheduler != nil && o.Policies.RestoreDegraded != (policy.RestoreDegradedOptions{}) {
		return invalid("custom scheduler cannot also configure RestoreDegraded")
	}
	if o.Policies.Lifecycle != nil && o.Policies.DefaultLifecycle != (policy.DefaultLifecycleOptions{}) {
		return invalid("custom lifecycle cannot also configure DefaultLifecycle")
	}
	var err error
	metricsSettings, err := internalmetrics.Settings(o.Metrics).Resolve()
	if err != nil {
		return NodeOptions{}, metaservice.Options{}, err
	}
	o.Metrics = MetricsOptions(metricsSettings)
	if o.Policies.Scheduler == nil {
		o.Policies.Scheduler, err = policy.NewRestoreDegraded(o.Policies.RestoreDegraded)
		if err != nil {
			return NodeOptions{}, metaservice.Options{}, err
		}
	}
	if o.Policies.Lifecycle == nil {
		o.Policies.Lifecycle, err = policy.NewDefaultLifecycle(o.Policies.DefaultLifecycle)
		if err != nil {
			return NodeOptions{}, metaservice.Options{}, err
		}
	}
	if o.Policies.CallbackTimeout == 0 {
		o.Policies.CallbackTimeout = 10 * time.Second
	}
	if err := validateLoadOptions(o.Loads); err != nil {
		return NodeOptions{}, metaservice.Options{}, err
	}
	if o.Loads.Storage.WAL != nil {
		owned := *o.Loads.Storage.WAL
		if owned.Prewrite != nil {
			v := *owned.Prewrite
			owned.Prewrite = &v
		}
		o.Loads.Storage.WAL = &owned
	}
	if o.Loads.Factory != nil {
		if o.Loads.EffectTimeout == 0 {
			o.Loads.EffectTimeout = 10 * time.Second
		}
		if o.Loads.RecoveryInterval == 0 {
			o.Loads.RecoveryInterval = 250 * time.Millisecond
		}
		if o.Loads.RetirementInterval == 0 {
			o.Loads.RetirementInterval = time.Minute
		}
	}
	m := metaservice.Options{
		Identity: o.Identity, Node: o.Node, DBPath: o.metaDBPath(),
		Join: o.Bootstrap.Join, JoinSeeds: o.Bootstrap.JoinSeeds,
		Limits: o.Limits, Placement: o.Placement,
		VoterPolicy: o.Meta.VoterPolicy, Reads: raftengine.ReadOptions(o.Meta.Reads),
		Timing: metaservice.Timing(o.Meta.Timing), SessionTTL: o.Meta.SessionTTL,
		SnapshotThreshold: o.Meta.SnapshotThreshold, MaxWatchers: o.Meta.MaxWatchers,
		Network:     metaservice.NetworkOptions(o.Meta.Network),
		Observation: metaservice.ObservationOptions(o.Meta.Observation),
		Metrics:     metricsSettings,
		Scheduling: metaservice.SchedulingOptions{
			Disabled: o.Policies.Scheduling.Disabled, Interval: o.Policies.Scheduling.Interval,
			TickTimeout: o.Policies.Scheduling.Timeout, Cooldown: o.Policies.Scheduling.Cooldown,
		},
		Scheduler: o.Policies.Scheduler, CallbackTimeout: o.Policies.CallbackTimeout,
		Health: o.Health, Events: o.Events, Logger: o.Logger,
	}
	for _, founder := range o.Bootstrap.Founders {
		m.Founders = append(m.Founders, metaservice.FoundingMember(founder))
	}
	if err := m.Validate(); err != nil {
		return NodeOptions{}, metaservice.Options{}, err
	}
	m = m.WithDefaults()
	o.Identity, o.Node, o.Limits, o.Placement, o.Logger = m.Identity, m.Node, m.Limits, m.Placement, m.Logger
	o.Bootstrap.Founders = make([]FoundingMember, len(m.Founders))
	for i, founder := range m.Founders {
		o.Bootstrap.Founders[i] = FoundingMember(founder)
	}
	o.Bootstrap.JoinSeeds = m.JoinSeeds
	o.Meta.Network, o.Meta.Timing = MetaNetworkOptions(m.Network), MetaTiming(m.Timing)
	o.Meta.SessionTTL, o.Meta.SnapshotThreshold, o.Meta.MaxWatchers = m.SessionTTL, m.SnapshotThreshold, m.MaxWatchers
	o.Meta.Observation = ObservationOptions(m.Observation)
	o.Meta.VoterPolicy = m.VoterPolicy
	o.Policies.Scheduling = SchedulingOptions{
		Disabled: m.Scheduling.Disabled, Interval: m.Scheduling.Interval,
		Timeout: m.Scheduling.TickTimeout, Cooldown: m.Scheduling.Cooldown,
	}
	return o, m, nil
}

func validateLoadOptions(o LoadOptions) error {
	invalid := func(message string) error { return fault.New(fault.Invalid, message, nil) }
	hasLoads := o.Factory != nil || o.Retire != nil || o.Transport != nil || o.Storage.WAL != nil || o.Storage.Store != nil || o.Storage.Retirement != nil || o.Engine != (load.EngineOptions{}) || o.EffectTimeout != 0 || o.RecoveryInterval != 0 || o.RetirementInterval != 0
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
		if strings.TrimSpace(o.Storage.WAL.RootDir) == "" || strings.ContainsRune(o.Storage.WAL.RootDir, 0) || o.Storage.WAL.SegmentBytes < 0 {
			return invalid("WAL requires a caller path and a nonnegative segment size")
		}
	} else if o.Storage.Store == nil || o.Storage.Retirement == nil {
		return invalid("borrowed load storage requires both Store and Retirement")
	}
	return (raftengine.Options{Reads: raftengine.ReadOptions(o.Engine.Reads), Timing: raftengine.Timing(o.Engine.Timing), MaxPendingProposals: o.Engine.MaxPendingProposals, ProposalTimeout: o.Engine.ProposalTimeout}).ValidateSettings()
}
