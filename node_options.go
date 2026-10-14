package gquorum

import (
	"log/slog"
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
