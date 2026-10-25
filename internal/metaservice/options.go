// Package metaservice implements the node's private _meta topology service.
package metaservice

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/internal/raftengine"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	internalscheduling "github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultTickInterval      = 100 * time.Millisecond
	defaultElectionTicks     = 10
	defaultDialTimeout       = 500 * time.Millisecond
	defaultSendTimeout       = 500 * time.Millisecond
	defaultPeerRetryInitial  = 100 * time.Millisecond
	defaultPeerRetryMax      = 5 * time.Second
	defaultSessionTTL        = 15 * time.Second
	defaultSnapshotThreshold = 512
	defaultMaxWatchers       = 256
	defaultMaxSnapshotBytes  = 64 << 20
	minimumTickInterval      = 10 * time.Millisecond
)

// Options supplies private meta identity, storage, networking and orchestration
// dependencies.
type Options struct {
	ObserveRecoveryCommand func(metaengine.ProposalKind, error)
	RecoveryResumeFounding bool // Node proved pin-only B with no completed metadata acquisition

	Recovery *model.LocalManifest // accepted private Node startup scope; never a general API request
	Identity topology.Identity
	// Node is the caller-defined local admission document. Founders may instead
	// supply it in the matching FoundingMember; joiners must supply its type
	// here.
	Node                 topology.Node
	DBPath               string
	Founders             []FoundingMember
	JoinSeeds            []string
	Join                 bool
	Limits               topology.Limits
	Placement            topology.PlacementRule
	VoterConfig          *topology.VoterConfig
	Reads                raftengine.ReadOptions
	Timing               Timing
	SessionTTL           time.Duration
	SnapshotThreshold    uint64
	MaxWatchers          int
	Network              NetworkOptions
	LoadGroups           operator.GroupEffects
	InstallLocalFence    func(context.Context, load.FenceRequest) (load.FenceResult, error)
	ApplyLocalMetaAction func(context.Context, load.ActionRequest) (load.ActionResult, error)
	LoadMetricsReads     func(context.Context, []topology.GroupProgress) []observe.ReplicaRead
	LocalLoadMetrics     func() []metrics.ReplicaObservation
	Health               topology.HealthSource
	Scheduling           SchedulingOptions
	RepairPolicy         scheduling.RepairPolicy
	CallbackTimeout      time.Duration
	HeldLoads            func() []storage.Identity
	Observation          ObservationOptions
	Metrics              observe.MetricsSettings
	Events               *events.Bus
	Logger               *slog.Logger
	// OnSelfRemoved is invoked asynchronously after durable approved removal.
	// It must honor cancellation and return before Stop/Quiesce can join it.
	// Use Group.RequestStop to schedule teardown instead of waiting on this
	// same group's Stop/Quiesce inside the callback. Callbacks are idempotent
	// across crashes/restarts and must verify the reason's exact local
	// identity.
	OnSelfRemoved       func(context.Context, string) error
	OnReadmission       func(context.Context, Readmission) error
	ReadmissionIdentity func() topology.Identity
	ReadmissionReceipts func(context.Context, Readmission) ([]topology.EffectReceipt, error)
	// Node owns final closure after managed application resources have joined.
	// BeforeOpenEngine acquires Node-owned authority under the exact accepted
	// identity, before creating a metadata pin. It is an internal owner hook.
	BeforeOpenEngine func(topology.Identity) error
	// AfterOpenEngine records acquisition after the pin is durable, before
	// publication or participation. Failure closes the store without starting
	// Raft.
	AfterOpenEngine  func(topology.Identity) error
	RequestOwnerStop func()
	OnApplied        func()
}

// SchedulingOptions controls local automatic load-replica repair. Disabled
// is an explicit opt-out. Zero timings use a 30s interval, 20s tick timeout,
// 60s legacy stable-node window and 5m action cooldown. Configured groups,
// including _meta, support guarded voter changes; unrelated staged intent
// is never approved by the scheduler.
type SchedulingOptions = internalscheduling.Settings

// ObservationOptions controls node/group health sampling. Defaults are a 5s
// interval and 2s timeout. Document application also wakes the sampler.
type ObservationOptions = observe.Settings

func (e *ReadmissionRequiredError) Error() string {
	return fmt.Sprintf("readmission requires local orchestration for %s: member %d to %d",
		e.Readmission.Granted.NodeName, e.Readmission.Previous.MemberID, e.Readmission.Granted.MemberID)
}

func (*ReadmissionRequiredError) Unwrap() error { return fault.ErrReadmissionRequired }

// DefaultTiming returns the library's default meta Raft and transport timings.
func DefaultTiming() Timing {
	return Timing{
		TickInterval:     defaultTickInterval,
		ElectionTicks:    defaultElectionTicks,
		HeartbeatTicks:   1,
		DialTimeout:      defaultDialTimeout,
		SendTimeout:      defaultSendTimeout,
		PeerRetryInitial: defaultPeerRetryInitial,
		PeerRetryMax:     defaultPeerRetryMax,
	}
}

// WithDefaults returns owned settings without acquiring resources or invoking
// dependencies. The public Node constructor shares this normalization path.
func (o Options) WithDefaults() Options { return o.defaults() }

func (o Options) defaults() Options {
	if o.Recovery != nil {
		owned := model.CloneManifest(*o.Recovery)
		o.Recovery = &owned
	}
	o.Metrics, _ = o.Metrics.Resolve()
	o.Limits = o.Limits.WithDefaults()
	o.Scheduling = o.Scheduling.WithDefaults()
	o.Observation = o.Observation.WithDefaults()
	if o.VoterConfig != nil {
		voterConfig := *o.VoterConfig
		o.VoterConfig = &voterConfig
	}
	o.Timing = o.Timing.withDefaults()
	if o.SessionTTL == 0 {
		o.SessionTTL = defaultSessionTTL
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = defaultSnapshotThreshold
	}
	if o.MaxWatchers == 0 {
		o.MaxWatchers = defaultMaxWatchers
	}
	if o.Network.MaxSnapshotBytes == 0 {
		o.Network.MaxSnapshotBytes = defaultMaxSnapshotBytes
	}
	if o.Placement.MinReplicas == 0 && o.Placement.MaxReplicas == 0 {
		o.Placement.MinReplicas = 1
		o.Placement.MaxReplicas = o.Limits.MaxReplicas
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	o = o.ownedDeclarations()
	return o
}

func (t Timing) withDefaults() Timing {
	d := DefaultTiming()
	if t.TickInterval == 0 {
		t.TickInterval = d.TickInterval
	}
	if t.ElectionTicks == 0 {
		t.ElectionTicks = d.ElectionTicks
	}
	if t.HeartbeatTicks == 0 {
		t.HeartbeatTicks = d.HeartbeatTicks
	}
	if t.DialTimeout == 0 {
		t.DialTimeout = d.DialTimeout
	}
	if t.SendTimeout == 0 {
		t.SendTimeout = d.SendTimeout
	}
	if t.PeerRetryInitial == 0 {
		t.PeerRetryInitial = d.PeerRetryInitial
	}
	if t.PeerRetryMax == 0 {
		t.PeerRetryMax = d.PeerRetryMax
	}
	return t
}

func (o Options) ownedDeclarations() Options {
	o.Founders = append([]FoundingMember(nil), o.Founders...)
	for i := range o.Founders {
		member := &o.Founders[i]
		owned := (topology.Document{Nodes: []topology.Node{member.Node}}).Clone()
		member.Node = owned.Nodes[0]
		if member.Node.State == "" {
			member.Node.State = topology.NodeActive
		}
		if member.Node.Name == o.Identity.NodeName {
			if o.Node.Name == "" && o.Node.Type == "" {
				o.Node = member.Node
			}
			if o.Identity.MemberID == 0 && !o.Join {
				o.Identity.MemberID = member.MemberID
			}
		}
	}
	owned := (topology.Document{Nodes: []topology.Node{o.Node}}).Clone()
	o.Node = owned.Nodes[0]
	if o.Node.Name == "" {
		o.Node.Name = o.Identity.NodeName
	}
	if o.Node.State == "" {
		o.Node.State = topology.NodeActive
	}
	o.JoinSeeds = append([]string(nil), o.JoinSeeds...)
	o.Placement.AllowedNodeTypes = append([]topology.NodeType(nil), o.Placement.AllowedNodeTypes...)
	o.Placement.NodeSelector = maps.Clone(o.Placement.NodeSelector)
	o.Network.ClusterCredential = bytes.Clone(o.Network.ClusterCredential)
	o.Network.TLS = cloneTLS(o.Network.TLS)
	o.Network.ClientTLS = cloneTLS(o.Network.ClientTLS)
	slices.SortFunc(o.Founders, func(a, b FoundingMember) int { return cmp.Compare(a.Node.Name, b.Node.Name) })
	return o
}

// Validate checks meta identity, startup declarations, durations and
// dependencies without acquisition.
func (o Options) Validate() error {
	if err := o.validateSuppliedSettings(); err != nil {
		return err
	}
	resolved := o.defaults()
	if err := resolved.validateDeclaration(); err != nil {
		return err
	}
	if err := resolved.validateTiming(); err != nil {
		return err
	}
	if err := resolved.validateNetwork(); err != nil {
		return err
	}
	return resolved.validateFounders()
}

func invalidMetaOptions(message string) error { return fault.New(fault.Invalid, message, nil) }

func (o Options) validateSuppliedSettings() error {
	if err := o.Reads.Validate(); err != nil {
		return err
	}
	if _, err := o.Metrics.Resolve(); err != nil {
		return err
	}
	if o.CallbackTimeout < 0 {
		return invalidMetaOptions("negative repair callback timeout")
	}
	if err := o.Scheduling.Validate(); err != nil {
		return err
	}
	if err := o.Observation.Validate(); err != nil {
		return err
	}
	return o.validateIdentityAndBounds()
}

func (o Options) validateIdentityAndBounds() error {
	if strings.TrimSpace(o.Identity.NodeName) == "" || strings.TrimSpace(o.Identity.ClusterID) == "" ||
		strings.TrimSpace(o.Identity.InstanceID) == "" || strings.ContainsRune(o.Identity.NodeName, 0) ||
		strings.ContainsRune(o.Identity.ClusterID, 0) || strings.ContainsRune(o.Identity.InstanceID, 0) ||
		strings.TrimSpace(o.DBPath) == "" {
		return invalidMetaOptions("meta identity and caller database path are required")
	}
	if o.Timing.TickInterval < 0 || o.Timing.ElectionTicks < 0 || o.Timing.HeartbeatTicks < 0 ||
		o.Timing.DialTimeout < 0 || o.Timing.SendTimeout < 0 || o.Timing.PeerRetryInitial < 0 ||
		o.Timing.PeerRetryMax < 0 || o.SessionTTL < 0 || o.MaxWatchers < 0 || o.Network.MaxSnapshotBytes < 0 {
		return invalidMetaOptions("negative meta timing or bound")
	}
	return nil
}

func (o Options) validateDeclaration() error {
	if err := o.Limits.Validate(); err != nil {
		return err
	}
	if o.VoterConfig != nil {
		if err := o.VoterConfig.Validate(1, o.Limits.MaxNodes); err != nil {
			return err
		}
	}
	if o.Node.Name != o.Identity.NodeName || o.Node.State != topology.NodeActive ||
		strings.TrimSpace(string(o.Node.Type)) == "" {
		return invalidMetaOptions("local admission requires matching node name, user-defined type and active state")
	}
	declaration := topology.Document{
		Version:   1,
		ClusterID: o.Identity.ClusterID,
		Placement: o.Placement,
		Nodes:     []topology.Node{o.Node},
		Groups: []topology.Group{
			{
				ID:            topology.MetaGroupID,
				NextReplicaID: 2,
				Replicas: []topology.Replica{{NodeName: o.Node.Name,
					ReplicaID: 1, Role: topology.ReplicaVoter}},
			},
		},
	}
	return declaration.Validate(topology.Rules{SchemaVersion: 1, Limits: o.Limits})
}

func (o Options) validateTiming() error {
	if o.Timing.TickInterval < minimumTickInterval || o.Timing.ElectionTicks < 2 ||
		o.Timing.HeartbeatTicks >= o.Timing.ElectionTicks ||
		o.Timing.PeerRetryMax < o.Timing.PeerRetryInitial ||
		int64(o.Timing.ElectionTicks) > math.MaxInt64/int64(o.Timing.TickInterval) {
		return invalidMetaOptions("invalid meta Raft timing")
	}
	if o.SessionTTL < time.Second || o.SessionTTL%time.Second != 0 ||
		o.SessionTTL/2 <= o.Timing.TickInterval*time.Duration(o.Timing.ElectionTicks) {
		return invalidMetaOptions("session TTL must be whole seconds and exceed two election intervals")
	}
	return nil
}

func (o Options) validateNetwork() error {
	if o.Network.Listener == nil || o.Network.AdvertiseAddress == "" ||
		len(o.Network.ClusterCredential) == 0 {
		return invalidMetaOptions("meta listener, advertised address and cluster credential are required")
	}
	if !o.Network.AllowPlaintext && (o.Network.TLS == nil || o.Network.ClientTLS == nil) {
		return invalidMetaOptions("plaintext meta networking must be explicitly enabled")
	}
	if _, _, err := net.SplitHostPort(o.Network.AdvertiseAddress); err != nil {
		return invalidMetaOptions("invalid advertised meta address")
	}
	if o.Join && len(o.JoinSeeds) == 0 {
		return invalidMetaOptions("joining requires seed addresses")
	}
	for _, seed := range o.JoinSeeds {
		if _, _, err := net.SplitHostPort(seed); err != nil {
			return invalidMetaOptions("invalid join seed address")
		}
	}
	if o.Join && len(o.Founders) > 0 {
		return invalidMetaOptions("joining cannot declare founding voters")
	}
	return nil
}

func (o Options) validateFounders() error {
	names := make(map[string]bool)
	ids := make(map[uint64]bool)
	doc := topology.Document{
		Version:   1,
		ClusterID: o.Identity.ClusterID,
		Placement: o.Placement,
		Groups:    []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 1, VoterConfig: o.VoterConfig}},
	}
	roles := o.legacyFoundingRoles()
	for _, founder := range o.Founders {
		if names[founder.Node.Name] || ids[founder.MemberID] || founder.MemberID == 0 ||
			founder.MemberID == math.MaxUint64 || founder.Node.State != topology.NodeActive {
			return invalidMetaOptions("founder names and positive member IDs must be unique")
		}
		if _, _, err := net.SplitHostPort(founder.Address); err != nil {
			return invalidMetaOptions("invalid founder meta address")
		}
		names[founder.Node.Name] = true
		ids[founder.MemberID] = true
		doc.Nodes = append(doc.Nodes, founder.Node)
		if err := doc.SetMemberID(founder.Node.Name, founder.MemberID, time.Time{}); err != nil {
			return err
		}
		replica := &doc.MetaGroup().Replicas[len(doc.MetaGroup().Replicas)-1]
		replica.Role, replica.PinnedLearner = roles[founder.Node.Name], founder.PinnedLearner
		if err := o.validateLocalFounder(founder); err != nil {
			return err
		}
	}
	if len(o.Founders) > 0 {
		if !names[o.Identity.NodeName] {
			return invalidMetaOptions("local node is absent from founders")
		}
		if err := doc.Validate(topology.Rules{SchemaVersion: 1, Limits: o.Limits}); err != nil {
			return err
		}
	}
	// A restart may omit founders: New verifies the existing pinned store
	// before constructing an engine, and refuses an empty store without
	// founding peers.
	return nil
}

func (o Options) validateLocalFounder(founder FoundingMember) error {
	if founder.Node.Name == o.Identity.NodeName && founder.MemberID != o.Identity.MemberID {
		return invalidMetaOptions("local identity differs from founder declaration")
	}
	if founder.Node.Name == o.Identity.NodeName && founder.Node.Type != o.Node.Type {
		return invalidMetaOptions("local node type differs from founder declaration")
	}
	return nil
}

// legacyFoundingRoles supplies only structural validation before retained
// storage tells us whether this is a fresh founding or a restart.
func (o Options) legacyFoundingRoles() map[string]topology.ReplicaRole {
	names := make([]string, 0, len(o.Founders))
	pinned := make(map[string]bool, len(o.Founders))
	for _, f := range o.Founders {
		names = append(names, f.Node.Name)
		pinned[f.Node.Name] = f.PinnedLearner
	}
	return topology.FoundingRoles(names, pinned, o.freshVoterConfig().TargetVoters)
}

// selectFoundingRoles applies the same document-aware placement decision to
// fresh meta peers and the initial desired topology.
func (o Options) selectFoundingRoles() (map[string]topology.ReplicaRole, error) {
	voterConfig := o.freshVoterConfig()
	doc := topology.Document{
		Version:   1,
		ClusterID: o.Identity.ClusterID,
		Placement: o.Placement,
		Groups:    []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 1, VoterConfig: &voterConfig}},
	}
	pinned := make(map[string]bool, len(o.Founders))
	for _, founder := range o.Founders {
		doc.Nodes = append(doc.Nodes, founder.Node)
		pinned[founder.Node.Name] = founder.PinnedLearner
	}
	return topology.SelectFoundingRoles(doc, topology.MetaGroupID, pinned)
}

// A constructor default is fresh-founding intent, never a reinterpretation of
// retained topology. Validate it only after storage has proven fresh founding.
func (o Options) freshVoterConfig() topology.VoterConfig {
	if o.VoterConfig != nil {
		return *o.VoterConfig
	}
	return topology.DefaultVoterConfig()
}

// FoundingIDs derives stable positive IDs independently of peer ordering.
func FoundingIDs(clusterID string, names []string) (map[string]uint64, error) {
	if len(names) == 0 || strings.TrimSpace(clusterID) == "" {
		return nil, fault.ErrInvalid
	}
	result := make(map[string]uint64, len(names))
	used := make(map[uint64]bool)
	for _, name := range names {
		if strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) {
			return nil, fault.ErrInvalid
		}
		if _, exists := result[name]; exists {
			return nil, fault.ErrInvalid
		}
		hash := sha256.Sum256([]byte(clusterID + "\x00" + name))
		id := binary.BigEndian.Uint64(hash[:8])
		if id == 0 || id == math.MaxUint64 || used[id] {
			return nil, fault.New(fault.IdentityMismatch, "founding identity collision", nil)
		}
		used[id] = true
		result[name] = id
	}
	return result, nil
}
