// Package metaservice implements the node's private _meta topology service.
package metaservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

type FoundingMember struct {
	Node     topology.Node
	MemberID uint64
	Address  string
}
type Timing struct {
	TickInterval                   time.Duration
	ElectionTicks, HeartbeatTicks  int
	DialTimeout, SendTimeout       time.Duration
	PeerRetryInitial, PeerRetryMax time.Duration
}
type NetworkOptions struct {
	Listener          net.Listener
	AdvertiseAddress  string
	Resolver          topology.AddressResolver
	TLS, ClientTLS    *tls.Config
	AllowPlaintext    bool
	ClusterCredential []byte
	MaxSnapshotBytes  int
}
type Options struct {
	Identity topology.Identity
	// Node is the caller-defined local admission document. Founders may instead
	// supply it in the matching FoundingMember; joiners must supply its type here.
	Node              topology.Node
	DBPath            string
	Founders          []FoundingMember
	JoinSeeds         []string
	Join              bool
	Limits            topology.Limits
	Placement         topology.PlacementRule
	Timing            Timing
	SessionTTL        time.Duration
	SnapshotThreshold uint64
	MaxWatchers       int
	Network           NetworkOptions
	LoadGroups        topology.GroupController
	Health            topology.HealthSource
	Reconciliation    ReconciliationOptions
	Observation       ObservationOptions
	Events            *events.Bus
	Logger            *slog.Logger
	// OnSelfRemoved is invoked asynchronously after durable approved removal.
	// It must honor cancellation and return before Stop/Quiesce can join it.
	// Use Group.RequestStop to schedule teardown instead of waiting on this
	// same group's Stop/Quiesce inside the callback. Callbacks are idempotent
	// across crashes/restarts and must verify the reason's exact local identity.
	OnSelfRemoved func(context.Context, string) error
	OnReadmission func(context.Context, Readmission) error
}

// ReconciliationOptions controls local automatic load-replica repair. Disabled
// is an explicit opt-out. Zero timings use a 30s interval, 20s tick timeout,
// 60s stable-node window and 5m action cooldown. It never repairs _meta members
// or approves staged operator intent.
type ReconciliationOptions = scheduling.Settings

// ObservationOptions controls node/group health sampling. Defaults are a 5s
// interval and 2s timeout. Document application also wakes the sampler.
type ObservationOptions = observe.Settings
type Readmission struct {
	Previous       topology.Identity `json:"previous"`
	Granted        topology.Identity `json:"granted"`
	AdmissionID    string            `json:"admission_id"`
	MetaDBPath     string            `json:"meta_db_path"`
	PreviousGroups []topology.Group  `json:"previous_groups"`
	GrantedGroups  []topology.Group  `json:"granted_groups"`
}
type ReadmissionRequiredError struct{ Readmission Readmission }

func (e *ReadmissionRequiredError) Error() string {
	return fmt.Sprintf("readmission requires local orchestration for %s: member %d to %d", e.Readmission.Granted.NodeName, e.Readmission.Previous.MemberID, e.Readmission.Granted.MemberID)
}
func (e *ReadmissionRequiredError) Unwrap() error { return fault.ErrReadmissionRequired }
func DefaultTiming() Timing {
	return Timing{TickInterval: 100 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 1, DialTimeout: 500 * time.Millisecond, SendTimeout: 500 * time.Millisecond, PeerRetryInitial: 100 * time.Millisecond, PeerRetryMax: 5 * time.Second}
}
func (o Options) defaults() Options {
	o.Limits = o.Limits.WithDefaults()
	o.Reconciliation = o.Reconciliation.WithDefaults()
	o.Observation = o.Observation.WithDefaults()
	d := DefaultTiming()
	if o.Timing.TickInterval == 0 {
		o.Timing.TickInterval = d.TickInterval
	}
	if o.Timing.ElectionTicks == 0 {
		o.Timing.ElectionTicks = d.ElectionTicks
	}
	if o.Timing.HeartbeatTicks == 0 {
		o.Timing.HeartbeatTicks = d.HeartbeatTicks
	}
	if o.Timing.DialTimeout == 0 {
		o.Timing.DialTimeout = d.DialTimeout
	}
	if o.Timing.SendTimeout == 0 {
		o.Timing.SendTimeout = d.SendTimeout
	}
	if o.Timing.PeerRetryInitial == 0 {
		o.Timing.PeerRetryInitial = d.PeerRetryInitial
	}
	if o.Timing.PeerRetryMax == 0 {
		o.Timing.PeerRetryMax = d.PeerRetryMax
	}
	if o.SessionTTL == 0 {
		o.SessionTTL = 15 * time.Second
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = 512
	}
	if o.MaxWatchers == 0 {
		o.MaxWatchers = 256
	}
	if o.Network.MaxSnapshotBytes == 0 {
		o.Network.MaxSnapshotBytes = 64 << 20
	}
	if o.Placement.MinReplicas == 0 && o.Placement.MaxReplicas == 0 {
		o.Placement.MinReplicas = 1
		o.Placement.MaxReplicas = o.Limits.MaxReplicas
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
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
	o.Network.ClusterCredential = bytes.Clone(o.Network.ClusterCredential)
	if o.Network.TLS != nil {
		o.Network.TLS = o.Network.TLS.Clone()
	}
	if o.Network.ClientTLS != nil {
		o.Network.ClientTLS = o.Network.ClientTLS.Clone()
	}
	sort.Slice(o.Founders, func(i, j int) bool { return o.Founders[i].Node.Name < o.Founders[j].Node.Name })
	return o
}
func (o Options) Validate() error {
	invalid := func(message string) error { return fault.New(fault.Invalid, message, nil) }
	if err := o.Reconciliation.Validate(); err != nil {
		return err
	}
	if err := o.Observation.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(o.Identity.NodeName) == "" || strings.TrimSpace(o.Identity.ClusterID) == "" || strings.TrimSpace(o.Identity.InstanceID) == "" || strings.ContainsRune(o.Identity.NodeName, 0) || strings.ContainsRune(o.Identity.ClusterID, 0) || strings.ContainsRune(o.Identity.InstanceID, 0) || strings.TrimSpace(o.DBPath) == "" {
		return invalid("meta identity and caller database path are required")
	}
	if o.Timing.TickInterval < 0 || o.Timing.ElectionTicks < 0 || o.Timing.HeartbeatTicks < 0 || o.Timing.DialTimeout < 0 || o.Timing.SendTimeout < 0 || o.Timing.PeerRetryInitial < 0 || o.Timing.PeerRetryMax < 0 || o.SessionTTL < 0 || o.MaxWatchers < 0 || o.Network.MaxSnapshotBytes < 0 {
		return invalid("negative meta timing or bound")
	}
	o = o.defaults()
	if err := o.Limits.Validate(); err != nil {
		return err
	}
	if o.Node.Name != o.Identity.NodeName || strings.TrimSpace(string(o.Node.Type)) == "" || o.Node.State != topology.NodeActive {
		return invalid("local admission requires matching node name, user-defined type and active state")
	}
	policy := topology.Document{Version: 1, ClusterID: o.Identity.ClusterID, Placement: o.Placement, Nodes: []topology.Node{o.Node}, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: o.Node.Name, ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	if err := policy.Validate(topology.Rules{SchemaVersion: 1, Limits: o.Limits}); err != nil {
		return err
	}
	if o.Timing.TickInterval < 10*time.Millisecond || o.Timing.ElectionTicks < 2 || o.Timing.HeartbeatTicks >= o.Timing.ElectionTicks || int64(o.Timing.ElectionTicks) > math.MaxInt64/int64(o.Timing.TickInterval) || o.Timing.PeerRetryMax < o.Timing.PeerRetryInitial {
		return invalid("invalid meta Raft timing")
	}
	if o.SessionTTL < time.Second || o.SessionTTL%time.Second != 0 || o.SessionTTL/2 <= o.Timing.TickInterval*time.Duration(o.Timing.ElectionTicks) {
		return invalid("session TTL must be whole seconds and exceed two election intervals")
	}
	if o.Network.Listener == nil || o.Network.AdvertiseAddress == "" || len(o.Network.ClusterCredential) == 0 {
		return invalid("meta listener, advertised address and cluster credential are required")
	}
	if !o.Network.AllowPlaintext && (o.Network.TLS == nil || o.Network.ClientTLS == nil) {
		return invalid("plaintext meta networking must be explicitly enabled")
	}
	if _, _, err := net.SplitHostPort(o.Network.AdvertiseAddress); err != nil {
		return invalid("invalid advertised meta address")
	}
	if o.Join && len(o.JoinSeeds) == 0 {
		return invalid("joining requires seed addresses")
	}
	for _, seed := range o.JoinSeeds {
		if _, _, err := net.SplitHostPort(seed); err != nil {
			return invalid("invalid join seed address")
		}
	}
	if o.Join && len(o.Founders) > 0 {
		return invalid("joining cannot declare founding voters")
	}
	names := make(map[string]bool)
	ids := make(map[uint64]bool)
	doc := topology.Document{Version: 1, ClusterID: o.Identity.ClusterID, Placement: o.Placement, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 1}}}
	for _, founder := range o.Founders {
		if names[founder.Node.Name] || ids[founder.MemberID] || founder.MemberID == 0 || founder.MemberID == math.MaxUint64 || founder.Node.State != topology.NodeActive {
			return invalid("founder names and positive member IDs must be unique")
		}
		if _, _, err := net.SplitHostPort(founder.Address); err != nil {
			return invalid("invalid founder meta address")
		}
		names[founder.Node.Name] = true
		ids[founder.MemberID] = true
		doc.Nodes = append(doc.Nodes, founder.Node)
		if err := doc.SetMemberID(founder.Node.Name, founder.MemberID, time.Time{}); err != nil {
			return err
		}
		if founder.Node.Name == o.Identity.NodeName && founder.MemberID != o.Identity.MemberID {
			return invalid("local identity differs from founder declaration")
		}
		if founder.Node.Name == o.Identity.NodeName && founder.Node.Type != o.Node.Type {
			return invalid("local node type differs from founder declaration")
		}
	}
	if len(o.Founders) > 0 {
		if !names[o.Identity.NodeName] {
			return invalid("local node is absent from founders")
		}
		if err := doc.Validate(topology.Rules{SchemaVersion: 1, Limits: o.Limits}); err != nil {
			return err
		}
	}
	// A restart may omit founders: New verifies the existing pinned store before
	// constructing an engine, and refuses an empty store without founding peers.
	return nil
}

// FoundingIDs derives stable positive IDs independently of peer ordering.
func FoundingIDs(clusterID string, names []string) (map[string]uint64, error) {
	if strings.TrimSpace(clusterID) == "" || len(names) == 0 {
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
