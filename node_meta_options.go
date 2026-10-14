package gquorum

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

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
