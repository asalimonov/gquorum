package metaservice

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// FoundingMember declares an initial meta node, numeric member ID and
// advertised endpoint.
type FoundingMember struct {
	Node          topology.Node
	MemberID      uint64
	Address       string
	PinnedLearner bool
}

// Timing configures meta Raft ticks, peer call timeouts and reconnect delays.
type Timing struct {
	TickInterval                   time.Duration
	ElectionTicks, HeartbeatTicks  int
	DialTimeout, SendTimeout       time.Duration
	PeerRetryInitial, PeerRetryMax time.Duration
}

// NetworkOptions supplies authenticated meta networking and an acquired
// listener.
type NetworkOptions struct {
	Listener          net.Listener
	AdvertiseAddress  string
	Resolver          topology.AddressResolver
	TLS, ClientTLS    *tls.Config
	AllowPlaintext    bool
	ClusterCredential []byte
	MaxSnapshotBytes  int
}
