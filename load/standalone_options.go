package load

import (
	"log/slog"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
)

// StandaloneOptions supplies exact identity, initial peers and borrowed engine
// dependencies.
type StandaloneOptions struct {
	Identity     storage.Identity
	InstanceID   string
	Mode         StartMode
	Peers        []raft.Peer
	Engine       EngineOptions
	Store        storage.Store
	StateMachine StateMachine
	Transport    RaftTransport
	Events       *events.Bus
	Logger       *slog.Logger
}
