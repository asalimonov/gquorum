// Package load runs Raft groups with caller-defined state machines and transports.
package load

import "github.com/asalimonov/gquorum/internal/raftengine"

type Command = raftengine.Command
type Result = raftengine.Result
type Machine = raftengine.Machine
type Transport = raftengine.Transport
type Mode = raftengine.Mode
type Timing = raftengine.Timing
type Options = raftengine.Options
type Proposal = raftengine.Proposal
type Group = raftengine.Group
type Registry = raftengine.Registry

const (
	Founding = raftengine.Founding
	Joining  = raftengine.Joining
)

func New(opts Options) (*Group, error) { return raftengine.New(opts) }
func NewRegistry() *Registry           { return raftengine.NewRegistry() }
