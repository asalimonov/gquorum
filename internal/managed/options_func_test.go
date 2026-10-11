//go:build functional

package managed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// The embedded nil interface panics on any I/O, including Close. Construction
// and stopping a never-started owner must leave these borrowed resources alone.
type unopenedStore struct{ storage.Store }

func TestConstructorValidatesWithoutAcquiringDependencies(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := f.cfg
	cfg.Store = unopenedStore{}
	cfg.Identity = func() topology.Identity { t.Error("constructor resolved service identity"); return topology.Identity{} }
	cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		t.Error("constructor called factory")
		return load.Application{}, fault.ErrInvalid
	}
	h, err := New(cfg)
	must(t, err)
	must(t, h.Stop(t.Context()))
	if err := h.Start(t.Context()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Factory = nil },
		func(c *Config) { c.Transport = nil },
		func(c *Config) { c.RecordReplica = nil },
		func(c *Config) { c.ValidatePeer = nil },
		func(c *Config) { c.CallbackTimeout = -time.Second },
		func(c *Config) { c.RecoveryInterval = -time.Second },
		func(c *Config) { c.EffectTimeout = -time.Second },
		func(c *Config) { c.Engine.Timing.TickInterval = time.Nanosecond },
		func(c *Config) { c.Engine.MaxPendingProposals = -1 },
	} {
		invalid := cfg
		change(&invalid)
		if _, err := New(invalid); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
