//go:build functional

package operator

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestConstructorRefusesInvalidDependenciesAndDurations(t *testing.T) {
	t.Parallel()
	for _, change := range []struct {
		name string
		set  func(*Config)
	}{
		{"rules", func(c *Config) { c.Rules.SchemaVersion = 0 }},
		{"store", func(c *Config) { c.Store = nil }},
		{"coordinator", func(c *Config) { c.Coordinator = nil }},
		{"effects", func(c *Config) { c.Groups = nil }},
		{"owner", func(c *Config) { c.OwnsGroup = nil }},
		{"interval", func(c *Config) { c.Interval = -time.Second }},
		{"retry", func(c *Config) { c.MaxRetryInterval = -time.Second }},
		{"proposal timeout", func(c *Config) { c.ProposalTimeout = -time.Second }},
	} {
		t.Run(change.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}}, Store: &controllerStore{}, Coordinator: &controllerCoordinator{}, Groups: &controllerGroups{}, OwnsGroup: func(string) bool { return true }}
			change.set(&cfg)
			c, err := New(cfg)
			if c != nil || !errors.Is(err, fault.ErrInvalid) {
				t.Fatalf("constructor = %v/%v", c, err)
			}
		})
	}
}
