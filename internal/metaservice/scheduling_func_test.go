//go:build functional

package metaservice

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaSchedulingConstructorSettingsAndDisabledStatus(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	o.Scheduling.TickTimeout = -time.Second
	if _, err := New(o); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.DBPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid repair settings changed storage", err)
	}
	groups := singleMetaFixtureWith(t, func(o *Options) { o.Scheduling.Disabled = true })
	leader := readyPublic(t, groups)
	s := leader.SchedulingStatus()
	if s.Enabled || s.State != topology.SchedulingDisabled || s.Reason != "config_disabled" {
		t.Fatal(s)
	}
}
