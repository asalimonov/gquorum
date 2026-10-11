package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// This is an application startup policy. Raft remains available throughout the
// wait, so a late founder can catch up without restarting its peers.
func (a *application) waitFounders(ctx context.Context) error {
	if !a.foundingGate {
		return nil
	}
	select {
	case <-a.foundingReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (a *application) foundersReady() bool {
	if !a.foundingGate {
		return true
	}
	select {
	case <-a.foundingReady:
		return true
	default:
		return false
	}
}
func (a *application) missingFounders(ctx context.Context) []string {
	active, err := a.meta.Active(ctx)
	observation := a.meta.HealthObservation()
	fresh := err == nil && active.Version != 0 && observation.Version == active.Version && !observation.At.IsZero() && time.Since(observation.At) <= a.cfg.SessionTTL
	var missing []string
	for _, peer := range a.cfg.Peers {
		if !fresh || active.MemberID(peer.Name) != peer.MemberID || observation.Nodes[peer.Name] != topology.HealthAlive {
			missing = append(missing, peer.Name)
		}
	}
	slices.Sort(missing)
	return missing
}
func (a *application) runFoundingGate() {
	ctx := a.workCtx
	if a.cfg.FoundingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.cfg.FoundingTimeout)
		defer cancel()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	last := ""
	for {
		missing := a.missingFounders(ctx)
		if len(missing) == 0 && a.meta.OwnSessionLive() {
			if err := a.persistEstablished(); err != nil {
				a.fail(err)
				return
			}
			a.logger.Info("configured founders activated")
			close(a.foundingReady)
			return
		}
		if names := strings.Join(missing, ","); names != last {
			a.logger.Info("waiting for configured cluster members to activate", "missing", names)
			last = names
		}
		select {
		case <-ctx.Done():
			if a.workCtx.Err() == nil {
				a.fail(fmt.Errorf("greenfield founding timed out after %s; missing: %s", a.cfg.FoundingTimeout, strings.Join(missing, ",")))
			}
			return
		case <-ticker.C:
		}
	}
}
