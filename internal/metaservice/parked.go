package metaservice

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

const (
	parkedJoinTTL       = 5 * time.Minute
	parkedSweepInterval = 30 * time.Second
	parkedJoinCap       = 128
)
const (
	parkedUnapproved   = "no_approved_execution"
	parkedExcluded     = "excluded"
	parkedSuperseded   = "identity_superseded"
	parkedNameConflict = "name_conflict"
)

type parkedKey struct{ name, instance string }
type parkedRegistry struct {
	mu      sync.Mutex
	entries map[parkedKey]topology.ParkedJoin
}

func (r *parkedRegistry) record(req joinRequest, kind, reason string) {
	if req.Identity.NodeName == "" || req.Identity.InstanceID == "" {
		return
	}
	switch kind {
	case parkedUnapproved, parkedExcluded, parkedSuperseded, parkedNameConflict:
	default:
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = map[parkedKey]topology.ParkedJoin{}
	}
	key := parkedKey{req.Identity.NodeName, req.Identity.InstanceID}
	entry, found := r.entries[key]
	now := time.Now().UTC()
	if !found {
		if len(r.entries) >= parkedJoinCap {
			r.evictOldest()
		}
		entry = topology.ParkedJoin{Name: req.Identity.NodeName, InstanceID: req.Identity.InstanceID, FirstSeen: now}
	}
	entry.ClusterID = req.Identity.ClusterID
	entry.Type = req.Node.Type
	entry.Address = req.Address
	entry.ReportedDocVersion = req.LastDocVersion
	entry.ReportedMemberID = req.Identity.MemberID
	entry.RefusalKind = kind
	entry.Reason = reason
	entry.LastSeen = now
	if entry.Attempts < ^uint64(0) {
		entry.Attempts++
	}
	r.entries[key] = entry
}
func (r *parkedRegistry) clear(name, instance string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.entries {
		if key.name == name && (key.instance == instance || entry.RefusalKind != parkedNameConflict) {
			delete(r.entries, key)
		}
	}
}
func (r *parkedRegistry) sweep(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for key, p := range r.entries {
		if p.LastSeen.Before(now.Add(-parkedJoinTTL)) {
			delete(r.entries, key)
			removed++
		}
	}
	return removed
}
func (r *parkedRegistry) snapshot() []topology.ParkedJoin {
	r.sweep(time.Now().UTC())
	r.mu.Lock()
	defer r.mu.Unlock()
	out := collection.Slice[topology.ParkedJoin](len(r.entries))
	for _, p := range r.entries {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b topology.ParkedJoin) int {
		return cmp.Or(b.LastSeen.Compare(a.LastSeen), cmp.Compare(a.Name, b.Name), cmp.Compare(a.InstanceID,
			b.InstanceID))
	})
	return out
}
func (g *Group) runParkedSweep() {
	defer g.wg.Done()
	ticker := time.NewTicker(parkedSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.workCtx.Done():
			return
		case now := <-ticker.C:
			g.parked.sweep(now)
		}
	}
}

// ParkedJoins reads volatile observations from the current leader. Entries are
// keyed by exact name/process, bounded and expiring; they are never replicated.
// A leader change rebuilds observations from the refused processes' retries.
func (g *Group) ParkedJoins(ctx context.Context) ([]topology.ParkedJoin, error) {
	if g == nil {
		return nil, nil
	}
	if err := g.ensureRunning(ctx); err != nil {
		return nil, err
	}
	if g.IsLeader() {
		return g.parked.snapshot(), nil
	}
	addresses := g.parkedReadAddresses(ctx)
	seen := map[string]bool{}
	var last error = fault.ErrUnavailable
	for i := 0; i < len(addresses); i++ {
		address := addresses[i]
		if address == "" || seen[address] {
			continue
		}
		seen[address] = true
		attempt, cancel := context.WithTimeout(ctx, g.opts.Timing.DialTimeout)
		reply, err := g.transport.CallAddress(attempt, address, "ParkedRead", nil)
		cancel()
		if err != nil {
			last = err
			continue
		}
		if err = metatransport.ReplyError(reply); err != nil {
			last = err
			if reply.LeaderAddress != "" {
				addresses = append(addresses, reply.LeaderAddress)
			}
			continue
		}
		var records []topology.ParkedJoin
		if err = json.Unmarshal(reply.Data, &records); err != nil {
			return nil, fault.New(fault.Unreadable, "invalid parked observation response", err)
		}
		return records, nil
	}
	return nil, last
}

// evictOldest is called while the registry lock is held.
func (r *parkedRegistry) evictOldest() {
	var oldest parkedKey
	var at time.Time
	for k, p := range r.entries {
		if at.IsZero() || p.LastSeen.Before(at) ||
			(k.name < oldest.name || k.name == oldest.name &&
				k.instance < oldest.instance) && p.LastSeen.Equal(at) {
			oldest = k
			at = p.LastSeen
		}
	}
	delete(r.entries, oldest)
}

func (g *Group) parkedReadAddresses(ctx context.Context) []string {
	addresses := append([]string(nil), g.opts.JoinSeeds...)
	if engine := g.engine.Load(); engine != nil && engine.RaftStatus().Lead != 0 {
		if address, err := g.resolveMember(ctx, engine.RaftStatus().Lead); err == nil {
			addresses = append([]string{address}, addresses...)
		}
	}
	return addresses
}
