package membership

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type groupLock struct {
	token chan struct{}
	users int
}

// Coordinator serializes one change per group and rechecks safety before proposing.
type Coordinator struct {
	mu    sync.Mutex
	locks map[string]*groupLock
}

func NewCoordinator() *Coordinator { return &Coordinator{locks: make(map[string]*groupLock)} }

func (c *Coordinator) Apply(ctx context.Context, g RaftGroup, live HealthSource, cc raftpb.ConfChangeV2) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := g.GroupID()
	c.mu.Lock()
	if c.locks == nil {
		c.locks = make(map[string]*groupLock)
	}
	lock := c.locks[id]
	if lock == nil {
		lock = &groupLock{token: make(chan struct{}, 1)}
		c.locks[id] = lock
	}
	lock.users++
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		lock.users--
		if lock.users == 0 {
			delete(c.locks, id)
		}
		c.mu.Unlock()
	}()
	select {
	case lock.token <- struct{}{}:
		defer func() { <-lock.token }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := (SafetyPolicy{}).Check(g, live, cc); err != nil {
		return err
	}
	if err := g.ProposeConfChangeV2(ctx, cc); err != nil {
		op, replicaID := describeChange(cc)
		return fmt.Errorf("membership: group %q operation %s replica %d proposal failed: %w", id, op, replicaID, err)
	}
	return nil
}
