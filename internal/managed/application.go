package managed

import (
	"context"
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

// startApplicationRunLocked reserves ownership in the handle publication
// critical section. A stop racing publication must join this exact callback.
func (h *Host) startApplicationRunLocked(slot *replica) {
	if slot.app.Run == nil {
		return
	}
	ctx, cancel := context.WithCancel(slot.core.RunContext())
	slot.runCancel, slot.runDone = cancel, make(chan struct{})
	callback, group := slot.app.Run, (*load.Group)(slot.handle)
	go func() {
		// Publication and startDone complete before application code can run.
		// Waiting here also prevents a fast failing callback from racing
		// startup's result.
		<-slot.startDone
		var err error
		if ctx.Err() == nil {
			err = callback(ctx, group)
		}
		h.finishApplicationRun(ctx, slot, err)
		cancel()
		close(slot.runDone)
	}()
}

func (h *Host) finishApplicationRun(ctx context.Context, slot *replica, err error) {
	if err == nil || ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	failure := fault.New(fault.Unavailable, fmt.Sprintf("application background work failed: %v", err), err)
	h.mu.Lock()
	slot.err = failure
	h.holds[slot.identity] = true
	h.runFailures[slot.identity] = ReplicaStatus{
		Identity: slot.identity, InstanceID: slot.node.InstanceID, RunID: slot.core.RunID(),
		State: "failed", ErrorCode: fault.Unavailable, Error: failure.Error(),
	}
	slot.core.Fail(failure)
	h.beginStopLocked(slot) // release owner joins runDone; this goroutine never does
	h.mu.Unlock()
	h.notifyChange()
	if h.cfg.Events != nil {
		h.cfg.Events.Publish(events.Event{
			Type:       events.RuntimeError,
			Code:       "application_run_failed",
			NodeName:   slot.node.NodeName,
			InstanceID: slot.node.InstanceID,
			ClusterID:  slot.identity.ClusterID,
			GroupID:    slot.identity.GroupID,
			ReplicaID:  slot.identity.ReplicaID,
		})
	}
	h.cfg.Logger.Error("application background work failed", "group", slot.identity.GroupID, "replica_id",
		slot.identity.ReplicaID, "error", err)
}

func (h *Host) notifyChange() {
	if h.cfg.OnChange != nil {
		h.cfg.OnChange()
	}
}
