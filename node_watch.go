package gquorum

import (
	"context"
	"reflect"
	"slices"
)

// NodeObservation is an owned local lifecycle/status snapshot. Revision gaps
// indicate coalescing, not event loss from a replicated log. Status composes
// local observations; it does not provide a cross-component atomic barrier.
type NodeObservation struct {
	Revision uint64     `json:"revision"`
	Status   NodeStatus `json:"status"`
}

type observationFinish uint8

const (
	publishUpdate observationFinish = iota
	finishUpdates
)

// Watch registers with a current snapshot, including before Start or after
// Stop.
// Slow consumers retain only the latest state. Cancellation closes this watch;
// successful shutdown retains the final state and closes all watches. Replica
// InstanceID/RunID distinguish replacements; old Group watches remain exact.
func (n *node) Watch(ctx context.Context) (<-chan NodeObservation, error) {
	n.observationMu.Lock()
	defer n.observationMu.Unlock()
	n.publishObservationLocked(publishUpdate)
	return n.observations.Subscribe(ctx)
}

func cloneNodeObservation(o NodeObservation) NodeObservation {
	o.Status.Meta = o.Status.Meta.Clone()
	o.Status.Scheduling = o.Status.Scheduling.Clone()
	o.Status.Loads = slices.Clone(o.Status.Loads)
	if o.Status.SelfRemoval != nil {
		removal := *o.Status.SelfRemoval
		o.Status.SelfRemoval = &removal
	}
	return o
}

func (n *node) publishObservation() {
	n.observationMu.Lock()
	defer n.observationMu.Unlock()
	n.publishObservationLocked(publishUpdate)
}

// Capture inside observationMu so a slower publisher cannot overwrite a newer
// capture. Call only outside Node/host ownership locks; Status acquires them.
func (n *node) publishObservationLocked(finish observationFinish) {
	if n.observations == nil || n.observationsFinished {
		return
	}
	status := n.Status()
	changed := !reflect.DeepEqual(n.observation.Status, status)
	if changed {
		n.observation = NodeObservation{Revision: n.observation.Revision + 1, Status: status}
	}
	if finish == finishUpdates {
		n.observationsFinished = true
		n.observations.Finish(n.observation)
	} else if changed {
		n.observations.Publish(n.observation)
	}
}

func (n *node) finishObservations() {
	n.observationMu.Lock()
	n.publishObservationLocked(finishUpdates)
	n.observationMu.Unlock()
	if n.observations != nil {
		_ = n.observations.Join(context.Background())
	}
}

// wakeObservation is safe from internal state producers: it never reads status
// or recursively wakes host recovery while those producers are changing state.
func (n *node) wakeObservation() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}
