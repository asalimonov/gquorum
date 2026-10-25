package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshotControlRequest struct {
	FailRestoreCount uint64            `json:"fail_restore_count"`
	Identity         topology.Identity `json:"identity"`
	Hold             string            `json:"hold"`
	HoldOffset       *uint64           `json:"hold_offset,omitempty"`
	ChunkDelayMillis int               `json:"chunk_delay_ms"`
	CorruptOffset    *uint64           `json:"corrupt_offset,omitempty"`
	DropAdmissionAck bool              `json:"drop_admission_ack"`
	DropChunkAckAt   uint64            `json:"drop_chunk_ack_at"`
	DropFinalAck     bool              `json:"drop_final_ack"`
}
type snapshotControl struct {
	request                                                 snapshotControlRequest
	release                                                 chan struct{}
	active                                                  uint64
	hits                                                    map[string]uint64
	corrupted, admissionDropped, chunkDropped, finalDropped bool
	offer                                                   snapshotOfferObservation
}
type snapshotOfferObservation struct {
	Index        uint64            `json:"index"`
	Term         uint64            `json:"term"`
	Identity     topology.Identity `json:"identity"`
	DeliveryID   uint64            `json:"delivery_id"`
	EncodedBytes uint64            `json:"encoded_bytes"`
	SHA256       string            `json:"sha256"`
}
type snapshotControlObservation struct {
	NotNeededReplies uint64                   `json:"not_needed_replies"`
	Identity         topology.Identity        `json:"identity"`
	ReplicaID        uint64                   `json:"replica_id"`
	Active           uint64                   `json:"active"`
	Hits             map[string]uint64        `json:"hits"`
	Offer            snapshotOfferObservation `json:"offer"`
	Application      syntheticObservation     `json:"application"`
	Transfers        snapshot.Page            `json:"transfers"`
}

func validSnapshotPhase(phase string) bool {
	switch phase {
	case "", "chunk", "before_finish", "after_handoff", "before_persist", "after_persist", "before_restore", "after_restore":
		return true
	default:
		return false
	}
}
func (a *application) snapshotPhase(ctx context.Context, id topology.Identity, key walKey, phase string, offset uint64) error {
	if a == nil {
		return ctx.Err()
	}
	h := a.clusterHooks
	if h == nil {
		return ctx.Err()
	}
	h.mu.Lock()
	control := h.snapshotControls[key]
	if control == nil || control.request.Identity != id {
		h.mu.Unlock()
		return ctx.Err()
	}
	control.hits[phase]++
	held := control.request.Hold == phase && (control.request.HoldOffset == nil || *control.request.HoldOffset == offset)
	release := control.release
	if held {
		control.active++
	}
	h.mu.Unlock()
	if !held {
		return ctx.Err()
	}
	defer func() { h.mu.Lock(); control.active--; h.mu.Unlock() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
		return nil
	}
}
func (a *application) snapshotWrite(ctx context.Context, chunk snapshot.Chunk) (snapshot.Chunk, error) {
	id := chunk.Session.Key.To.Node
	key := walKey{chunk.Session.Key.To.Replica.GroupID, chunk.Session.Key.To.Replica.ReplicaID}
	if err := a.snapshotPhase(ctx, id, key, "chunk", chunk.Offset); err != nil {
		return chunk, err
	}
	h := a.clusterHooks
	if h == nil {
		return chunk, nil
	}
	h.mu.Lock()
	control := h.snapshotControls[key]
	delay := time.Duration(0)
	corrupt := false
	if control != nil && control.request.Identity == id {
		delay = time.Duration(control.request.ChunkDelayMillis) * time.Millisecond
		if control.request.CorruptOffset != nil && *control.request.CorruptOffset == chunk.Offset && !control.corrupted {
			control.corrupted = true
			corrupt = true
		}
	}
	h.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return chunk, ctx.Err()
		case <-timer.C:
		}
	}
	value := chunk
	if corrupt && len(value.Data) > 0 {
		value.Data = append([]byte(nil), chunk.Data...)
		value.Data[0] ^= 0xff
	}
	return value, nil
}
func (a *application) snapshotWireAck(ctx context.Context, ack fixtureSnapshotWireAck) error {
	h := a.clusterHooks
	if h == nil {
		return nil
	}
	key := walKey{ack.Key.To.Replica.GroupID, ack.Key.To.Replica.ReplicaID}
	h.mu.Lock()
	control := h.snapshotControls[key]
	drop := false
	if control != nil && control.request.Identity == ack.Key.To.Node {
		if ack.State == snapshot.DeliveryReceiving && ack.Verified == 0 && control.request.DropAdmissionAck && !control.admissionDropped {
			control.admissionDropped = true
			drop = true
		}
		if ack.Verified > 0 && ack.State == snapshot.DeliveryReceiving && control.request.DropChunkAckAt > 0 &&
			ack.Verified >= control.request.DropChunkAckAt && !control.chunkDropped {
			control.chunkDropped = true
			drop = true
		}
		if ack.State == snapshot.DeliveryAccepted && control.request.DropFinalAck && !control.finalDropped {
			control.finalDropped = true
			drop = true
		}
	}
	h.mu.Unlock()
	if drop {
		return status.Error(codes.Unavailable, "fixture dropped snapshot acknowledgement")
	}
	return ctx.Err()
}
func (a *application) recordSnapshotOffer(send snapshot.Send) {
	h := a.clusterHooks
	if h == nil {
		return
	}
	key := walKey{send.Manifest.Key.From.Replica.GroupID, send.Manifest.Key.From.Replica.ReplicaID}
	h.mu.Lock()
	defer h.mu.Unlock()
	control := h.snapshotControls[key]
	if control == nil {
		if len(h.snapshotControls) >= 512 {
			return
		}
		control = &snapshotControl{request: snapshotControlRequest{Identity: send.Manifest.Key.From.Node}, hits: make(map[string]uint64)}
		h.snapshotControls[key] = control
	}
	var header raftpb.Message
	_ = header.Unmarshal(send.Manifest.NativeHeader)
	control.offer = snapshotOfferObservation{Identity: send.Manifest.Key.From.Node, DeliveryID: send.Manifest.Key.DeliveryID,
		EncodedBytes: send.Manifest.TotalBytes, SHA256: hex.EncodeToString(send.Manifest.SHA256[:])}
	if header.Snapshot != nil {
		control.offer.Index = header.Snapshot.Metadata.Index
		control.offer.Term = header.Snapshot.Metadata.Term
	}
}
func (a *application) validateSnapshotControl(ctx context.Context, group string, replica uint64, id topology.Identity) error {
	if id != a.currentNode().Identity() {
		return fault.ErrIdentityMismatch
	}
	if topology.ValidateLoadID(group) != nil || replica == 0 {
		return fault.ErrInvalid
	}
	target, err := a.currentCluster().Target(ctx)
	if err != nil {
		return err
	}
	if declaration := target.Group(group); declaration != nil {
		for _, r := range declaration.Replicas {
			if r.NodeName == id.NodeName && r.ReplicaID == replica {
				return nil
			}
		}
	}
	return fault.ErrIdentityMismatch
}
func (a *application) registerSnapshotRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("PUT /internal/test/snapshot/{group}/{replica}", protect(a.configureSnapshotHTTP))
	mux.HandleFunc("GET /internal/test/snapshot/{group}/{replica}", protect(a.snapshotObservationHTTP))
	mux.HandleFunc("POST /internal/test/snapshot/{group}/{replica}/generate", protect(a.generateSnapshotHTTP))
}
func snapshotRoute(r *http.Request) (walKey, error) {
	replica, err := strconv.ParseUint(r.PathValue("replica"), 10, 64)
	if err != nil || replica == 0 {
		return walKey{}, fault.ErrInvalid
	}
	return walKey{r.PathValue("group"), replica}, nil
}
func (a *application) configureSnapshotHTTP(w http.ResponseWriter, r *http.Request) {
	key, err := snapshotRoute(r)
	if err != nil {
		httpError(w, err)
		return
	}
	var body snapshotControlRequest
	if err = requestBody(w, r, &body); err != nil {
		httpError(w, err)
		return
	}
	if (body.Hold == "before_persist" || body.Hold == "after_persist") && a.store == nil {
		httpError(w, fault.ErrUnsupported)
		return
	}
	if !validSnapshotPhase(body.Hold) || body.ChunkDelayMillis < 0 || body.ChunkDelayMillis > 60000 {
		httpError(w, fault.ErrInvalid)
		return
	}
	if err = a.validateSnapshotControl(r.Context(), key.group, key.replica, body.Identity); err != nil {
		httpError(w, err)
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	control := h.snapshotControls[key]
	if control == nil {
		if len(h.snapshotControls) >= 512 {
			h.mu.Unlock()
			httpError(w, fault.ErrOverloaded)
			return
		}
		control = &snapshotControl{hits: make(map[string]uint64)}
		h.snapshotControls[key] = control
	}
	if control.release != nil {
		close(control.release)
	}
	control.request = body
	control.corrupted, control.admissionDropped, control.chunkDropped, control.finalDropped = false, false, false, false
	control.release = nil
	if body.Hold != "" {
		control.release = make(chan struct{})
	}
	h.mu.Unlock()
	jsonResponse(w, http.StatusOK, map[string]bool{"configured": true})
}
func (a *application) snapshotObservationHTTP(w http.ResponseWriter, r *http.Request) {
	key, err := snapshotRoute(r)
	if err != nil {
		httpError(w, err)
		return
	}
	result := snapshotControlObservation{Identity: a.currentNode().Identity(), ReplicaID: key.replica, Hits: make(map[string]uint64),
		Transfers: a.currentNode().LoadSnapshots().List(snapshot.Query{GroupID: key.group})}
	h := a.clusterHooks
	h.mu.Lock()
	control := h.snapshotControls[key]
	if control != nil {
		result.Active = control.active
		result.Offer = control.offer
		for phase, count := range control.hits {
			result.Hits[phase] = count
		}
	}
	h.mu.Unlock()
	if local := a.local(key.group); local != nil && local.group.ReplicaID() == key.replica {
		result.Application = local.machine.syntheticView()
	}
	a.generationMu.RLock()
	generation := a.generation
	if generation != nil && generation.production != nil {
		service := generation.production.snapshots
		service.mu.Lock()
		result.NotNeededReplies = service.notNeeded
		service.mu.Unlock()
	}
	a.generationMu.RUnlock()
	jsonResponse(w, http.StatusOK, result)
}
func (a *application) generateSnapshotHTTP(w http.ResponseWriter, r *http.Request) {
	key, err := snapshotRoute(r)
	if err != nil {
		httpError(w, err)
		return
	}
	var body struct {
		Identity  topology.Identity `json:"identity"`
		RequestID string            `json:"request_id"`
		Spec      syntheticSpec     `json:"spec"`
	}
	if err = requestBody(w, r, &body); err != nil {
		httpError(w, err)
		return
	}
	if err = a.validateSnapshotControl(r.Context(), key.group, key.replica, body.Identity); err != nil {
		httpError(w, err)
		return
	}
	if body.Spec.EncodedBytes > a.currentNode().LoadSnapshots().Options().MaxSnapshotBytes {
		httpError(w, fault.ErrInvalid)
		return
	}
	local := a.local(key.group)
	if local == nil {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	data, err := json.Marshal(putCommand{RequestID: body.RequestID, Synthetic: &body.Spec})
	if err != nil {
		httpError(w, err)
		return
	}
	result, err := local.group.Propose(r.Context(), load.Proposal{RequestID: body.RequestID, Data: data})
	if err != nil {
		httpError(w, err)
		return
	}
	if result.Code != "ok" {
		httpError(w, fault.ErrInvalid)
		return
	}
	if err = local.group.Snapshot(r.Context()); err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, local.machine.syntheticView())
}

func (a *application) snapshotNodeIdentity() topology.Identity {
	if node := a.currentNode(); node != nil {
		return node.Identity()
	}
	return topology.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, InstanceID: a.cfg.InstanceID}
}

func (a *application) snapshotRestoreFailure(id topology.Identity, key walKey) error {
	if a == nil || a.clusterHooks == nil {
		return nil
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	control := h.snapshotControls[key]
	if control != nil && control.request.Identity == id && control.request.FailRestoreCount > 0 {
		control.request.FailRestoreCount--
		control.hits["restore_failed"]++
		return fault.ErrUnavailable
	}
	return nil
}
