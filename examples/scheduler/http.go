package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultTaskPageSize = 20
	maxHTTPTaskPageSize = 100
	httpIntegerBase     = 10
	httpIntegerBits     = 64
	maxControlBodyBytes = 4096
)

func newHTTPHandler(r *runtime) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", r.handleHealth)
	mux.HandleFunc("GET /status", r.handleStatus)
	mux.HandleFunc("GET /tasks", r.handleTasks)
	mux.HandleFunc("GET /tasks/{id}", r.handleTask)
	mux.HandleFunc("POST /transfer", r.handleTransfer)
	mux.HandleFunc("POST /snapshot", r.handleSnapshot)
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/healthz" {
			values := request.Header.Values("Authorization")
			valid := false
			if len(values) == 1 && len(values[0]) <= 4103 && strings.HasPrefix(values[0], "Bearer ") {
				got := sha256.Sum256([]byte(strings.TrimPrefix(values[0], "Bearer ")))
				want := sha256.Sum256([]byte(r.secret))
				valid = subtle.ConstantTimeCompare(got[:], want[:]) == 1
			}
			if !valid {
				writeHTTP(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		ctx, cancel := context.WithTimeout(request.Context(), 4*time.Second)
		defer cancel()
		mux.ServeHTTP(w, request.WithContext(ctx))
	})
}
func writeHTTP(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
func httpError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	var business *task.Error
	if errors.As(err, &business) {
		switch business.Code {
		case "task_retired":
			code = http.StatusGone
		case "not_found":
			code = http.StatusNotFound
		default:
			code = http.StatusConflict
		}
	}
	if errors.Is(err, fault.ErrInvalid) {
		code = http.StatusBadRequest
	}
	writeHTTP(w, code, map[string]string{"error": err.Error()})
}

// localGroup returns the published local scheduler replica, if any.
func (r *runtime) localGroup() (*load.Group, error) {
	if group, ok := r.node.LoadGroup(groupID); ok {
		return group, nil
	}
	return nil, fault.ErrReplicaNotReady
}
func (r *runtime) handleHealth(w http.ResponseWriter, request *http.Request) {
	status := r.node.Status()
	group, err := r.localGroup()
	if err != nil || !status.Ready || !status.SessionLive {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	watchCtx, cancel := context.WithCancel(request.Context())
	defer cancel()
	watch, err := group.Watch(watchCtx)
	if err != nil {
		httpError(w, err)
		return
	}
	if observed := <-watch; !observed.Ready {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	writeHTTP(w, http.StatusOK, map[string]any{"ready": true, "node": r.cfg.Node, "replica": group.ReplicaID()})
}
func (r *runtime) handleStatus(w http.ResponseWriter, request *http.Request) {
	nodeStatus := r.node.Status()
	out := map[string]any{"observational": true, "node": nodeStatus}
	if id := nodeStatus.Scheduling.CurrentOperationID; id != nil {
		if operation, err := r.node.Cluster().Operation(request.Context(), topology.OperationID(*id)); err == nil {
			out["operation"] = operation
		} else {
			out["operation_error"] = err.Error()
		}
	}
	if s := r.service.Load(); s != nil {
		out["application"] = s.Status(request.Context())
	} else {
		out["application_error"] = fault.ErrReplicaNotReady.Error()
	}
	writeHTTP(w, http.StatusOK, out)
}
func (r *runtime) handleTasks(w http.ResponseWriter, request *http.Request) {
	after := int64(0)
	limit := defaultTaskPageSize
	var err error
	if raw := request.URL.Query().Get("after"); raw != "" {
		after, err = strconv.ParseInt(raw, httpIntegerBase, httpIntegerBits)
	}
	if raw := request.URL.Query().Get("limit"); raw != "" && err == nil {
		limit, err = strconv.Atoi(raw)
	}
	if err != nil || after < 0 || limit < 1 || limit > maxHTTPTaskPageSize {
		httpError(w, fault.ErrInvalid)
		return
	}
	group, err := r.localGroup()
	if err != nil {
		httpError(w, err)
		return
	}
	page, err := task.ReadTasks(request.Context(), group, after, limit)
	if err != nil {
		httpError(w, err)
		return
	}
	writeHTTP(w, http.StatusOK, page)
}
func (r *runtime) handleTask(w http.ResponseWriter, request *http.Request) {
	id, err := strconv.ParseInt(request.PathValue("id"), httpIntegerBase, httpIntegerBits)
	if err != nil || id <= 0 {
		httpError(w, fault.ErrInvalid)
		return
	}
	group, err := r.localGroup()
	if err != nil {
		httpError(w, err)
		return
	}
	currentTask, err := task.ReadTask(request.Context(), group, id)
	if err != nil {
		httpError(w, err)
		return
	}
	writeHTTP(w, http.StatusOK, currentTask)
}
func (r *runtime) handleTransfer(w http.ResponseWriter, request *http.Request) {
	data, err := io.ReadAll(http.MaxBytesReader(w, request.Body, maxControlBodyBytes))
	if err != nil {
		httpError(w, fault.ErrInvalid)
		return
	}
	var body struct {
		Node string `json:"node"`
	}
	if jsonwire.Decode(data, &body) != nil {
		httpError(w, fault.ErrInvalid)
		return
	}
	if !slices.Contains(r.cfg.Peers, body.Node) {
		httpError(w, fault.ErrInvalid)
		return
	}
	if err := r.node.Cluster().TransferLeader(request.Context(), groupID, body.Node); err != nil {
		httpError(w, err)
		return
	}
	writeHTTP(w, http.StatusAccepted, map[string]string{"status": "transfer_requested", "node": body.Node})
}
func (r *runtime) handleSnapshot(w http.ResponseWriter, request *http.Request) {
	data, err := io.ReadAll(http.MaxBytesReader(w, request.Body, maxControlBodyBytes))
	if err != nil || len(strings.TrimSpace(string(data))) != 0 {
		httpError(w, fault.ErrInvalid)
		return
	}
	group, err := r.localGroup()
	if err != nil {
		httpError(w, err)
		return
	}
	if err := group.Snapshot(request.Context()); err != nil {
		httpError(w, err)
		return
	}
	writeHTTP(w, http.StatusOK, map[string]string{"status": "snapshot_saved"})
}
