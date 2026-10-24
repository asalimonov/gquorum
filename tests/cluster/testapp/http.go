package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Caller readiness logic may relax serving readiness using the persisted local override.
// Fresh health and exact approved identity remain required. Consensus writes keep
// their ordinary Raft quorum requirements regardless of this local decision.
func (a *application) canServe(ctx context.Context) bool {
	if a.currentNode().OwnSessionLive() {
		return true
	}
	minimum := a.currentNode().ServeOverride()
	if minimum == 0 || !a.currentNode().Status().Meta.Found {
		return false
	}
	active, err := a.currentCluster().Target(ctx)
	if err != nil || active.Version == 0 || active.MetaGroup() == nil ||
		active.MemberID(a.cfg.NodeName) != a.currentNode().Identity().MemberID {
		return false
	}
	observation := a.currentCluster().HealthObservation()
	if observation.Version != active.Version || observation.At.IsZero() || time.Since(observation.At) > a.cfg.SessionTTL {
		return false
	}
	alive := 0
	for _, replica := range active.MetaGroup().Replicas {
		if replica.Role == topology.ReplicaVoter && observation.Nodes[replica.NodeName] == topology.HealthAlive {
			alive++
		}
	}
	return alive >= minimum
}

// Serving follows caller readiness logic and requires the exact converged replica and role.
// Approved learners serve local reads and forward mutations through Raft.
// Prepared replicas and stale handles remain ineligible until their group converges.
func (a *application) localServing(ctx context.Context, id string) (*localReplica, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-a.closing:
		return nil, fault.ErrStopped
	default:
	}
	if !a.canServe(ctx) {
		return nil, fault.ErrUnavailable
	}
	identity := a.currentNode().Identity()
	group, err := a.servingGroup(ctx, id)
	if err != nil {
		return nil, err
	}
	local := a.local(id)
	if local == nil || !local.group.Running() || local.group.Error() != nil {
		return nil, fault.ErrReplicaNotReady
	}
	native, found := a.currentNode().LoadGroup(id)
	if !found || native != local.group {
		return nil, fault.ErrReplicaNotReady
	}
	conf := native.Status().ConfState
	eligible := slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
		if r.NodeName != identity.NodeName || r.ReplicaID != native.ReplicaID() {
			return false
		}
		return r.Role == topology.ReplicaVoter && slices.Contains(conf.Voters, r.ReplicaID) ||
			r.Role == topology.ReplicaLearner && slices.Contains(conf.Learners, r.ReplicaID)
	})
	if !eligible {
		return nil, fault.ErrReplicaNotReady
	}
	return local, nil
}
func jsonResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func httpError(w http.ResponseWriter, err error) {
	code, status := fault.Code("internal"), http.StatusInternalServerError
	var typed *fault.Error
	if errors.As(err, &typed) {
		code = typed.Code
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = fault.Unavailable
	}
	switch code {
	case fault.Invalid, fault.Unsupported:
		status = http.StatusBadRequest
	case fault.VersionConflict, fault.GroupRetired, fault.IdentityMismatch, fault.OperationAuthorization, fault.AuthorityFenced, fault.ReplanRequired:
		status = http.StatusConflict
	case fault.UnsafeMembership:
		status = http.StatusUnprocessableEntity
	case fault.HistoryUnavailable:
		status = http.StatusNotFound
	case fault.NotLeader, fault.ReplicaNotReady, fault.Unavailable, fault.Stopped, fault.Overloaded, fault.NoController, fault.AuthorityPending:
		status = http.StatusServiceUnavailable
	}
	body := map[string]any{"code": code, "error": err.Error()}
	if typed != nil && code == fault.VersionConflict {
		body["expected_version"], body["current_version"] = typed.ExpectedVersion, typed.CurrentVersion
		body["rebased_to"] = typed.RebasedTo
	}
	jsonResponse(w, status, body)
}
func requestBody(w http.ResponseWriter, r *http.Request, out any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return fault.New(fault.Invalid, "body exceeds limit or cannot be read", err)
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '{' {
		return fault.ErrInvalid
	}
	return decodeJSON(body, out)
}

func positiveNumber(value string) (uint64, error) {
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, fault.New(fault.Invalid, "positive decimal version required", nil)
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n == 0 {
		return 0, fault.New(fault.Invalid, "positive version required", err)
	}
	return n, nil
}
func expectedVersion(r *http.Request) (uint64, error) {
	if len(r.Header.Values("If-Match")) != 1 {
		return 0, fault.New(fault.Invalid, "exactly one If-Match version is required", nil)
	}
	value := r.Header.Get("If-Match")
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}
	if value == "" {
		return 0, fault.New(fault.Invalid, "If-Match with the observed Latest version is required", nil)
	}
	return positiveNumber(value)
}
func mutationOptions(r *http.Request, opts gquorum.MutationOptions) (gquorum.MutationOptions, error) {
	if opts.Approve && opts.Preview {
		return opts, fault.New(fault.Invalid, "preview cannot be approved", nil)
	}
	expected, err := expectedVersion(r)
	if err != nil {
		return opts, err
	}
	if opts.ExpectedVersion != 0 && opts.ExpectedVersion != expected {
		return opts, fault.New(fault.Invalid, "body version differs from If-Match", nil)
	}
	opts.ExpectedVersion = expected
	return opts, nil
}
func approvalOptions(w http.ResponseWriter, r *http.Request, version uint64) error {
	if r.URL.RawQuery != "" {
		return fault.New(fault.Invalid, "approval does not accept query parameters", nil)
	}
	if len(r.Header.Values("If-Match")) != 0 {
		expected, err := expectedVersion(r)
		if err != nil {
			return err
		}
		if expected != version {
			return fault.New(fault.Invalid, "path version differs from If-Match", nil)
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return fault.New(fault.Invalid, "body exceeds limit or cannot be read", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	body = bytes.TrimSpace(body)
	if body[0] != '{' {
		return fault.ErrInvalid
	}
	return decodeJSON(body, &struct{}{})
}

func mutationResponse(w http.ResponseWriter, result gquorum.MutationResult, err error) {
	if err != nil {
		// Staging may have succeeded before approval failed. Preserve that committed
		// version so an operator can inspect and apply it rather than repeat an edit.
		if result.Document.Version != 0 {
			w.Header().Set("X-Staged-Version", strconv.FormatUint(result.Document.Version, 10))
		}
		httpError(w, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", result.Document.Version))
	status := http.StatusOK
	if result.OperationID != "" {
		status = http.StatusAccepted
	}
	jsonResponse(w, status, result)
}
func (a *application) routes() http.Handler {
	mux := http.NewServeMux()
	a.registerClusterRoutes(mux)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, http.StatusOK, map[string]any{"node": a.cfg.NodeName})
	})
	mux.HandleFunc("GET /health/ready", a.readyHTTP)
	mux.HandleFunc("GET /api/cluster/parked", func(w http.ResponseWriter, r *http.Request) {
		parked, err := a.currentCluster().ParkedJoins(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, parked)
	})
	mux.HandleFunc("POST /api/nodes/{name}/readmit", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			gquorum.MutationOptions
			ExpectedMemberID uint64 `json:"expected_member_id"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.MutationOptions)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().ReadmitNode(r.Context(), r.PathValue("name"), body.ExpectedMemberID, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("GET /api/progress", func(w http.ResponseWriter, r *http.Request) {
		query := topology.ProgressQuery{GroupID: r.URL.Query().Get("group"), NodeName: r.URL.Query().Get("node"),
			OperationID: topology.OperationID(r.URL.Query().Get("operation")), After: r.URL.Query().Get("after")}
		if raw := r.URL.Query().Get("limit"); raw != "" {
			query.Limit, _ = strconv.Atoi(raw)
			if query.Limit == 0 {
				query.Limit = -1
			}
		}
		page, err := a.currentCluster().ReadProgress(r.Context(), query)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, page)
	})
	mux.HandleFunc("GET /api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := a.currentCluster().Status(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, status)
	})
	mux.HandleFunc("GET /api/topology/history", a.historyHTTP)
	mux.HandleFunc("GET /api/topology/{view}", func(w http.ResponseWriter, r *http.Request) {
		var doc topology.Document
		var err error
		switch r.PathValue("view") {
		case "latest":
			doc, err = a.currentCluster().Latest(r.Context())
		case "target":
			doc, err = a.currentCluster().Target(r.Context())
		case "active":
			doc, err = readConvergedTopology(r.Context(), a.currentCluster())
		default:
			var version uint64
			version, err = positiveNumber(r.PathValue("view"))
			if err == nil {
				doc, err = a.currentCluster().Get(r.Context(), version)
			}
		}
		if err != nil {
			httpError(w, err)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%d\"", doc.Version))
		jsonResponse(w, http.StatusOK, doc)
	})
	mux.HandleFunc("PUT /api/topology", func(w http.ResponseWriter, r *http.Request) {
		var doc topology.Document
		if err := requestBody(w, r, &doc); err != nil {
			httpError(w, err)
			return
		}
		version, err := expectedVersion(r)
		if err != nil {
			httpError(w, err)
			return
		}
		committed, err := a.currentCluster().Stage(r.Context(), doc, version)
		mutationResponse(w, gquorum.MutationResult{Document: committed}, err)
	})
	mux.HandleFunc("POST /api/topology/preview/plan", func(w http.ResponseWriter, r *http.Request) {
		var doc topology.Document
		if err := requestBody(w, r, &doc); err != nil {
			httpError(w, err)
			return
		}
		plan, err := a.currentCluster().PlanFor(r.Context(), doc)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, plan)
	})
	mux.HandleFunc("PUT /api/placement", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Placement topology.PlacementRule  `json:"placement"`
			Mutation  gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().SetPlacement(r.Context(), body.Placement, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/topology/{version}/plan", func(w http.ResponseWriter, r *http.Request) {
		version, err := positiveNumber(r.PathValue("version"))
		if err != nil {
			httpError(w, err)
			return
		}
		plan, err := a.currentCluster().Plan(r.Context(), version)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, plan)
	})
	mux.HandleFunc("POST /api/topology/{version}/approve", func(w http.ResponseWriter, r *http.Request) {
		version, err := positiveNumber(r.PathValue("version"))
		if err != nil {
			httpError(w, err)
			return
		}
		if err := approvalOptions(w, r, version); err != nil {
			httpError(w, fault.New(fault.Invalid, fmt.Sprintf("approve accepts no options; preview with POST /api/topology/%d/plan", version), err))
			return
		}
		id, err := a.currentCluster().Approve(r.Context(), version)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusAccepted, map[string]any{"operation_id": id})
	})
	mux.HandleFunc("POST /api/operations/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		id, err := a.currentCluster().RetryOperation(r.Context(), topology.OperationID(r.PathValue("id")))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusAccepted, map[string]any{"operation_id": id})
	})
	mux.HandleFunc("GET /api/operations/{id}", func(w http.ResponseWriter, r *http.Request) {
		status, err := a.currentCluster().Operation(r.Context(), topology.OperationID(r.PathValue("id")))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, projectOperationHTTPStatus(status))
	})
	mux.HandleFunc("POST /api/nodes", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Node     topology.Node           `json:"node"`
			Mutation gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().AddNode(r.Context(), body.Node, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/nodes/{name}/{action}", func(w http.ResponseWriter, r *http.Request) {
		var opts gquorum.MutationOptions
		if err := requestBody(w, r, &opts); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, opts)
		if err != nil {
			httpError(w, err)
			return
		}
		var result gquorum.MutationResult
		switch r.PathValue("action") {
		case "drain":
			result, err = a.currentCluster().DrainNode(r.Context(), r.PathValue("name"), opts)
		case "activate":
			result, err = a.currentCluster().ActivateNode(r.Context(), r.PathValue("name"), opts)
		case "exclude":
			result, err = a.currentCluster().ExcludeNode(r.Context(), r.PathValue("name"), opts)
		default:
			err = fault.ErrInvalid
		}
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/groups", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Group    gquorum.GroupSpec       `json:"group"`
			Mutation gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().CreateGroup(r.Context(), body.Group, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("DELETE /api/groups/{id}", func(w http.ResponseWriter, r *http.Request) {
		var opts gquorum.MutationOptions
		if err := requestBody(w, r, &opts); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, opts)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().DeleteGroup(r.Context(), r.PathValue("id"), opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("PUT /api/groups/{id}/placement", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Placement topology.PlacementRule  `json:"placement"`
			Mutation  gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().SetGroupPlacement(r.Context(), r.PathValue("id"), body.Placement, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("PUT /api/groups/{id}/voter-config", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			VoterConfig topology.VoterConfig    `json:"voter_config"`
			Mutation    gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().SetGroupVoterConfig(r.Context(), r.PathValue("id"), body.VoterConfig, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("PUT /api/groups/{id}/hosting", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Hosting  topology.HostingRule    `json:"hosting"`
			Mutation gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().SetGroupHosting(r.Context(), r.PathValue("id"), body.Hosting, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("PUT /api/groups/{id}/pinned-learner", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Replica  topology.ReplicaKey     `json:"replica"`
			Pinned   bool                    `json:"pinned"`
			Mutation gquorum.MutationOptions `json:"mutation"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		pin := topology.LearnerPin{Replica: body.Replica, Pinned: body.Pinned}
		result, err := a.currentCluster().SetReplicaPinnedLearner(r.Context(), r.PathValue("id"), pin, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/groups/{id}/leader/{name}", func(w http.ResponseWriter, r *http.Request) {
		if err := a.currentCluster().TransferLeader(r.Context(), r.PathValue("id"), r.PathValue("name")); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"transferred": true})
	})
	mux.HandleFunc("POST /api/nodes/{name}/force-retire", func(w http.ResponseWriter, r *http.Request) {
		var opts gquorum.RetireOptions
		if err := requestBody(w, r, &opts); err != nil {
			httpError(w, err)
			return
		}
		mutation, err := mutationOptions(r, opts.Mutation)
		if err != nil {
			httpError(w, err)
			return
		}
		opts.Mutation = mutation
		result, err := a.currentCluster().ForceRetireNode(r.Context(), r.PathValue("name"), opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/groups/{id}/replicas/{name}/reseat", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			gquorum.MutationOptions
			ReplicaID uint64 `json:"replica_id"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		opts, err := mutationOptions(r, body.MutationOptions)
		if err != nil {
			httpError(w, err)
			return
		}
		result, err := a.currentCluster().ReseatReplica(r.Context(), r.PathValue("id"), topology.ReplicaKey{NodeName: r.PathValue("name"), ReplicaID: body.ReplicaID}, opts)
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("PUT /api/load/{id}/keys/{key}", a.putHTTP)
	mux.HandleFunc("GET /api/load/{id}/keys/{key}", a.getHTTP)
	mux.HandleFunc("GET /api/load/{id}/read/{key}", a.readHTTP)
	mux.HandleFunc("GET /api/events", a.eventsHTTP)
	mux.HandleFunc("GET /api/events/watch", a.watchHTTP)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		begin := a.beginEffect
		if managementRoute(r.URL.Path) {
			begin = a.beginProcessEffect
		}
		ctx, done, err := begin(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		defer done()
		if a.cfg.GateFounders && a.currentNode() != nil && !a.currentNode().Status().Ready && (strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/health/ready") {
			httpError(w, fault.New(fault.Unavailable, a.foundingMessage(), nil))
			return
		}
		if r.URL.Path != "/api/events/watch" && !strings.HasPrefix(r.URL.Path, "/internal/test/watch/") {
			bounded, cancel := context.WithTimeout(ctx, clusterRequestTimeout(r))
			defer cancel()
			ctx = bounded
		}
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// operationHTTPStatus keeps compact process diagnostics beside the canonical
// operation response while callers migrate to per-block fields.
type operationHTTPStatus struct {
	topology.OperationStatus
	GroupID       string `json:"group_id,omitempty"`
	NodeName      string `json:"node_name,omitempty"`
	CurrentStep   int    `json:"current_step,omitempty"`
	TotalSteps    int    `json:"total_steps,omitempty"`
	WaitingReason string `json:"waiting_reason,omitempty"`
}

func projectOperationHTTPStatus(status topology.OperationStatus) operationHTTPStatus {
	projected := operationHTTPStatus{OperationStatus: status}
	for _, block := range status.Blocks {
		projected.TotalSteps += block.TotalSteps
	}
	var selected *topology.BlockStatus
	for i := range status.Blocks {
		if status.Blocks[i].State != topology.OperationSucceeded {
			selected = &status.Blocks[i]
			break
		}
	}
	if selected == nil && len(status.Blocks) != 0 {
		selected = &status.Blocks[len(status.Blocks)-1]
	}
	if selected != nil {
		projected.GroupID, projected.CurrentStep = selected.GroupID, selected.CurrentStep
		if selected.Blocked != nil {
			projected.NodeName, projected.WaitingReason = selected.Blocked.NodeName, selected.Blocked.Message
		}
	}
	return projected
}
func (a *application) readyHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.canServe(r.Context()) {
		httpError(w, fault.ErrUnavailable)
		return
	}
	target, err := a.currentCluster().Target(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}
	if target.Version == 0 || target.MemberID(a.cfg.NodeName) != a.currentNode().Identity().MemberID {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	if node := target.Node(a.cfg.NodeName); node == nil || node.State != topology.NodeActive {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	var groups []string
	for _, group := range target.LoadGroups() {
		for _, replica := range group.Replicas {
			if replica.NodeName == a.cfg.NodeName {
				if _, err := a.localServing(r.Context(), group.ID); err != nil {
					httpError(w, err)
					return
				}
				groups = append(groups, group.ID)
			}
		}
	}
	// Initial founding has no load declarations; bootstrap readiness must wait for
	// the groups it promises. A retired demo ID is intentionally allowed later.
	if a.cfg.Bootstrap && target.Version == 1 {
		httpError(w, fault.ErrReplicaNotReady)
		return
	}
	jsonResponse(w, http.StatusOK, map[string]any{"node": a.cfg.NodeName, "session_live": a.currentNode().OwnSessionLive(), "serve_override": a.currentNode().ServeOverride(), "groups": groups, "target_version": target.Version})
}
func (a *application) historyHTTP(w http.ResponseWriter, r *http.Request) {
	offset, limit := 0, 100
	var err error
	if text := r.URL.Query().Get("offset"); text != "" {
		offset, err = strconv.Atoi(text)
		if err != nil {
			httpError(w, fault.ErrInvalid)
			return
		}
	}
	if text := r.URL.Query().Get("limit"); text != "" {
		limit, err = strconv.Atoi(text)
		if err != nil {
			httpError(w, fault.ErrInvalid)
			return
		}
	}
	if offset < 0 || limit < 1 || limit > 500 {
		httpError(w, fault.ErrInvalid)
		return
	}
	result, err := a.currentCluster().History(r.Context(), topology.Page{Offset: offset, Limit: limit})
	if err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, result)
}
func (a *application) putHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RequestID string `json:"request_id"`
		Value     string `json:"value"`
	}
	if err := requestBody(w, r, &body); err != nil {
		httpError(w, err)
		return
	}
	if body.RequestID == "" || len(body.RequestID) > 128 || r.PathValue("key") == "" || len(r.PathValue("key")) > 256 || len(body.Value) > 1<<16 {
		httpError(w, fault.ErrInvalid)
		return
	}
	group := r.PathValue("id")
	if err := topology.ValidateLoadID(group); err != nil {
		httpError(w, err)
		return
	}
	// An ingress may be an observer without this load group. Only the exact
	// serving voter on the observed leader accepts the replicated proposal.
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		status, err := a.groupStatus(r.Context(), group)
		if err == nil && status.Found && status.LeaderID != 0 {
			name, ownerErr := a.owner(r.Context(), group, status.LeaderID)
			err = ownerErr
			if err == nil {
				reply, callErr := a.callNode(r.Context(), name, "Put", group, status.LeaderID, putCommand{RequestID: body.RequestID, Key: r.PathValue("key"), Value: body.Value})
				err = callErr
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(reply.Data)
					return
				}
			}
		} else if err == nil {
			err = fault.ErrReplicaNotReady
		}
		last = err
		if !errors.Is(err, fault.ErrNotLeader) && !errors.Is(err, fault.ErrUnavailable) {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-r.Context().Done():
			timer.Stop()
			httpError(w, r.Context().Err())
			return
		case <-timer.C:
		}
	}
	httpError(w, last)
}
func (a *application) getHTTP(w http.ResponseWriter, r *http.Request) {
	if err := topology.ValidateLoadID(r.PathValue("id")); err != nil {
		httpError(w, err)
		return
	}
	local, err := a.localServing(r.Context(), r.PathValue("id"))
	if err != nil {
		httpError(w, err)
		return
	}
	value, found, index := local.machine.get(r.PathValue("key"))
	status := http.StatusOK
	if !found {
		status = http.StatusNotFound
	}
	jsonResponse(w, status, map[string]any{"key": r.PathValue("key"), "value": value, "found": found, "applied_index": index, "consistency": "local", "node": a.cfg.NodeName})
}
func (a *application) readHTTP(w http.ResponseWriter, r *http.Request) {
	if err := topology.ValidateLoadID(r.PathValue("id")); err != nil {
		httpError(w, err)
		return
	}
	local, err := a.localServing(r.Context(), r.PathValue("id"))
	if err != nil {
		httpError(w, err)
		return
	}
	query, err := json.Marshal(getQuery{Key: r.PathValue("key")})
	if err != nil {
		httpError(w, err)
		return
	}
	result, err := local.group.Read(r.Context(), query)
	if err != nil {
		httpError(w, err)
		return
	}
	if result.Code != "ok" {
		httpError(w, fault.New(fault.Code(result.Code), "load query refused", nil))
		return
	}
	var value getResult
	if err := decodeJSON(result.Data, &value); err != nil {
		httpError(w, err)
		return
	}
	status := http.StatusOK
	if !value.Found {
		status = http.StatusNotFound
	}
	jsonResponse(w, status, map[string]any{"key": value.Key, "value": value.Value, "found": value.Found, "applied_index": value.AppliedIndex, "consistency": "linearizable", "node": a.cfg.NodeName})
}

func eventQuery(r *http.Request) (events.Query, error) {
	q := events.Query{Limit: 100, Filter: events.Filter{GroupID: r.URL.Query().Get("group")}}
	var err error
	if text := r.URL.Query().Get("after"); text != "" {
		q.AfterSeq, err = strconv.ParseUint(text, 10, 64)
		if err != nil {
			return q, fault.ErrInvalid
		}
	}
	if text := r.URL.Query().Get("limit"); text != "" {
		q.Limit, err = strconv.Atoi(text)
		if err != nil || q.Limit < 1 || q.Limit > 500 {
			return q, fault.ErrInvalid
		}
	}
	for _, text := range r.URL.Query()["type"] {
		if text != "" {
			q.Filter.Types = append(q.Filter.Types, events.Type(text))
		}
	}
	return q, nil
}
func (a *application) eventsHTTP(w http.ResponseWriter, r *http.Request) {
	q, err := eventQuery(r)
	if err != nil {
		httpError(w, err)
		return
	}
	result, err := a.journal.List(r.Context(), q)
	if err != nil {
		httpError(w, err)
		return
	}
	next := q.AfterSeq
	if len(result) > 0 {
		next = result[len(result)-1].Seq
	}
	jsonResponse(w, http.StatusOK, map[string]any{"node": a.cfg.NodeName, "events": result, "next_cursor": next})
}
func (a *application) watchHTTP(w http.ResponseWriter, r *http.Request) {
	q, err := eventQuery(r)
	if err != nil {
		httpError(w, err)
		return
	}
	// This stream is live-only; journal pagination is the separate replay API.
	if q.AfterSeq != 0 || r.Header.Get("Last-Event-ID") != "" {
		httpError(w, fault.New(fault.Invalid, "watch is live-only; use /api/events for replay", nil))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	deliveries, err := a.bus.Subscribe(ctx, q.Filter, 64)
	if err != nil {
		httpError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	controller := http.NewResponseController(w)
	send := func(data string) error {
		if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if _, err := io.WriteString(w, data); err != nil {
			return err
		}
		return controller.Flush()
	}
	if err := send(": live subscription; journal cursors are node-local\n\n"); err != nil {
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			data, err := json.Marshal(map[string]any{"event": delivery.Event, "lost_before": delivery.LostBefore})
			if err != nil {
				return
			}
			// A journal append failure produces seq=0; always expose it as live-only.
			id := ""
			if delivery.Event.Seq != 0 {
				id = fmt.Sprintf("id: %d\n", delivery.Event.Seq)
			}
			if err := send(id + "event: cluster\ndata: " + string(data) + "\n\n"); err != nil {
				return
			}
		case <-ticker.C:
			if err := send(": heartbeat\n\n"); err != nil {
				return
			}
		}
	}
}
