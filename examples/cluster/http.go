package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

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
	case fault.VersionConflict, fault.GroupRetired, fault.IdentityMismatch, fault.OperatorAuthorization:
		status = http.StatusConflict
	case fault.UnsafeMembership:
		status = http.StatusUnprocessableEntity
	case fault.HistoryUnavailable:
		status = http.StatusNotFound
	case fault.NotLeader, fault.ReplicaNotReady, fault.Unavailable, fault.Stopped, fault.Overloaded, fault.NoController:
		status = http.StatusServiceUnavailable
	}
	body := map[string]any{"code": code, "error": err.Error()}
	if typed != nil && code == fault.VersionConflict {
		body["expected_version"], body["current_version"] = typed.ExpectedVersion, typed.CurrentVersion
	}
	jsonResponse(w, status, body)
}
func requestBody(w http.ResponseWriter, r *http.Request, out any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return fault.New(fault.Invalid, "body exceeds limit or cannot be read", err)
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
	return decodeJSON(body, &struct{}{})
}

func mutationResponse(w http.ResponseWriter, result gquorum.MutationResult, err error) {
	if err != nil {
		// Staging may have succeeded before approval failed. Preserve that committed
		// version so an operator can inspect and approve it rather than repeat an edit.
		if result.Document.Version != 0 {
			w.Header().Set("X-Staged-Version", strconv.FormatUint(result.Document.Version, 10))
		}
		httpError(w, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", result.Document.Version))
	status := http.StatusOK
	if result.OperatorID != "" {
		status = http.StatusAccepted
	}
	jsonResponse(w, status, result)
}

func decodeJSON(data []byte, out any) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return fault.New(fault.Invalid, "JSON object required", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return fault.New(fault.Invalid, "invalid JSON", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fault.ErrInvalid
	}
	return nil
}

func routes(node gquorum.Node) http.Handler {
	cluster := node.Cluster()
	name := node.Identity().NodeName
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, map[string]string{"node": name}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		status := node.Status()
		if !status.Ready || !status.SessionLive {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		active, err := cluster.Active(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		member := active.Node(name)
		if member == nil || member.State != topology.NodeActive {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		jsonResponse(w, 200, map[string]any{"node": name, "session_live": true, "active_version": active.Version})
	})
	mux.HandleFunc("GET /api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := cluster.Status(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, status)
	})
	for path, read := range map[string]func(context.Context) (topology.Document, error){
		"GET /api/topology/latest": cluster.Latest,
		"GET /api/topology/target": cluster.Target,
		"GET /api/topology/active": cluster.Active,
	} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			document, err := read(r.Context())
			if err != nil {
				httpError(w, err)
				return
			}
			w.Header().Set("ETag", fmt.Sprintf("\"%d\"", document.Version))
			jsonResponse(w, 200, document)
		})
	}
	mutate := func(change func(context.Context, string, gquorum.MutationOptions) (gquorum.MutationResult, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var options gquorum.MutationOptions
			if err := requestBody(w, r, &options); err != nil {
				httpError(w, err)
				return
			}
			options, err := mutationOptions(r, options)
			if err != nil {
				httpError(w, err)
				return
			}
			result, err := change(r.Context(), r.PathValue("name"), options)
			mutationResponse(w, result, err)
		}
	}
	mux.HandleFunc("POST /api/nodes/{name}/drain", mutate(cluster.DrainNode))
	mux.HandleFunc("POST /api/nodes/{name}/activate", mutate(cluster.ActivateNode))
	mux.HandleFunc("POST /api/topology/{version}/approve", func(w http.ResponseWriter, r *http.Request) {
		version, err := positiveNumber(r.PathValue("version"))
		if err != nil {
			httpError(w, err)
			return
		}
		if err := approvalOptions(w, r, version); err != nil {
			httpError(w, err)
			return
		}
		id, err := cluster.Approve(r.Context(), version)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 202, map[string]any{"operator_id": id})
	})
	mux.HandleFunc("GET /api/operators/{id}", func(w http.ResponseWriter, r *http.Request) {
		operator, err := cluster.Operator(r.Context(), topology.OperatorID(r.PathValue("id")))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, operator)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
