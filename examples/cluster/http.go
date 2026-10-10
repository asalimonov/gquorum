package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
	"io"
	"net/http"
	"strconv"
	"time"
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
	case fault.VersionConflict, fault.GroupRetired, fault.IdentityMismatch, fault.ExecutionAuthorization:
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
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n == 0 {
		return 0, fault.New(fault.Invalid, "positive version required", err)
	}
	return n, nil
}
func expectedVersion(r *http.Request) (uint64, error) {
	value := r.Header.Get("If-Match")
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}
	if value == "" {
		return 0, fault.New(fault.Invalid, "If-Match with the observed Latest version is required", nil)
	}
	return positiveNumber(value)
}
func mutationOptions(r *http.Request, opts meta.MutationOptions) (meta.MutationOptions, error) {
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
func mutationResponse(w http.ResponseWriter, result meta.MutationResult, err error) {
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
	if result.ExecutionID != "" {
		status = http.StatusAccepted
	}
	jsonResponse(w, status, result)
}

func decodeJSON(data []byte, out any) error {
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

func routes(group *meta.Group, name string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, map[string]string{"node": name}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		active, err := group.Active(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		node := active.Node(name)
		if !group.OwnSessionLive() || node == nil || node.State != topology.NodeActive {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		jsonResponse(w, 200, map[string]any{"node": name, "session_live": true, "active_version": active.Version})
	})
	mux.HandleFunc("GET /api/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := group.Status(r.Context())
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, status)
	})
	mux.HandleFunc("GET /api/topology/{view}", func(w http.ResponseWriter, r *http.Request) {
		var document topology.Document
		var err error
		switch r.PathValue("view") {
		case "latest":
			document, err = group.Latest(r.Context())
		case "target":
			document, err = group.Target(r.Context())
		case "active":
			document, err = group.Active(r.Context())
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			httpError(w, err)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%d\"", document.Version))
		jsonResponse(w, 200, document)
	})
	mux.HandleFunc("POST /api/nodes/{name}/{action}", func(w http.ResponseWriter, r *http.Request) {
		var options meta.MutationOptions
		if err := requestBody(w, r, &options); err != nil {
			httpError(w, err)
			return
		}
		options, err := mutationOptions(r, options)
		if err != nil {
			httpError(w, err)
			return
		}
		var result meta.MutationResult
		switch r.PathValue("action") {
		case "drain":
			result, err = group.DrainNode(r.Context(), r.PathValue("name"), options)
		case "activate":
			result, err = group.ActivateNode(r.Context(), r.PathValue("name"), options)
		default:
			http.NotFound(w, r)
			return
		}
		mutationResponse(w, result, err)
	})
	mux.HandleFunc("POST /api/topology/{version}/apply", func(w http.ResponseWriter, r *http.Request) {
		version, err := positiveNumber(r.PathValue("version"))
		if err != nil {
			httpError(w, err)
			return
		}
		id, err := group.Apply(r.Context(), version)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 202, map[string]any{"execution_id": id})
	})
	mux.HandleFunc("GET /api/executions/{id}", func(w http.ResponseWriter, r *http.Request) {
		execution, err := group.Execution(r.Context(), topology.ExecutionID(r.PathValue("id")))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, execution)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
