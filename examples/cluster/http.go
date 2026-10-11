package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type healthcheckCommand struct {
	URL     string        `arg:"" help:"Readiness URL to probe."`
	Timeout time.Duration `default:"2s" help:"Probe timeout."`
}

func (cmd *healthcheckCommand) Run() error {
	if cmd.Timeout <= 0 {
		return fmt.Errorf("healthcheck timeout must be positive")
	}
	client := http.Client{Timeout: cmd.Timeout}
	response, err := client.Get(cmd.URL)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness: HTTP %d", response.StatusCode)
	}
	return nil
}

func serveHTTP(ctx context.Context, address string, handler http.Handler) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	slog.Info("example listening", "http", listener.Addr())
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		<-serveDone
		return err
	}
	<-serveDone
	return nil
}

type response struct {
	status  int
	headers http.Header
	body    any
}

// endpoint writes headers even on failure, preserving a successfully staged
// version when subsequent approval fails. Domain errors share one HTTP mapping.
func endpoint(call func(*http.Request) (response, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result, err := call(r)
		for name, values := range result.headers {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		if err != nil {
			httpError(w, err)
			return
		}
		if result.status == 0 {
			result.status = http.StatusOK
		}
		jsonResponse(w, result.status, result.body)
	}
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

func requestBody(r *http.Request, out any) error {
	body, err := io.ReadAll(r.Body)
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

func approvalOptions(r *http.Request, version uint64) error {
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fault.New(fault.Invalid, "body exceeds limit or cannot be read", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	return decodeJSON(body, &struct{}{})
}

func mutationResponse(result gquorum.MutationResult, err error) (response, error) {
	if err != nil {
		headers := http.Header{}
		if result.Document.Version != 0 {
			headers.Set("X-Staged-Version", strconv.FormatUint(result.Document.Version, 10))
		}
		return response{headers: headers}, err
	}
	status := http.StatusOK
	if result.OperatorID != "" {
		status = http.StatusAccepted
	}
	return response{status: status, headers: http.Header{"ETag": {fmt.Sprintf("\"%d\"", result.Document.Version)}}, body: result}, nil
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
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", endpoint(func(r *http.Request) (response, error) {
		return response{body: map[string]string{"node": node.Identity().NodeName}}, nil
	}))
	mux.HandleFunc("GET /health/ready", endpoint(func(r *http.Request) (response, error) {
		ready, err := readReadiness(r.Context(), node)
		return response{body: ready}, err
	}))
	mux.HandleFunc("GET /api/cluster/status", endpoint(func(r *http.Request) (response, error) {
		status, err := cluster.Status(r.Context())
		return response{body: status}, err
	}))
	for path, read := range map[string]func(context.Context) (topology.Document, error){
		"GET /api/topology/latest": cluster.Latest,
		"GET /api/topology/target": cluster.Target,
		"GET /api/topology/active": cluster.Active,
	} {
		mux.HandleFunc(path, endpoint(func(r *http.Request) (response, error) {
			document, err := read(r.Context())
			if err != nil {
				return response{}, err
			}
			return response{headers: http.Header{"ETag": {fmt.Sprintf("\"%d\"", document.Version)}}, body: document}, nil
		}))
	}
	mutate := func(change func(context.Context, string, gquorum.MutationOptions) (gquorum.MutationResult, error)) http.HandlerFunc {
		return endpoint(func(r *http.Request) (response, error) {
			var options gquorum.MutationOptions
			if err := requestBody(r, &options); err != nil {
				return response{}, err
			}
			options, err := mutationOptions(r, options)
			if err != nil {
				return response{}, err
			}
			return mutationResponse(change(r.Context(), r.PathValue("name"), options))
		})
	}
	mux.HandleFunc("POST /api/nodes/{name}/drain", mutate(cluster.DrainNode))
	mux.HandleFunc("POST /api/nodes/{name}/activate", mutate(cluster.ActivateNode))
	mux.HandleFunc("POST /api/topology/{version}/approve", endpoint(func(r *http.Request) (response, error) {
		version, err := positiveNumber(r.PathValue("version"))
		if err != nil {
			return response{}, err
		}
		if err := approvalOptions(r, version); err != nil {
			return response{}, err
		}
		id, err := cluster.Approve(r.Context(), version)
		return response{status: http.StatusAccepted, body: map[string]any{"operator_id": id}}, err
	}))
	mux.HandleFunc("GET /api/operators/{id}", endpoint(func(r *http.Request) (response, error) {
		operator, err := cluster.Operator(r.Context(), topology.OperatorID(r.PathValue("id")))
		return response{body: operator}, err
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
