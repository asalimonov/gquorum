package main

import (
	"net/http"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
)

type prepareRecoveryBody struct {
	ID      string           `json:"id"`
	Request recovery.Request `json:"request"`
}

func (a *application) registerRecoveryRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("PUT /internal/test/recovery/controls", protect(a.recoveryControlsHTTP))
	mux.HandleFunc("GET /internal/test/recovery/metrics", protect(a.recoveryMetricsHTTP))
	mux.HandleFunc("POST /internal/test/recovery/prepare", protect(a.prepareRecoveryHTTP))
	mux.HandleFunc("POST /internal/test/recovery/{id}/{action}", protect(a.recoveryCommandHTTP))
	mux.HandleFunc("GET /internal/test/recovery/{id}", protect(a.recoveryJobHTTP))
	mux.HandleFunc("GET /internal/test/recovery", protect(a.recoveryListHTTP))
}
func (a *application) recoveryAvailable(w http.ResponseWriter) bool {
	if !a.cfg.NodeOwnedWAL || a.recoveryJobs == nil {
		httpError(w, fault.ErrUnsupported)
		return false
	}
	return true
}
func (a *application) prepareRecoveryHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.recoveryAvailable(w) {
		return
	}
	var body prepareRecoveryBody
	if err := requestBody(w, r, &body); err != nil {
		httpError(w, err)
		return
	}
	if err := a.recoveryJobs.prepare(body.ID, body.Request); err != nil {
		httpError(w, err)
		return
	}
	job, err := a.recoveryJobs.view(r.Context(), body.ID)
	if err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusAccepted, job)
}
func (a *application) recoveryCommandHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.recoveryAvailable(w) {
		return
	}
	var acceptance recovery.Acceptance
	var cancellation recoveryCancellation
	var err error
	switch r.PathValue("action") {
	case "confirm":
		err = requestBody(w, r, &acceptance)
	case "cancel":
		err = requestBody(w, r, &cancellation)
	case "retry":
	default:
		err = fault.ErrInvalid
	}
	if err != nil {
		httpError(w, err)
		return
	}
	if err := a.recoveryJobs.command(r.PathValue("id"), r.PathValue("action"), acceptance, cancellation); err != nil {
		httpError(w, err)
		return
	}
	job, err := a.recoveryJobs.view(r.Context(), r.PathValue("id"))
	if err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusAccepted, job)
}
func (a *application) recoveryJobHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.recoveryAvailable(w) {
		return
	}
	job, err := a.recoveryJobs.view(r.Context(), r.PathValue("id"))
	if err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, job)
}
func (a *application) recoveryListHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.recoveryAvailable(w) {
		return
	}
	handle, err := a.recoveryJobs.handle()
	if err != nil {
		httpError(w, err)
		return
	}
	statuses, err := handle.List(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, map[string]any{"job": a.recoveryJobs.snapshot(), "operations": statuses})
}
