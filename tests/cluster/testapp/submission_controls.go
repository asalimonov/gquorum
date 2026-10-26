package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// This envelope belongs only to the authenticated process-test application.
// It is not a library wire format or a remote GroupClient implementation.
type submissionReply struct {
	Submission load.Submission `json:"submission"`
	ErrorCode  string          `json:"error_code,omitempty"`
	Error      string          `json:"error,omitempty"`
}

func (a *application) registerSubmissionRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("POST /internal/test/submission/{group}/{replica}", protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		g, found := a.currentNode().LoadGroup(group)
		if !found || fmt.Sprint(g.ReplicaID()) != r.PathValue("replica") {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		var command putCommand
		if err := requestBody(w, r, &command); err != nil {
			httpError(w, err)
			return
		}
		data, err := json.Marshal(command)
		if err != nil {
			httpError(w, err)
			return
		}
		s, err := g.Propose(r.Context(), load.Proposal{RequestID: command.RequestID, Data: data})
		reply := submissionReply{Submission: s}
		if err != nil {
			reply.Error = err.Error()
			var failure *fault.Error
			if errors.As(err, &failure) {
				reply.ErrorCode = string(failure.Code)
			}
		}
		jsonResponse(w, http.StatusOK, reply)
	}))
}
