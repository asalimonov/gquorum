package main

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/protobuf/encoding/protowire"
)

// One exact admission per process; the first authenticated matching packet binds
// its process incarnation. Only the metadata receipt is withheld from its batch.
type readmissionReportControl struct {
	NodeName    string               `json:"node_name"`
	MemberID    uint64               `json:"member_id"`
	OperationID topology.OperationID `json:"operation_id"`
	InstanceID  string               `json:"instance_id"`
	Held        bool                 `json:"held"`
	Hits        uint64               `json:"hits"`
}

func (a *application) registerReadmissionReportRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("PUT /internal/test/readmission-report", protect(func(w http.ResponseWriter, r *http.Request) {
		var body readmissionReportControl
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if a.metaProxy == nil || body.NodeName == "" || body.MemberID == 0 || body.OperationID == "" {
			httpError(w, fault.ErrInvalid)
			return
		}
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		if a.clusterHooks.readmissionReport.Held {
			httpError(w, fault.ErrOverloaded)
			return
		}
		body.Held, body.Hits, body.InstanceID = true, 0, ""
		a.clusterHooks.readmissionReport = body
		jsonResponse(w, 200, body)
	}))
	mux.HandleFunc("GET /internal/test/readmission-report", protect(func(w http.ResponseWriter, _ *http.Request) {
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		jsonResponse(w, 200, a.clusterHooks.readmissionReport)
	}))
	mux.HandleFunc("DELETE /internal/test/readmission-report", protect(func(w http.ResponseWriter, _ *http.Request) {
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		a.clusterHooks.readmissionReport.Held = false
		jsonResponse(w, 200, a.clusterHooks.readmissionReport)
	}))
}

func (a *application) partialReadmissionReport(frame proxyFrame) (proxyFrame, error) {
	data, err := proxyBytesField(frame, 2)
	if err != nil {
		return nil, err
	}
	var request struct {
		NodeName, InstanceID string
		MemberID             uint64
		OperationID          topology.OperationID     `json:"ExecutionID"`
		Receipts             []topology.EffectReceipt `json:"receipts"`
	}
	if err = json.Unmarshal(data, &request); err != nil {
		return nil, err
	}
	if len(request.Receipts) == 0 {
		return frame, nil
	}
	header, err := proxyBytesField(frame, 1)
	if err != nil {
		return nil, err
	}
	member, err := proxyVarintField(header, 3)
	if err != nil {
		return nil, err
	}
	node, err := proxyBytesField(header, 2)
	if err != nil {
		return nil, err
	}
	instance, err := proxyBytesField(header, 6)
	if err != nil {
		return nil, err
	}
	if member != request.MemberID || string(node) != request.NodeName || string(instance) != request.InstanceID || request.InstanceID == "" {
		return nil, fault.ErrIdentityMismatch
	}
	a.clusterHooks.mu.Lock()
	defer a.clusterHooks.mu.Unlock()
	control := &a.clusterHooks.readmissionReport
	if !control.Held || control.NodeName != request.NodeName || control.MemberID != request.MemberID ||
		control.OperationID != request.OperationID || control.InstanceID != "" && control.InstanceID != request.InstanceID {
		return frame, nil
	}
	control.InstanceID = request.InstanceID
	control.Hits++
	receipts := slices.DeleteFunc(request.Receipts, func(r topology.EffectReceipt) bool { return r.Key.GroupID == topology.MetaGroupID })
	var object map[string]json.RawMessage
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	object["receipts"], err = json.Marshal(receipts)
	if err != nil {
		return nil, err
	}
	data, err = json.Marshal(object)
	if err != nil {
		return nil, err
	}
	out := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), header)
	return protowire.AppendBytes(protowire.AppendTag(out, 2, protowire.BytesType), data), nil
}
