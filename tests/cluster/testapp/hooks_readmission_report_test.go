//go:build !functional

package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/protobuf/encoding/protowire"
)

func readmissionReportFrame(t *testing.T, instance string) proxyFrame {
	t.Helper()
	field := func(n protowire.Number, data []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), data)
	}
	header := field(1, []byte("cluster"))
	header = append(header, field(2, []byte("returning"))...)
	header = protowire.AppendVarint(protowire.AppendTag(header, 3, protowire.VarintType), 4)
	header = append(header, field(6, []byte(instance))...)
	data, err := json.Marshal(map[string]any{"NodeName": "returning", "MemberID": 4, "InstanceID": instance,
		"ExecutionID": "exec-v4-a1", "Address": "address", "receipts": []topology.EffectReceipt{
			{Key: topology.AuthorityKey{GroupID: topology.MetaGroupID}}, {Key: topology.AuthorityKey{GroupID: "work"}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	return append(field(1, header), field(2, data)...)
}

func TestReadmissionReportKeepsPartialBatchBoundToExactIncarnation(t *testing.T) {
	t.Parallel()
	a := &application{clusterHooks: newClusterHooks()}
	a.clusterHooks.readmissionReport = readmissionReportControl{NodeName: "returning", MemberID: 4, OperationID: "exec-v4-a1", Held: true}
	frame := readmissionReportFrame(t, "return-run")
	original := bytes.Clone(frame)
	partial, err := a.partialReadmissionReport(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, frame) {
		t.Fatal("partial forwarding modified source frame")
	}
	data, err := proxyBytesField(partial, 2)
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Address  string
		Receipts []topology.EffectReceipt `json:"receipts"`
	}
	if err = json.Unmarshal(data, &view); err != nil || view.Address != "address" || len(view.Receipts) != 1 || view.Receipts[0].Key.GroupID != "work" {
		t.Fatal(view, err)
	}
	if a.clusterHooks.readmissionReport.Hits != 1 || a.clusterHooks.readmissionReport.InstanceID != "return-run" {
		t.Fatal(a.clusterHooks.readmissionReport)
	}
	foreign := readmissionReportFrame(t, "successor-run")
	passed, err := a.partialReadmissionReport(foreign)
	if err != nil || !bytes.Equal(passed, foreign) || a.clusterHooks.readmissionReport.Hits != 1 {
		t.Fatal("control reached successor", err)
	}
	a.clusterHooks.readmissionReport.Held = false
	passed, err = a.partialReadmissionReport(frame)
	if err != nil || !bytes.Equal(passed, frame) {
		t.Fatal("release changed full batch", err)
	}
}
