//go:build !functional

package task

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/load"
)

func TestCommandCodecIsStrictAndBounded(t *testing.T) {
	t.Parallel()
	p, err := Encode("start", Start{Key: Key{TaskID: 1, Attempt: 1}, Executor: Executor{ReplicaID: 2, NodeName: "node2"}})
	if err != nil || !strings.HasPrefix(p.RequestID, "start/") {
		t.Fatal(p, err)
	}
	again, _ := Encode("start", Start{Key: Key{TaskID: 1, Attempt: 1}, Executor: Executor{ReplicaID: 2, NodeName: "node2"}})
	if again.RequestID != p.RequestID {
		t.Fatal("request ID is not deterministic")
	}
	if _, err := Encode("unknown", struct{}{}); err == nil {
		t.Fatal("unknown kind encoded")
	}
	for _, data := range []string{
		`null`, `{} {}`, `{"version":1,"kind":"start","body":{}}`,
		`{"version":2,"kind":"start","body":{"sql":"DROP TABLE tasks"}}`,
		`{"version":2,"kind":"start","body":{},"extra":true}`,
		strings.Repeat(" ", maxCommandBytes+1),
	} {
		if _, err := Decode(load.Command{Data: []byte(data)}); err == nil {
			t.Fatalf("malformed command accepted: %.40s", data)
		}
	}
	for _, hash := range []string{strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("a", 63)} {
		if CanonicalHash(hash) {
			t.Fatal("noncanonical hash accepted")
		}
	}
	if !CanonicalHash(strings.Repeat("a", 64)) {
		t.Fatal("valid hash refused")
	}
	if ValidExecutor(Executor{ReplicaID: math.MaxUint64, NodeName: "n"}) || ValidExecutor(Executor{ReplicaID: 1, NodeName: " n"}) || !ValidExecutor(Executor{ReplicaID: 1, NodeName: "n"}) {
		t.Fatal("executor bounds")
	}
	if _, err := CheckedInt(math.MaxUint64); err == nil {
		t.Fatal("unsigned overflow accepted")
	}
}

func TestReferencedIDs(t *testing.T) {
	t.Parallel()
	assign, _ := json.Marshal(Assign{Tasks: []Assignment{{TaskID: 3}, {TaskID: 5}}})
	revoke, _ := json.Marshal(Revoke{Tasks: []Revocation{{Key: Key{TaskID: 7, Attempt: 1}}}})
	finish, _ := json.Marshal(Finish{Key: Key{TaskID: 9, Attempt: 2}})
	cases := map[string]struct {
		c    Command
		want []int64
	}{
		"assign": {Command{Kind: "assign", Body: assign}, []int64{3, 5}},
		"revoke": {Command{Kind: "revoke", Body: revoke}, []int64{7}},
		"finish": {Command{Kind: "finish", Body: finish}, []int64{9}},
		"create": {Command{Kind: "create", Body: json.RawMessage(`{}`)}, nil},
	}
	for name, c := range cases {
		got := ReferencedIDs(c.c)
		if len(got) != len(c.want) {
			t.Fatalf("%s: %v", name, got)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: %v", name, got)
			}
		}
	}
}
