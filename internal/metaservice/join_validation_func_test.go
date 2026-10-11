//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionReplyCarriesApprovedFormerReplicasWithoutLocalMetaDisk(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	for i := range doc.Groups {
		for j := range doc.Groups[i].Replicas {
			if doc.Groups[i].Replicas[j].NodeName == "n1" {
				doc.Groups[i].Replicas[j].ReplicaID = 4
			}
		}
		doc.Groups[i].NextReplicaID = 5
	}
	f.commit(t, doc, true)
	staged := f.g.machine.Latest()
	staged.Groups = append(staged.Groups, topology.Group{ID: "unapproved", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	f.commit(t, staged, false)
	reply := f.g.joinReply(t.Context(), metaengine.AdmissionRecord{NodeName: "n1", InstanceID: "fresh-process", MemberID: 4, OperatorID: topology.MakeOperatorID(2, 1)}, true)
	if reply.ErrorCode != "" {
		t.Fatal(reply)
	}
	var result joinResponse
	if err := json.Unmarshal(reply.Data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.PreviousGroups) != 1 || result.PreviousGroups[0].ID != "work" || len(result.PreviousGroups[0].Replicas) != 1 || result.PreviousGroups[0].Replicas[0].NodeName != "n1" || result.PreviousGroups[0].Replicas[0].ReplicaID != 1 {
		t.Fatal("readmission reply lost old approval or included staged/granted/foreign identities", result.PreviousGroups)
	}
}

func TestJoiningCannotClaimUsedMemberWithoutPinnedStore(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path := filepath.Join(t.TempDir(), "unopened.bolt")
	opts := Options{Identity: topology.Identity{NodeName: "one", ClusterID: "cluster", InstanceID: "fresh-process", MemberID: 1}, Node: topology.Node{Name: "one", Type: "worker"}, DBPath: path, Join: true, JoinSeeds: []string{"127.0.0.1:1"}, Network: NetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test-credential")}}
	group, err := New(opts)
	if group != nil {
		group.Stop(context.Background())
	}
	if !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("unbacked retained identity accepted", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("identity refusal touched storage", err)
	}
}
