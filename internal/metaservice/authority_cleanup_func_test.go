//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

// This protocol double models committed proof responses from a remote metadata
// owner. The real leader-confirmed read path is covered by authority_reads tests.
type authorityQueryProtocol struct {
	*Group
	machine *metaengine.Machine
}

func (p authorityQueryProtocol) Call(ctx context.Context, method string, _ *metapb.Header, data []byte) *metapb.Reply {
	var req authorityReadRequest
	if method != "AuthorityRead" || json.Unmarshal(data, &req) != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	query, _ := json.Marshal(metaengine.QueryRequest{Kind: "authority_proof", AuthorityKey: &req.Key, Key: req.CourierNode})
	result, err := p.machine.Query(ctx, query)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: result.Data}
}

func TestFencedCleanupUsesRemoteGrantAndPreservesSuccessorEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"cleanup", "successor_replica", "successor_pin", "unknown_grant", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, action := cleanupLedgerFixture(t)
			grant := *f.command(t, metaengine.BeginCleanupEffectProposal{Identity: fixtureLoadIdentity(), Action: action, ProposalID: newProposalID()}).Grant
			credential := []byte("authority-cleanup-test")
			address := joinProtocolServer(t, authorityQueryProtocol{machine: f.g.machine}, credential)
			client := joinProtocolClient(t, []string{address}, func(o *Options) { o.Network.ClusterCredential = credential })
			client.mu.Lock()
			client.identity.MemberID, client.started = 1, true
			client.ctx, client.cancel = context.WithCancel(t.Context())
			client.workCtx, client.workCancel = context.WithCancel(client.ctx)
			client.mu.Unlock()
			client.transport.SetMemberID(1)
			if err := client.FenceParticipation(t.Context()); err != nil {
				t.Fatal(err)
			}
			s, err := metastore.Open(filepath.Join(t.TempDir(), "retained.bolt"))
			if err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			client.store = s
			client.mu.Unlock()
			member := uint64(1)
			if mode == "successor_pin" {
				member = 4
			}
			if err = s.EnsureIdentity("n1", "cluster", member); err != nil {
				t.Fatal(err)
			}
			replica := uint64(1)
			if mode == "successor_replica" {
				replica = 4
			}
			if err = s.RecordReplicaCAS("work", ReplicaRecord{NodeName: "n1", ClusterID: "cluster", ReplicaID: replica}, 0); err != nil {
				t.Fatal(err)
			}
			if mode == "unknown_grant" {
				grant.Key.Revision += 1000
			}
			if mode == "stopped" {
				if err = client.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			err = client.ForgetGrantedLoadReplica(t.Context(), grant, 1)
			if mode == "cleanup" {
				if err != nil {
					t.Fatal("fenced owner could not clean absent local grant", err)
				}
				if err = client.ForgetGrantedLoadReplica(t.Context(), grant, 1); err != nil {
					t.Fatal("cleanup retry was not idempotent", err)
				}
			} else if err == nil {
				t.Fatal("mismatched or stopped owner erased evidence")
			}
			if mode == "stopped" {
				if !errors.Is(err, fault.ErrStopped) {
					t.Fatal(err)
				}
				return
			}
			record, err := client.LookupLoadReplica(t.Context(), "work")
			if err != nil || record.Found != (mode != "cleanup") || record.Found && record.Record.ReplicaID != replica {
				t.Fatal("ledger evidence changed unexpectedly", record.Record, record.Found, err)
			}
		})
	}
}
