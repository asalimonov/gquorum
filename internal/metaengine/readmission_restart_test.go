//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestPendingReadmissionRestartsKeepOriginalRetirementAuthority(t *testing.T) {
	t.Parallel()
	for _, issued := range []bool{false, true} {
		name := "before-grants"
		if issued {
			name = "after-grants"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m, p, index := readmissionAuthorityFixture(t)
			command := func(p any) ApplyResult {
				t.Helper()
				r := apply(t, m.topologySM, index, p)
				index++
				return r
			}
			original, _ := m.Admission(p.NodeName, p.InstanceID)
			var grants []topology.AuthorityGrant
			if issued {
				r := command(p)
				if !r.Accepted {
					t.Fatal(r)
				}
				grants = r.Grants
			}
			var current AdmissionRecord
			for _, process := range []string{"restart-1", "restart-2"} {
				current = original
				current.InstanceID = process
				r := command(BindAdmissionProposal{Admission: current, ReplaceInstance: true, At: time.Unix(300, 0).UTC()})
				if !r.Accepted || r.Admission.ReadmissionInstanceID != original.InstanceID || ReadmissionID(r.Admission) != ReadmissionID(original) {
					t.Fatal("process rebind changed retirement authority", r)
				}
				current = r.Admission
				p.InstanceID = process
				r = command(p)
				if !r.Accepted {
					t.Fatal(r)
				}
				if grants == nil {
					grants = r.Grants
				} else if !reflect.DeepEqual(grants, r.Grants) {
					t.Fatal("restart issued new grants", grants, r.Grants)
				}
				if len(grants) != 2 || grants[0].AdmissionID != ReadmissionID(original) {
					t.Fatal(grants)
				}
				// The query resolves the stored origin, never caller-supplied aliases.
				query := current
				query.ReadmissionInstanceID = "forged"
				got, err := m.ReadmissionGrants(t.Context(), query)
				if err != nil || !reflect.DeepEqual(got, grants) {
					t.Fatal(got, err)
				}
			}
			data, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			m = NewMachine(m.rules, nil)
			if err = m.Restore(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			ready := ReadyAdmissionProposal{NodeName: current.NodeName, InstanceID: current.InstanceID, MemberID: current.MemberID, OperatorID: current.OperatorID}
			if r := command(ready); r.Accepted || r.Code != proposalAuthorityPending {
				t.Fatal("restart became ready without original receipts", r)
			}
			var receipts []topology.EffectReceipt
			for _, grant := range grants {
				receipts = append(receipts, fixtureEffectReceipts(grant)...)
			}
			// JoinReady supplies current process identity, with historical receipts.
			current.ReadmissionInstanceID = ""
			if r := command(RecordReadmissionReceiptsProposal{Admission: current, Receipts: receipts}); !r.Accepted {
				t.Fatal(r)
			}
			for _, process := range []string{original.InstanceID, "restart-1"} {
				old := ready
				old.InstanceID = process
				if r := command(old); r.Accepted {
					t.Fatal("superseded process became ready", r)
				}
			}
			if r := command(ready); !r.Accepted {
				t.Fatal("current process could not acknowledge original retirement", r)
			}
		})
	}
}

func TestReadmissionOriginCannotBeSelectedByProposalOrCorruptSnapshot(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	original, _ := m.Admission(p.NodeName, p.InstanceID)
	restarted := original
	restarted.InstanceID = "restarted"
	restarted.ReadmissionInstanceID = original.InstanceID
	if r := apply(t, m.topologySM, index, BindAdmissionProposal{Admission: restarted, ReplaceInstance: true, At: time.Unix(300, 0).UTC()}); r.Accepted {
		t.Fatal("proposal chose its own readmission origin", r)
	}
	index++
	restarted.ReadmissionInstanceID = ""
	if r := apply(t, m.topologySM, index, BindAdmissionProposal{Admission: restarted, ReplaceInstance: true, At: time.Unix(300, 0).UTC()}); !r.Accepted {
		t.Fatal(r)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*AdmissionRecord){
		"missing":        func(a *AdmissionRecord) { a.ReadmissionInstanceID = "missing" },
		"self":           func(a *AdmissionRecord) { a.ReadmissionInstanceID = a.InstanceID },
		"foreign-member": func(a *AdmissionRecord) { a.MemberID++ },
		"foreign-operator": func(a *AdmissionRecord) {
			a.OperatorID = topology.MakeOperatorID(900, 1)
			a.State = AdmissionSuperseded
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var state smState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			for i := range state.Admissions {
				if state.Admissions[i].InstanceID == restarted.InstanceID {
					mutate(&state.Admissions[i])
				}
			}
			bad, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err = NewMachine(m.rules, nil).Restore(t.Context(), bad); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt readmission origin accepted", err)
			}
		})
	}
}
