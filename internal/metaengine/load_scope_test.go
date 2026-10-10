package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestLoadScopeCreationProgressDistinguishesEmptyPreparedLearners(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                           string
		role                           topology.ReplicaRole
		kind                           topology.StepKind
		current                        int
		state                          topology.ExecutionState
		started, established, founding bool
	}{
		{"pending founding voter", topology.ReplicaVoter, topology.StepCreateFoundingGroup, 0, topology.ExecutionPending, false, false, true},
		{"failed founding effect", topology.ReplicaVoter, topology.StepCreateFoundingGroup, 1, topology.ExecutionFailed, false, false, true},
		{"completed founding effect", topology.ReplicaVoter, topology.StepCreateFoundingGroup, 2, topology.ExecutionRunning, true, true, true},
		{"intentional learner skipped by founding", topology.ReplicaLearner, topology.StepCreateFoundingGroup, 2, topology.ExecutionRunning, false, false, false},
		{"prepared learner", topology.ReplicaLearner, topology.StepPrepareReplica, 2, topology.ExecutionRunning, true, false, false},
		{"admitted learner", topology.ReplicaLearner, topology.StepAddLearner, 2, topology.ExecutionRunning, true, true, false},
		{"succeeded final prepare", topology.ReplicaLearner, topology.StepPrepareReplica, 1, topology.ExecutionSucceeded, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewMachine(testRules(), nil)
			doc := baseDoc()
			doc.Version = 1
			doc.Group("shard0").Replicas[0].Role = tc.role
			m.latest, m.target = doc.Clone(), doc.Clone()
			m.history = []topology.Document{doc.Clone()}
			ex := newExecutionRecord(1, 1, []topology.Step{{Seq: 1, Kind: tc.kind, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}}, "actor", topology.OriginOperator, doc.CreatedAt)
			ex.CurrentStep, ex.State = tc.current, tc.state
			m.executions[ex.ID] = &ex
			id := storage.Identity{NodeName: "n1", ClusterID: doc.ClusterID, GroupID: "shard0", ReplicaID: 1}
			if err := m.WithLoadScope(id, 1, func(scope LoadScope) error {
				if scope.Started != tc.started || scope.Established != tc.established || scope.Founding != tc.founding {
					t.Fatal(scope)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadScopeRejectsOldOwnerAndRetiredGroupAtApproval(t *testing.T) {
	t.Parallel()
	sm, _ := activatedSM(t)
	m := &Machine{topologySM: sm}
	id := storage.Identity{NodeName: "n1", ClusterID: "cluster-abc", GroupID: "shard0", ReplicaID: 1}
	called := false
	if err := m.WithLoadScope(id, 9, func(LoadScope) error { called = true; return nil }); !errors.Is(err, fault.ErrIdentityMismatch) || called {
		t.Fatal(called, err)
	}
	m.retired["shard0"] = 2
	if err := m.WithLoadScope(id, 1, func(LoadScope) error { called = true; return nil }); !errors.Is(err, fault.ErrGroupRetired) || called {
		t.Fatal(called, err)
	}
}
