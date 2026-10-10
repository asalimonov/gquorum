package metaengine

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"go.etcd.io/raft/v3/raftpb"
)

type observerPersister struct {
	saved bool
	fail  error
}

func (*observerPersister) SaveState([]byte) error { return nil }
func (p *observerPersister) SaveApplication(uint64, raftpb.ConfState, []byte) error {
	if p.fail != nil {
		return p.fail
	}
	p.saved = true
	return nil
}

func TestAppliedObserverRunsOutsideLockAfterPersistence(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{true, false} {
		p := &observerPersister{}
		if fail {
			p.fail = errors.New("application fsync failed")
		}
		m := NewMachine(testRules(), p)
		data, err := Encode(ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = m.Apply(context.Background(), raftengine.Command{Index: 1, Term: 1, Data: data}); err != nil {
			t.Fatal(err)
		}
		calls := 0
		m.OnApplied = func(index uint64, cs raftpb.ConfState) {
			calls++
			if !p.saved || index != 1 || len(cs.Voters) != 1 || m.Latest().Version != 1 {
				t.Error("callback before persistence or wrong scope")
			}
		}
		err = m.AfterApply(context.Background(), 1, 1, raftpb.ConfState{Voters: []uint64{1}})
		want := 1
		if fail {
			want = 0
		}
		if calls != want || (err != nil) != fail {
			t.Fatal(calls, err)
		}
	}
}
