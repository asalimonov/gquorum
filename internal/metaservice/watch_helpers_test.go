package metaservice

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func receiveUpdate(t *testing.T, ch <-chan topology.Update) topology.Update {
	t.Helper()
	select {
	case u, ok := <-ch:
		if !ok {
			t.Fatal("watch closed")
		}
		return u
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not deliver")
		return topology.Update{}
	}
}

func assertWatchQuiet(t *testing.T, ch <-chan topology.Update) {
	t.Helper()
	select {
	case u := <-ch:
		t.Fatalf("unexpected watch update %+v", u)
	case <-time.After(30 * time.Millisecond):
	}
}
