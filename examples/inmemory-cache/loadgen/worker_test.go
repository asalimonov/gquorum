//go:build !functional

package main

import (
	"testing"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

func TestClassifyReadAgainstAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	k := &keyState{name: "key:0001", acked: 5, written: 7, failures: map[uint64]bool{3: true}}
	bulk := func(v uint64) resp.Reply { return resp.Reply{Kind: '$', Str: encodeValue("key:0001", v, "p")} }
	cases := []struct {
		reply resp.Reply
		want  outcome
	}{
		{resp.Reply{Kind: '$', Nil: true}, miss},
		{bulk(5), hit},
		{bulk(7), hit}, // a later write's value can arrive first
		{bulk(3), uncertainHit},
		{bulk(4), staleRead},
		{bulk(8), corruptRead},
		{resp.Reply{Kind: '$', Str: encodeValue("key:0002", 5, "p")}, corruptRead},
		{resp.Reply{Kind: '$', Str: "garbage"}, corruptRead},
	}
	for _, c := range cases {
		if got := classify(k, c.reply); got != c.want {
			t.Errorf("%q: got %d, want %d", c.reply.Str, got, c.want)
		}
	}
}
