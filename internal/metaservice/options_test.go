//go:build !functional

package metaservice

import (
	"reflect"
	"testing"
)

func TestFoundingIDsAreIndependentOfOrdering(t *testing.T) {
	t.Parallel()
	a, err := FoundingIDs("cluster", []string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := FoundingIDs("cluster", []string{"n3", "n1", "n2"})
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal(a, b, err)
	}
	c, err := FoundingIDs("other", []string{"n1", "n2", "n3"})
	if err != nil || reflect.DeepEqual(a, c) {
		t.Fatal("IDs did not bind cluster", err)
	}
	for _, names := range [][]string{nil, {""}, {"n1", "n1"}, {"n1\x00n2"}} {
		if _, err := FoundingIDs("cluster", names); err == nil {
			t.Fatal("invalid names accepted", names)
		}
	}
}
