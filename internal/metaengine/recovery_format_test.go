//go:build !functional

package metaengine

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
)

//go:embed testdata/recovery-schema1/importing.json
var frozenRecoveryImport []byte

//go:embed testdata/recovery-schema1/complete.json
var frozenRecoveryComplete []byte

func TestRecoverySchemaOneFrozenSnapshotsRestoreWithoutChangingOrdinaryFormats(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct {
		phase, digest string
		data          []byte
	}{
		{"importing", "fccd790266766f4e09141a6d2aaeb5ffedbe726340efa60cee9a4a9a6e774ca2", frozenRecoveryImport},
		{"complete", "d7f28a825a06c0921daaef41a99fd4b73f02af7b39d74dd185a9214ce8307c5b", frozenRecoveryComplete},
	} {
		t.Run(fixture.phase, func(t *testing.T) {
			t.Parallel()
			if fmt.Sprintf("%x", sha256.Sum256(fixture.data)) != fixture.digest {
				t.Fatal("frozen recovery fixture changed")
			}
			machine := mustMachine(t, testRules(), nil)
			if err := machine.Restore(t.Context(), fixture.data); err != nil {
				t.Fatal(err)
			}
			owned := machine.RecoveryState()
			if owned == nil || owned.Phase != fixture.phase || owned.RequestDigest == "" {
				t.Fatal(owned)
			}
			owned.Groups[0].Source.Digest = "mutated caller copy"
			if machine.RecoveryState().Groups[0].Source.Digest == owned.Groups[0].Source.Digest {
				t.Fatal("read alias")
			}
			data, err := machine.Snapshot(t.Context())
			if err != nil || !bytes.Equal(data, fixture.data) {
				t.Fatal("recovered encoding changed", err)
			}
			query, err := machine.Query(t.Context(), []byte(`{"kind":"recovery"}`))
			if err != nil || !bytes.Contains(query.Data, []byte(`"request_digest"`)) {
				t.Fatal(query, err)
			}
			if err := machine.scoped.pruneRetainedProofs(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoverySnapshotRejectsUnknownDuplicateAndContradictoryBindings(t *testing.T) {
	t.Parallel()
	for name, transform := range map[string]func([]byte) []byte{
		"unknown": func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"recovery":{"schema":1,`), []byte(`"recovery":{"unrecognized":1,"schema":1,`), 1)
		},
		"duplicate": func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"recovery":{"schema":1,`), []byte(`"recovery":{"schema":1,"schema":1,`), 1)
		},
		"request digest": func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"request_digest":"`), []byte(`"request_digest":"invalid-`), 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data := transform(bytes.Clone(frozenRecoveryImport))
			if bytes.Equal(data, frozenRecoveryImport) {
				t.Fatal("test did not alter fixture")
			}
			var state scopedState
			if err := json.Unmarshal(data, &state); err == nil {
				t.Fatal("invalid recovered snapshot accepted")
			}
		})
	}
}
