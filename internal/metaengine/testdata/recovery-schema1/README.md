# Independent recovery subtree schema 1 fixtures

Captured on 2026-10-25 during Task 9, working tree based on `0eff8c4`.
The Task 9 commit retains the complete capture inputs in the pure test helpers
`recovery_test.go` and `recovery_consumed_test.go`; the one-time capture source is
`capture.go.txt`. These are new recovery fixtures. No existing baseline/scoped
fixture was regenerated or changed.

The snapshots use the complete ordinary metadata schema 4 plus the optional
recovery schema 1 subtree, including its exact original request digest. The
importing fixture has retained three-AZ policy deficits; the complete fixture has
ordinary founding receipts, the normalized checkpoint evidence and a completed
ordinary import operation. Data completion retains the unsatisfied policies.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| importing.json | 10345 | fccd790266766f4e09141a6d2aaeb5ffedbe726340efa60cee9a4a9a6e774ca2 |
| complete.json | 14452 | d7f28a825a06c0921daaef41a99fd4b73f02af7b39d74dd185a9214ce8307c5b |

The current decoder restores, validates, queries and canonically re-encodes each
snapshot byte-for-byte. `recovery_format_test.go` embeds the bytes and asserts
these hashes; it does not write/regenerate fixtures.

A separate checkout extracted from immutable revision
`26842024d2fb4fe6fd74b15efa5106749a8b0674` ran its real `Machine.Restore`
against both files. Both returned `fault.Unreadable`, with underlying
`json: unknown field "recovery"`. The one-time baseline probe is retained as
`older-decoder-probe.go.txt`; substitute the fixture directory before running in
that revision's `internal/metaengine` package with the functional tag. Command:

```sh
go test -tags=functional ./internal/metaengine -run '^TestRecoverySchemaOneRefusedByBaseline2684202$' -v -count=1
```

This establishes explicit old-binary refusal, not migration or mixed-version
support. No protobuf, outer envelope, authority journal or engine format changed.
