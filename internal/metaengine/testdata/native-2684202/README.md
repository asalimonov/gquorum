# Native metadata baseline 2684202

Captured once on 2026-10-25 from an isolated `git archive` of
`26842024d2fb4fe6fd74b15efa5106749a8b0674`, using Go 1.26.5 on darwin/arm64.
No current recovery codec generated these bytes. Existing legacy and scoped
fixtures were not changed. Never regenerate this fixture to make a test pass.

The baseline public Node starts one participant (`cluster`, `n1`, member 1,
instance `source`, type `worker`), activates its session and acquires 27 distinct
application locks with SnapshotThreshold=8. After joined Stop, native `_meta`
has a nonempty snapshot at index 33/term 2, retained commit 35/term 2 and two
committed tail entries. `_authority.bolt` is the corresponding closed baseline
journal. Ephemeral addresses/session IDs/times are captured data, not expectations
that a generator must reproduce byte for byte.

Files are gzip-compressed with mtime=0. The functional test checks the raw hash,
expands only into private temporary roots, replays without modifying the source,
and restarts a real current Node with a fresh process instance and listener.

| Raw file | Raw bytes | Raw SHA-256 | Gzip SHA-256 |
| --- | --- | --- | --- |
| `_meta.bolt` | 262144 | `c65821529d4460ea7a789cb36f1fc97d88d7fac58dc54ffd816336ee1a192a9d` | `5429d3d89e60f1784604c63d288b1b1da9a8d5c1b5b86cd707e9a4a06e9b1e05` |
| `_authority.bolt` | 131072 | `b108d9a7deef3f64d077753b12259c898b9bc75880cb2b5ad2545b5d4ca9fd82` | `9766c2a55229dc6320d6d2369dd217cafa57c42e9491383605bd7a81da69742b` |

Reproduction of the capture process (not replacement of frozen artifacts):

1. Export the exact baseline commit into an isolated directory.
2. Copy adjacent `capture_func_test.go.txt` to its
   `internal/metaengine/native_capture_func_test.go`.
3. Create `/tmp/gquorum-baseline-native-output` and run
   `go test -tags=functional ./internal/metaengine -run '^TestCaptureNativeBaseline$' -v -count=1`.
4. Confirm nonempty native snapshot plus committed tail, then record provenance
   and hashes before copying any new artifact to a separately named destination.

The source includes assertions and writes only to its temporary Node root and the
named capture-output directory. It is inert `.txt` data in the current repository.
