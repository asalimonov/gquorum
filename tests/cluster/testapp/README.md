# Process-test application

This package is the key/value application used by the Docker cluster tests.
It supplies a state machine, transport, storage and test controls around
`gquorum.Node`. The small Compose example lives separately in `examples/cluster`.

## Package structure

| Files | Purpose |
|---|---|
| [main.go](main.go), [app.go](app.go) | Configuration, application construction and process lifetime. |
| [node_generation.go](node_generation.go), [controller.go](controller.go) | Node generations and topology operations. |
| [machine.go](machine.go), [store.go](store.go) | KV state, application request deduplication, snapshots and storage. |
| [http.go](http.go), [routing.go](routing.go), [serving.go](serving.go) | Application HTTP API, routing and readiness. |
| [load_transport.go](load_transport.go), [load_snapshot.go](load_snapshot.go) | Authenticated load transport and snapshot integration. |
| [hooks_cluster.go](hooks_cluster.go), `hooks_*` | Test routes, observations and fault controls. |
| [submission_controls.go](submission_controls.go) | Exact-replica proposal receipts for process tests. |
| `recovery_*`, [meta_proxy_cluster.go](meta_proxy_cluster.go) | Recovery jobs and optional metadata fault proxy. |
| `snapshot_*`, [load_snapshot_stats.go](load_snapshot_stats.go) | Generated snapshot fixtures, controls and resource observations. |
| `*_test.go`, `*_func_test.go` | Units and isolated functional checks; multi-node scenarios live in the parent package. |
| `load.proto`, `loadpb/`, [Dockerfile](Dockerfile) | Fixture protocol and process-test image. |

## Run checks

From the repository root:

```sh
make test
make test-function
make test-cluster-build
make test-cluster-run CLUSTER_ARGS='-v -run ^TestSubmission'
```

Rebuild the image after application changes. The harness owns isolated clusters
and their cleanup. See the [cluster testing guide](../README.md) for other suites.

## Key information

- Configuration is declared in `main.go`; the harness `Spec` supplies per-test
  overrides. Use an explicit data directory and preserve it for retained restarts.
- The process serves HTTP on 8080, metadata on 9000 and load traffic on 9001.
- Every `/internal/test/` request requires the fresh fixture credential in
  `X-Gquorum-Test-Credential`. Normal application traffic also authenticates its
  cluster and peer identity. Controls are not production library APIs.
- Controls target exact replica/process identities. Release armed barriers and
  join workers during cleanup; stale controls must not affect replacements.
- `POST /internal/test/submission/{group}/{replica}` returns the local proposal's
  receipt and error. Business rejection stays in the applied result code.
- Consistent reads use `Group.Read`; local inspection is observational. Topology
  edits use version checks, approval and operation status.
- Recovery requires Node-owned WAL. Its caller-owned maintenance job survives
  process restart; a request acknowledgement is not proof of completion.

## Snapshot controls

Ordinary startup generates no large payloads. Snapshot controls and generated
qualification data are opt-in; never commit generated blobs. See
[large-snapshot qualification](../README.md#explicit-large-snapshot-qualification).
