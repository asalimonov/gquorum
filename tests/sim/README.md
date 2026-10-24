# Deterministic cluster simulations

Run from the repository root:

```sh
make test-sim
make test-sim SIM_SEEDS=17 SIM_ARGS='-run ^TestClusterIntermittentFollower'
make test-sim SIM_SEEDS=1-3
make test-sim SIM_SEEDS=1-3 SIM_PARALLEL=4
```

This separate module uses Gosim's runtime, filesystem and IPv4 TCP simulation.
It runs three real `gquorum.Node` instances with the library's authenticated
plaintext gRPC metadata transport and private bbolt metadata/authority stores.
It requires no Docker or external servers. `make test` stays unchanged.

`SIM_PARALLEL` (default 4, range 1–256) bounds isolated test processes **within
one seed**. All tests for that seed finish before the next seed starts. Use
`SIM_PARALLEL=1` for sequential execution. The runner builds and validates one
binary, discovers its test inventory using Gosim's versioned handshake, and
reuses it across workers. Each process owns an independent simulation world;
this does not add `t.Parallel()` inside a shared simulated network.

`SIM_ARGS` accepts `-run` and `-skip` regular expressions over top-level test
names (Python regex syntax), plus `-simtrace` categories. Empty selections,
unexpected results and interrupted workers fail the suite. Each worker has a
120-second host timeout; the fixture's virtual deadline remains separate.
Standalone runner invocation can override it with `--test-timeout`.

The Make target builds the Gosim launcher with this module's selected compiler
and dependency. Go 1.26.5 is the toolchain pin. Gosim currently supports
Darwin ARM64, Linux ARM64 and Linux AMD64; its launcher validates compatibility.
A C compiler is needed to build its SQLite translation cache dependency.
The first run builds the worker and translates dependencies; later runs cache
those artifacts under `.gosim`.

## Cases and assertions

| Case | Coverage |
| --- | --- |
| `TestClusterLatencyZero` | Three nodes at zero network delay |
| `TestClusterLatency50ms` | Fixed 50 ms one-way delay |
| `TestClusterLatency100ms` | Fixed 100 ms one-way delay |
| `TestClusterLatency200ms` | Fixed 200 ms one-way delay (400 ms RTT) |
| `TestClusterLatencyJitter` | Seeded variable one-way delay in 0–200 ms |
| `TestClusterIntermittentFollower` | Ten bidirectional partitions of one follower; partition onsets 3–10 seconds apart, each lasting two seconds; the majority link is never partitioned |
| `TestQuorumProbeRejectsIsolatedNode` | An isolated voter cannot confirm quorum; reads and mutations succeed again after repair |

Latency cases run for at least 30 simulated seconds, committing lock mutations
from each participant. They check the committed lock owner through quorum reads
on all replicas. Partition cases commit a fresh lock while the minority is
isolated, then confirm replication after repair.

The monitor starts a **fresh `Cluster.ReadBarrier` every 100 ms per observed
node**: all three for latency cases, both stable voters for intermittent
connectivity. Every barrier must succeed within **5 simulated seconds**, without
retries or skipped failures. Requests overlap so network delay cannot reduce the
sampling cadence. The monitor checks cadence and sample count, drains all
requests, and reports failure counts and the slowest completion. Local
`Status.Ready` and leader observations are not quorum proof.
The deadline accommodates a follower waiting behind an active read cohort before
forwarding its own round; fixed 200 ms delay measured up to 4.8 seconds.

The intermittent case chooses a follower after election, keeping the elected
leader in the stable majority. This checks uninterrupted majority service during
follower isolation. It does not assert zero interruption when the isolated node
is the leader: Raft must elect a replacement in that scenario.

The fixture sets a 100 ms Raft tick, 3-second minimum election timeout and
2-second dial/send timeouts to accommodate a 400 ms network RTT plus forwarding.
These are test application settings; library defaults are unchanged. Automatic
topology repair is disabled so all three voters remain in the tested
configuration throughout the faults.
The simulated TCP buffer is 256 KiB per connection. Gosim's legacy 1 KiB default
would impose a bandwidth bottleneck unrelated to the configured network latency.

Each Node and every public API call run inside their owning simulated machine.
Controller channels schedule local calls and collect results; no Raft message
uses these channels. Machine cleanup joins Node shutdown before destroying the
machine. The filesystem provider owns identical `/app/state` paths independently
on each node. These tests exercise metadata storage, not load-group WAL crash
recovery, shared-file lock contention, TLS or host resource measurements.

## Reports and reproduction

Reports use the same runner as the other suites:

- `test-reports/sim/latest/report.md`: outcomes and exact commands.
- `timeline.md`, `events.json`, and JUnit XML: host execution durations.
- Package logs: seed, simulated fault schedule, quorum counts and durations.

`TEST_REPORTS_DIR` overrides the report root. Seed runs are separate reported
tests. Translation/build failures, incomplete output, empty selections and skips
cannot produce a successful suite. Virtual durations remain in raw log output;
JUnit elapsed time measures host execution.
Parallel package logs prefix each line with its test/seed; Go JSON events also
retain the test label. The report timeline shows overlapping host execution.

## Animated log playback

Create a reusable viewer, open it, then drop one or several simulation logs onto
the page (or use **Choose files**):

```sh
make sim-viewer
open test-reports/sim/viewer.html
```

Each test/seed gets its own entry and playback position. Files are parsed locally
in the browser, with no upload. Loading new files replaces the capture list; an
invalid input leaves the previous capture intact. The HTML can be copied to
another machine and opened directly without this repository or a web server.

To generate a report with a capture already loaded:

```sh
make sim-visualize SIM_LOG=test-reports/sim/latest/tests-sim-root.log
# Or choose the destination explicitly:
python3 scripts/sim_visualize.py path/to/capture.log -o /tmp/cluster-replay.html
open /tmp/cluster-replay.html # macOS; otherwise open it in your browser
```

The default output replaces `.log` with `.html` beside the input. Each test/seed
has a separate view with play/pause, speed, time scrubber, previous/next state
events, node roles/terms, health bands, scheduled isolation intervals and source
log line numbers. Seeking backward recomputes state from the test's own history.
The generator accepts old sequential package logs, new prefixed parallel logs,
and the shared runner's `events.json`. Truncated captures stay visibly incomplete.

Reported health is an observation, not a fresh quorum proof. Isolation interval
ends are taken from the logged schedule; they do not claim an observed repair.
Unlogged faults, missing health observations, and instantaneous quorum status
are not reconstructed. Full-run probe totals remain labeled as full-run totals.

The HTML is self-contained and works offline. Preloaded reports also accept
dropped files. Python alone generates it from the checked-in JavaScript compiled
from `scripts/sim_visualization/{parser,player}.ts`.
After editing TypeScript, rebuild with the pinned compiler (no in-project npm
installation is required):

```sh
npx --yes --package typescript@5.9.3 tsc -p scripts/sim_visualization/tsconfig.json
```

Gosim fingerprints module contents and rejects symlinks in source inputs; keep
Node dependency installs outside this Go module. The visualization needs no
browser-side TypeScript compiler, CDN or web server.

A seed reproduces an execution only with the same complete Gosim build identity,
including dependency sources, compiler and architecture. Keep `go.mod`, `go.sum`,
`gosim.json`, the source revision and the report together.

For Gosim development, temporarily replace its dependency with a local checkout:

```sh
cd tests/sim
go mod edit -replace github.com/neria-cloud/gosim=/absolute/path/to/gosim
# Run make test-sim from the repository root.
# Restore the pinned dependency before committing:
go mod edit -dropreplace github.com/neria-cloud/gosim
```

Do not commit an absolute replacement. A workspace containing both modules
currently exposes a Gosim module-graph validation limitation; use the replacement
above for development.
