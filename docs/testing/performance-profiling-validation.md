# Performance capture implementation verification

Verified on 2026-10-18 against
`docs/brainstorms/20261018-performance-capture-target.md`, at base commit
`5c241ecfd69aeb6e2e9c287eddd4d76c1da26d72` plus the working-tree changes.
This record concerns the profiling tools; existing application/library changes
in the index were preserved. Run-specific manifests retain their exact source
status, tracked diff, binaries and build metadata.

## Requirements and evidence

| Requirement | Implementation and verification |
| --- | --- |
| Separate tooling dependencies; no library API/storage changes | Python standard-library orchestration and standalone `scripts/profiledecode` Go module. Root module dependencies unchanged. Repository lint includes the decoder. |
| Profile a working-tree build | `make profile-cache` built and ran a fresh three-node cache cluster. Exact server and loader binaries and their Go build information were retained. |
| Configurable workload and capture | Default 30-second CPU/10-second syscall capture passed. A second run with ten workers, fsync enabled and `--no-syscalls --seconds 2 --warmup 0` passed. Numeric/input constraints are checked before capture. |
| Separate CPU and tracing windows | Manifest CPU end timestamp precedes syscall start. All three CPU requests overlap; no sidecar starts during CPU capture. |
| Existing application endpoints | `capture --target fixture=http://127.0.0.1:PORT --binary fixture=PATH --seconds 1` captured a live Go pprof server on an ephemeral port. Retained binary bytes match the original. |
| Import existing evidence | `make profile-report` imported a CPU profile, compressed trace and binary. Resulting CPU/syscall aggregates exactly match the original capture. Syscall-only import also passed. |
| Raw evidence and reproducibility | All 31 manifest artifact sizes and SHA-256 hashes in the default run were checked. CPU, heap, goroutine, trace, event, binary, command, source-diff and workload artifacts exist. Saved heap and goroutine profiles open with `go tool pprof -top`. |
| Sampled method statistics | Self totals equal total sampled CPU; recursive inclusive cost counts a method once. Actual recursive/inline stack structure is retained. Unit coverage includes inline frames, unknown addresses/stacks, non-CPU and negative differential rejection. Invocation count and per-call latency remain null. |
| Syscall statistics and arguments | JSONL event counts equal completed-call totals; event durations sum to aggregate elapsed time. Trace windows cover approximately ten seconds and have zero orphan resumes. Units cover interleaving, missing durations, unfinished/truncated records, errno parsing and gzip input. |
| Offline interactive heatmap | Browser checks exercised filtering, metric changes, all 1,208 method rows, CPU/syscall details and target selection. No console errors or external requests; only the report document was fetched. Missing measurement domains render as unavailable. |
| Interactive flame graph | Browser checks verified 2,834 initial frames, keyboard zoom to 937 frames, Up/Reset, source/cost details, 84 search highlights and target switching. CPU-only and syscall-only reports render appropriately. |
| Safe output and partial failures | Nonempty output is refused without changing its manifest. A real CLI import with valid CPU plus invalid heap-as-CPU exits 1 and retains the successful profile, failed manifest and partial HTML report. Embedded script escaping has unit coverage. |
| Ownership and cleanup | Both live cache runs left no owned containers, networks, volumes or image tags. Unit tests verify startup failure and KeyboardInterrupt cleanup even when log collection fails, using only the generated project/tag. Temporary browser/server fixtures were also stopped. |
| Endpoint and subprocess failures | Units verify successful peer retention when another CPU endpoint fails, optional heap/goroutine warnings, and retained partial command output on subprocess timeout. |
| Documentation and entry points | Make targets `profile-cache`, `profile-report`, `test-profile-tools`; root/cache README links; [usage guide](performance-profiling.md). `git diff --check` passes. |

The audit corrected truncated syscall records being counted as completed,
missing measurement domains being presented as zero, method zoom ambiguity
between identical names in different source files, and missing command output
on timeout. Heatmap scaling also avoids JavaScript argument-count limits for
large sets of rows.

## Test results

| Command | Result |
| --- | --- |
| `make test-profile-tools` | PASS: 13 Python tests, 3 Go decoder tests, Go vet. |
| `python3 -m unittest discover -s scripts -p '*_test.py'` | PASS: 37 tests. |
| `make test` | PASS: 1,902 tests including subtests, across all five application modules. |
| `make test-race` | PASS: the same 1,902 tests under the race detector. |
| `make test-function` | PASS: 1,049 functional tests including subtests. |
| `make lint` | PASS: all configured modules/variants, including the new decoder. |
| Default cache capture, CPU-only cache capture, existing endpoint capture | PASS. |
| Import, syscall-only report, failed import/overwrite preservation | PASS. |
| Browser interaction and artifact consistency checks | PASS. |

This task did not rerun the unrelated full Docker cluster fault/performance
suites, benchmark suite or functional-race suite. The live three-node captures
exercise the profiling tools' Docker/process integration.

## Retained local evidence

These directories are generated, gitignored artifacts in this checkout:

- `test-reports/performance/architecture-verification/`: default complete capture
  and interactive report; node1/node2/node3 sampled CPU totals were
  18.97/11.40/12.03 seconds, with 6,059/6,950/9,263 observed syscall entries.
- `test-reports/performance/architecture-cpu-only/`: ten workers, fsync enabled,
  syscall tracing disabled.
- `test-reports/performance/architecture-endpoint/`: live existing Go endpoint.
- `test-reports/performance/architecture-import/`: combined import and binary.
- `test-reports/performance/architecture-trace-only/`: syscall-only report.
- `test-reports/performance/architecture-invalid-input/`: intentional failed
  import with preserved successful evidence.
- `test-reports/unit/20261018T183713.079613Z-u10vvdyv/`.
- `test-reports/unit-race/20261018T183915.744468Z-bqs_ha52/`.
- `test-reports/function/20261018T183842.352250Z-yuu7ci3d/`.

The captures verify functionality and measurement accounting. Their CPU totals
are not comparative performance benchmarks: other verification jobs were
running on the host during capture.

## Colima host-path regression

The original live verification used Docker Desktop. A subsequent user run on
Colima failed because the VM could not access the checkout at `/Volumes/M4DATA`:
the file-backed Compose credential was a daemon-side bind mount. The trace
output bind mount had the same dependency.

The profiler now creates the containers first and copies the demo credential
into each through `docker cp`. Sidecars write traces inside their own filesystem;
the runner copies traces out before removal, including partial evidence after
tracer failure. Neither path requires a host filesystem share in the VM.

Verification on the active `colima` context: the default 30-second CPU plus
10-second syscall capture completed at
`test-reports/performance/colima-mount-fix/report.html`. All three targets contain
CPU and syscall data, artifact hashes match, and all owned containers, volumes,
networks and image tags were removed. Focused profiling tests (14 Python tests
plus Go decoder/vet), all 38 Python script tests, and `git diff --check` passed.
Regression coverage checks credential transfer before startup, absence of the
Compose secret mount, and partial trace copying before sidecar removal.
