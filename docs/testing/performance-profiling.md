# Capturing and investigating performance

`make profile-cache` builds the current working tree and profiles an isolated
three-node cache cluster. It stores an offline HTML report, raw pprof profiles,
syscall traces, per-call syscall events, exact application binaries, build
metadata, a tracked-source diff, logs and SHA-256 hashes under
`test-reports/performance/<UTC timestamp>-<id>/`.

```sh
make profile-cache
make profile-cache PROFILE_ARGS='--workers 10 --seconds 30 --syscall-seconds 10'
make profile-cache PROFILE_ARGS='--fsync --gomaxprocs 2'
# CPU-only run, without the separate syscall tracing pass:
make profile-cache PROFILE_ARGS='--no-syscalls --seconds 15'
```

Requirements: Python 3.9+, the pinned Go toolchain, Docker with Compose, and
network access on the first build for Go modules and the diagnostic Alpine
image. The decoder uses the official `github.com/google/pprof/profile` package
in a separate tooling module; nothing is added to the library's dependencies.

The default workload uses one worker, 615 keys and 80% reads / 20% writes,
`GOMAXPROCS=1`, a five-second warmup, a 30-second CPU capture and a separate
10-second syscall capture. Load-group fsync is disabled unless `--fsync` is set.
The runner uses fresh private volumes and a unique image/project name. It does
not replace `gquorum-cache:local`, stop the user's cluster or reuse its data.
It exposes pprof on randomly assigned loopback ports while running.

The profiler does not bind-mount the workspace or output directory. It creates
the node containers, copies the public demo credential into each with `docker cp`,
and then starts them. This also works when Colima or Docker Desktop cannot see
the host workspace path (for example, a checkout on an external macOS volume).

CPU profiles are fetched concurrently from all three nodes. Heap and goroutine
profiles are saved afterward for further pprof investigation. The loader stays
active through both measurement phases. A separate strace sidecar attaches to
each server **after CPU capture finishes**, using `SYS_PTRACE` and an unconfined
seccomp profile. It detaches after the requested interval; the servers keep
running until the runner collects logs and cleans up its own resources.
Each sidecar writes its trace inside the container; the runner copies it out
before removing the sidecar, including partial traces on failure.

Open the printed `report.html` path directly in a browser. It is self-contained
and makes no external requests. The report provides:

- A searchable heatmap of methods across targets, selecting inclusive or self CPU.
- A syscall heatmap selecting observed count, total, mean or maximum elapsed time.
- A zoomable flame graph with source-file/line details and method search highlights.
- Per-selection statistics and decoded syscall argument/return examples.
- Links to full CSV/JSON summaries, raw evidence and capture provenance.

## Measurement semantics

**A CPU sample is not a method invocation.** pprof provides sampled self and
inclusive CPU cost, not exact method call counts or individual method duration.
The tooling explicitly records `invocation_count` and `mean_call_ns` as JSON
`null` / empty CSV cells. It never divides CPU time by sample count and labels
that result as method latency. Inclusive costs overlap; self cost counts each
sample once. Recursive occurrences count once per method for inclusive totals,
but remain visible at each depth in the actual sampled stack.

Syscall counts are observed syscall entries during the trace window, including
failed calls. Interleaved unfinished/resumed records are paired by thread ID.
Calls lacking a return or duration are counted but excluded from duration
averages. `timed_calls` is the denominator for mean latency. The summary gives
count, completed/incomplete/timed counts, errno counts and total/min/mean/max
elapsed duration. `raw/<target>.events.jsonl.gz` stores each completed observed
call with its thread, start timestamp, duration (or null), errno and decoded
arguments/result. `raw/<target>.strace.gz` preserves the original full trace.

`strace -T` durations include time waiting and tracer overhead. They are not CPU
time. Totals can exceed wall time because threads wait concurrently. CPU and
syscall captures deliberately use separate windows; they must not be merged
into one stack tree. The tool cannot identify which sampled Go invocation
issued a particular syscall. pprof can still show Go syscall wrapper frames,
while the syscall view shows actual Linux syscall names and decoded arguments.

Captured arguments are truncated by strace at 80 bytes for strings and by the
JSON event view at 4096 characters per event; the original trace is retained.
A syscall errno is not automatically an application error: `EAGAIN`,
`EINPROGRESS`, interrupted waits and optional feature probes are common.

For the underlying measurement contracts, see the official
[Go diagnostics guide](https://go.dev/doc/diagnostics),
[runtime/pprof documentation](https://pkg.go.dev/runtime/pprof), and
[strace manual source](https://github.com/strace/strace/blob/master/doc/strace.1.in).

## Existing applications and stored profiles

Any application exposing Go's standard pprof HTTP endpoints can be captured:

```sh
python3 scripts/profile.py capture --seconds 30 \
  --target node1=http://127.0.0.1:6061 \
  --target node2=http://127.0.0.1:6062 \
  --binary server=/path/to/exact/server-binary
```

This command does not restart applications or add profiling listeners. Supply
base URLs without `/debug/pprof/profile`; an optional URL path prefix is allowed.
The cache example already provides `CACHE_PPROF_LISTEN` and its optional
`compose.profile.yaml` overlay. Other applications own their diagnostic listener.
The generic capture command does not attach syscall tracers automatically.
Retain exact binaries with repeatable `--binary NAME=PATH` for later symbolization.

Analyze existing evidence, including Go benchmark `-cpuprofile` files:

```sh
make profile-report PROFILE_ARGS='--profile node1=/tmp/node1.cpu.pprof --strace node1=/tmp/node1.strace.gz'
python3 scripts/profile.py analyze \
  --profile before=/tmp/before.pprof --profile after=/tmp/after.pprof
python3 scripts/profile.py analyze --strace node1=/tmp/node1.strace
```

Use `strace -f -ttt -T -yy -s 80 -o node1.strace -p PID` on Linux to capture an
existing process manually. Attaching may require ptrace permissions. The tool
accepts either plain or gzip trace files. Caller labels identify targets in the
report; only combine evidence from different builds/windows intentionally.
Missing symbols are retained as unknown frames. Only ordinary, nonnegative
CPU profiles with `cpu/nanoseconds` and `samples/count` are accepted for the
CPU view; heap/allocs/differential profiles are not silently interpreted as CPU.

Add `--output PATH` to any command to choose an empty directory. Existing
nonempty output directories are refused. Raw inputs are copied so analysis does
not depend on the originals remaining present. Failed captures retain partial
artifacts and a manifest with `status: failed`; the command exits nonzero.

## Artifact contract

| File | Content |
| --- | --- |
| `report.html` | Offline interactive heatmaps and CPU flame graphs; chart data embedded. |
| `summary.md` | Overview, units and limitations. |
| `summary.json` | Versioned manifest, per-target method/syscall statistics and sampled trees. |
| `methods.csv` | Every observed method: source, self/inclusive ns and sample counts; invocation statistics unavailable. |
| `syscalls.csv` | Every observed syscall: counts, errors, total/min/mean/max elapsed ns. |
| `manifest.json` | Status, UTC phase boundaries, options, Git/Go/Docker metadata, errors/warnings and artifact hashes. |
| `raw/*.pprof` | Original CPU, heap and goroutine profiles. |
| `raw/*.strace.gz` | Original syscall trace, including incomplete records. |
| `raw/*.events.jsonl.gz` | Completed syscall events with individual durations and decoded arguments. |
| `binaries/` | Exact server/loader binaries and `go version -m` output for cache captures; user-supplied binaries otherwise. |
| `compose.json`, `commands.jsonl`, `*.log` | Resolved isolated configuration, command results, workload and node logs. |
| `demo-credential.txt` | Public example credential copied into the isolated nodes; no host secret bind mount is used. |
| `source.patch` | Tracked changes relative to HEAD, including staged changes. Untracked source files are not included. |
| `profiledecode` | The native decoder executable used for this capture. |

Memory and trace files can be large; retention is explicit and there is no
background deletion. The application binaries include build IDs and module
versions; `source.patch` alone is not a complete source snapshot if the build
includes untracked files. Profile files can be revisited with ordinary Go tools:

```sh
go tool pprof -http=127.0.0.1:8090 RUN/binaries/cache-server RUN/raw/node1.cpu.pprof
go tool pprof -http=127.0.0.1:8090 RUN/binaries/cache-server RUN/raw/node1.heap.pprof
```

## Verification and scope

```sh
make test-profile-tools
python3 -m unittest discover -s scripts -p '*_test.py'
make lint
```

The focused tests cover inline frames, recursion, unknown frames, CPU sample
semantics, interleaved syscall records, absent timing, partial calls, compressed
input, output escaping and isolated Compose configuration. `make lint` includes
the decoder module. An actual cache capture exercises Docker and HTTP paths;
it is intentionally separate from the fast unit suite.

Exact method instrumentation, eBPF syscall-to-Go-stack correlation, continuous
profiling servers, automatic retention/remote upload, percentile estimation,
block/mutex-rate configuration and arbitrary application deployment are outside
this tool's current scope. Existing pprof data is sufficient for the CPU
heatmap/flame graph; exact invocation metrics require a separately designed
instrumentation mode.
