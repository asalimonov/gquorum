#!/usr/bin/env python3
"""Adapt gosim's seed-labelled results to the shared Go JSON report stream."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "github.com/asalimonov/gquorum/tests/sim"
START = re.compile(r"^\s*=== RUN\s+(.+) \(seed (-?\d+)\)\s*$")
RESULT = re.compile(r"^\s*--- (PASS|FAIL|SKIP): (.+) \(([0-9.]+)s simulated\)\s*$")


class Events:
    """Preserve raw output and seed identity; never infer pass from exit alone."""

    def __init__(self, output, expected=None):
        self.output = output
        self.expected = expected
        self.active = None
        self.started = time.monotonic()
        self.test_started = None
        self.tests = 0
        self.failed = False
        self.names = set()
        if expected is None:
            self.emit("start")
        else:
            self.emit("run", Test=f"{expected[0]}/seed={expected[1]}")

    def emit(self, action, **fields):
        value = dict(Time=datetime.now(timezone.utc).isoformat(), Action=action,
                     Package=PACKAGE, **fields)
        self.output.write(json.dumps(value) + "\n")
        self.output.flush()

    def line(self, raw):
        start = START.fullmatch(raw.rstrip("\n"))
        result = RESULT.fullmatch(raw.rstrip("\n"))
        if start:
            name, seed = start.groups()
            label = f"{name}/seed={seed}"
            if self.expected is not None and self.expected != (name, int(seed)):
                self.failed = True
            if self.active is not None or label in self.names:
                self.failed = True
                self.emit("output", Output="Invalid overlapping or repeated gosim result: " + raw)
            self.names.add(label)
            self.active = (name, label)
            self.test_started = time.monotonic()
            if self.expected is None:
                self.emit("run", Test=label)
        fields = {"Test": self.active[1]} if self.active else {}
        prefix = f"[{self.expected[0]}/seed={self.expected[1]}] " if self.expected else ""
        self.emit("output", Output=prefix + raw, **fields)
        if result:
            status, name, _ = result.groups()
            if self.active is None or self.active[0] != name:
                self.failed = True
                self.emit("output", Output="Unmatched gosim result: " + raw)
                return
            self.emit(status.lower(), Test=self.active[1],
                      Elapsed=time.monotonic() - self.test_started)
            self.tests += 1
            self.failed |= status != "PASS"
            self.active = None

    def finish(self, code):
        if self.active:
            self.emit("output", Test=self.active[1],
                      Output="gosim exited without a completed test result\n")
            self.emit("fail", Test=self.active[1],
                      Elapsed=time.monotonic() - self.test_started)
            self.failed = True
        if not self.tests or self.expected is not None and self.tests != 1:
            self.emit("output", Output="No completed simulation tests; check translation/build errors or -run filter.\n")
            self.failed = True
            if self.expected is not None and self.active is None and not self.tests:
                self.emit("fail", Test=f"{self.expected[0]}/seed={self.expected[1]}",
                          Elapsed=time.monotonic() - self.started)
        result = code or int(self.failed)
        if self.expected is None:
            self.emit("fail" if result else "pass", Elapsed=time.monotonic() - self.started)
        return result


class LockedOutput:
    """Keep one complete JSON event per line when workers report concurrently."""

    def __init__(self, output):
        self.output = output
        self.lock = threading.Lock()

    def write(self, value):
        with self.lock:
            self.output.write(value)
            self.output.flush()

    def flush(self):
        pass


def parse_seeds(value):
    seeds = []
    seen = set()
    for part in value.split(","):
        match = re.fullmatch(r"(-?\d+)(?:-(-?\d+))?", part.strip())
        if not match:
            raise ValueError(f"invalid seed range: {part}")
        first = int(match[1])
        last = int(match[2]) if match[2] is not None else first
        if not -(2**63) <= first <= last < 2**63 or last - first > 100000:
            raise ValueError(f"invalid or excessive seed range: {part}")
        for seed in range(first, last + 1):
            if seed in seen:
                raise ValueError(f"duplicate seed: {seed}")
            seen.add(seed)
            seeds.append(seed)
    return seeds


def prepare_binary(launcher, events):
    """Let Gosim build and validate its artifact; never guess a cache path."""
    for operation in ("build-tests", "lookup-test"):
        command = [str(launcher), "-C", str(ROOT / "tests/sim"), operation, "."]
        events.emit("output", Output="Command: " + json.dumps(command) + "\n")
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.stderr:
            events.emit("output", Output=result.stderr)
        if result.returncode:
            raise RuntimeError(f"{operation} failed ({result.returncode}): {result.stdout}")
        if operation == "build-tests" and result.stdout:
            events.emit("output", Output=result.stdout)
    lookup = json.loads(result.stdout)
    binary = Path(lookup["Binary"])
    manifest = json.loads(binary.with_name("manifest.json").read_text())
    if hashlib.sha256(binary.read_bytes()).hexdigest() != manifest["binary"]["sha256"]:
        raise ValueError("simulation binary does not match its manifest")
    identity = manifest["identity"]
    # Identity contains scalar build metadata; use Go's compact field order.
    digest = hashlib.sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    hello = {"protocol": 3, "identity_digest": digest, "toolchain": identity["toolchain"]}
    result = subprocess.run([str(binary), "-metatest"], input=json.dumps(hello) + "\n"
                            + json.dumps({"Test": "listtests"}) + "\n",
                            capture_output=True, text=True, timeout=10, check=True)
    lines = result.stdout.splitlines()
    if len(lines) != 2 or json.loads(lines[0]) != hello:
        raise ValueError("Gosim test discovery protocol/identity mismatch")
    tests = json.loads(lines[1])
    if not isinstance(tests, list) or not tests or any(
            not isinstance(name, str) or not re.fullmatch(r"Test\w+", name) for name in tests):
        raise ValueError("invalid top-level test inventory")
    if len(set(tests)) != len(tests):
        raise ValueError("duplicate tests in inventory")
    events.emit("output", Output=f"Build identity: {digest}; binary: {binary}\n")
    return binary, tests


class Workers:
    def __init__(self, output, timeout):
        self.output, self.timeout = output, timeout
        self.stopped = threading.Event()
        self.lock = threading.RLock()
        self.processes = set()

    def interrupt(self, *_):
        self.stopped.set()
        with self.lock:
            for process in self.processes:
                if process.poll() is None:
                    process.kill()

    def run(self, binary, name, seed, trace=""):
        if self.stopped.is_set():
            return 130
        events = Events(self.output, (name, seed))
        command = [str(binary), f"-test.run=^{name}$", "-test.v=true",
                   "-logformat=raw", f"-seeds={seed}", f"-simtrace={trace}"]
        events.emit("output", Test=f"{name}/seed={seed}",
                    Output="Command: " + json.dumps(command) + "\n")
        expired = threading.Event()
        process = None
        timer = None
        code = 1
        try:
            # Inherit the report runner's process group; its interruption kills
            # the adapter and all workers. Direct invocation also owns cleanup.
            with self.lock:
                if self.stopped.is_set():
                    return 130
                process = subprocess.Popen(command, cwd=ROOT / "tests/sim",
                                           env={**os.environ, "GODEBUG": ""},
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           text=True, encoding="utf-8", errors="replace")
                self.processes.add(process)

            def expire():
                if process.poll() is None:
                    expired.set()
                    process.kill()

            timer = threading.Timer(self.timeout, expire)
            timer.start()
            for line in process.stdout:
                events.line(line)
            code = process.wait()
            if expired.is_set():
                events.emit("output", Test=f"{name}/seed={seed}",
                            Output=f"Simulation exceeded {self.timeout}s host timeout\n")
                code = 1
        except OSError as exc:
            events.emit("output", Output=str(exc) + "\n")
        finally:
            if timer:
                timer.cancel()
                timer.join()
            if process:
                if process.poll() is None:
                    process.kill()
                process.wait()
                process.stdout.close()
                with self.lock:
                    self.processes.discard(process)
        return events.finish(code)


def run_matrix(binary, tests, seeds, parallel, workers, trace=""):
    result = 0
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        for seed in seeds:
            if workers.stopped.is_set():
                return 130
            # Join this whole seed before submitting the next one.
            futures = [executor.submit(workers.run, binary, name, seed, trace) for name in tests]
            for future in futures:
                code = future.result()
                result = result or code
    return 130 if workers.stopped.is_set() else result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", default="1-3")
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--test-timeout", type=float, default=120,
                        help="host seconds per isolated test process")
    parser.add_argument("extra", nargs=argparse.REMAINDER)
    options = parser.parse_args(argv)
    extra = options.extra
    if extra[:1] == ["--"]:
        extra = extra[1:]
    filters = argparse.ArgumentParser(description="Top-level simulation selection")
    filters.add_argument("-run", default="", help="Python regular expression for test names")
    filters.add_argument("-skip", default="")
    filters.add_argument("-simtrace", default="")
    selected = filters.parse_args(extra)
    if not 1 <= options.parallel <= 256 or options.test_timeout <= 0:
        parser.error("parallel must be 1..256 and test-timeout must be positive")
    output = LockedOutput(sys.stdout)
    events = Events(output)
    workers = Workers(output, options.test_timeout)
    try:
        seeds = parse_seeds(options.seeds)
        match, skip = re.compile(selected.run), re.compile(selected.skip) if selected.skip else None
        binary, tests = prepare_binary(ROOT / "tests/sim/.gosim/gosimtool", events)
        tests = [name for name in tests if match.search(name) and not (skip and skip.search(name))]
        if not tests:
            raise ValueError("no simulation tests selected")
        events.emit("output", Output=f"Simulation workers={options.parallel}; tests={len(tests)}; seeds={seeds}\n")
        old = {sig: signal.signal(sig, workers.interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            code = run_matrix(binary, tests, seeds, options.parallel, workers, selected.simtrace)
        finally:
            workers.interrupt()
            for sig, handler in old.items():
                signal.signal(sig, handler)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as exc:
        events.emit("output", Output=str(exc) + "\n")
        code = 1
    events.emit("fail" if code else "pass", Elapsed=time.monotonic() - events.started)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
