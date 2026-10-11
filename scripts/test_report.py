#!/usr/bin/env python3
"""Run Go suites with Meridian-style summaries and persistent reports.

Uses only Python's standard library. Go's JSON stream is the source of truth;
verbose output is flushed to disk while tests are running.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
MODULE = "github.com/asalimonov/gquorum"
LABELS = {"unit": "Unit", "function": "Functional", "cluster": "Cluster", "example": "Example"}
TERMINAL = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP"}


def timestamp(value):
    # Go timestamps use RFC3339Nano; datetime retains microsecond precision.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="milliseconds")


def xml_text(value):
    # Preserve the original bytes in logs, but strip XML 1.0-invalid controls.
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]", "", value)


def md(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


class Test:
    def __init__(self, name, started):
        self.name = name
        self.start = started
        self.end = None
        self.elapsed = 0.0
        self.status = None
        self.output = []
        self.segments = []

    def close_segment(self, ended):
        if self.segments and self.segments[-1][1] is None:
            self.segments[-1][1] = ended

    def event(self, event, now):
        action = event["Action"]
        if action in ("run", "cont"):
            if not self.segments or self.segments[-1][1] is not None:
                self.segments.append([now, None])
        elif action == "pause":
            self.close_segment(now)
        elif action in TERMINAL:
            self.close_segment(now)
            if not self.segments:
                self.segments.append([self.start, now])
            self.end = now
            self.elapsed = event.get("Elapsed", max(0.0, now - self.start))
            self.status = TERMINAL[action]
        if event.get("Output"):
            self.output.append(event["Output"])

    def error(self, ended, message):
        self.close_segment(ended)
        if not self.segments:
            self.segments.append([self.start, ended])
        self.end = ended
        self.elapsed = max(0.0, ended - self.start)
        self.status = "ERROR"
        self.output.append(message + "\n")


class Package:
    def __init__(self, module, name, stem, started, directory):
        self.module = module
        self.name = name
        self.stem = stem
        self.start = started
        self.end = None
        self.elapsed = None
        self.status = None
        self.tests = []
        self.current = {}
        self.log = (directory / f"{stem}.log").open("w", encoding="utf-8")
        self.json = (directory / f"{stem}.json").open("w", encoding="utf-8")

    def event(self, event, now):
        self.json.write(json.dumps(event, ensure_ascii=False) + "\n")
        self.json.flush()
        if event.get("Output"):
            self.log.write(event["Output"])
            self.log.flush()
        name = event.get("Test")
        if name:
            test = self.current.get(name)
            if test is None or (event["Action"] == "run" and test.status is not None):
                test = Test(name, now)
                self.current[name] = test
                self.tests.append(test)
            test.event(event, now)
        elif event["Action"] in TERMINAL:
            self.end = now
            self.elapsed = event.get("Elapsed", max(0.0, now - self.start))
            self.status = TERMINAL[event["Action"]]

    def finish(self, ended, error=None):
        if self.end is None:
            self.end = ended
        if self.elapsed is None:
            self.elapsed = max(0.0, self.end - self.start)
        for test in self.tests:
            if test.status is None:
                test.error(self.end, error or "Test ended without a terminal result (panic, timeout or interruption).")
        failed = any(t.status in ("FAIL", "ERROR") for t in self.tests)
        if not failed and self.status in (None, "FAIL"):
            test = Test("[package]", self.start)
            test.error(self.end, error or "Package failed outside a test; see the full package log.")
            self.tests.append(test)
            self.status = "FAIL"
        if error or failed:
            self.status = "FAIL"
        self.log.close()
        self.json.close()

    def counts(self):
        return [len(self.tests)] + [sum(t.status == status for t in self.tests)
                                   for status in ("PASS", "FAIL", "ERROR", "SKIP")]

    def write(self, directory):
        total, passed, failed, errors, skipped = self.counts()
        attributes = dict(tests=str(total), failures=str(failed), errors=str(errors),
                          skipped=str(skipped), time=f"{self.elapsed:.6f}")
        root = ET.Element("testsuites", attributes)
        suite = ET.SubElement(root, "testsuite", dict(attributes, name=self.name, timestamp=utc(self.start)))
        for test in self.tests:
            case = ET.SubElement(suite, "testcase", classname=self.name, name=test.name,
                                 time=f"{test.elapsed:.6f}")
            detail = xml_text("".join(test.output))
            if test.status in ("FAIL", "ERROR"):
                ET.SubElement(case, "failure" if test.status == "FAIL" else "error",
                              message=f"{test.name}: {test.status}").text = detail
            elif test.status == "SKIP":
                ET.SubElement(case, "skipped", message=detail.strip()).text = detail
            ET.SubElement(case, "system-out").text = detail
        ET.indent(root)
        ET.ElementTree(root).write(directory / f"{self.stem}.xml", encoding="utf-8", xml_declaration=True)
        lines = [f"# Test Report: {self.stem}", "", f"**Status:** {self.status}",
                 f"**Total:** {total} | **Passed:** {passed} | **Failed:** {failed} | **Errors:** {errors} | **Skipped:** {skipped}",
                 f"**Duration:** {self.elapsed:.3f}s", "",
                 f"[Full log]({self.stem}.log) · [JSON events]({self.stem}.json) · [JUnit XML]({self.stem}.xml)", ""]
        for test in self.tests:
            if test.status in ("FAIL", "ERROR"):
                detail = "".join(test.output).strip()
                fence = "`" * max(3, 1 + max((len(x) for x in re.findall(r"`+", detail)), default=0))
                lines += [f"## {test.status}: {test.name}", "", fence, detail, fence, ""]
        lines += ["## All tests (sorted by duration)", "", "| Status | Test | Duration |", "| --- | --- | ---: |"]
        for test in sorted(self.tests, key=lambda t: t.elapsed, reverse=True):
            lines.append(f"| {test.status} | {md(test.name)} | {test.elapsed:.3f}s |")
        (directory / f"{self.stem}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


class Run:
    def __init__(self, suite, reports, race=False):
        self.label = LABELS[suite] + (" Race" if race else "")
        self.suite = suite
        self.start = time.time()
        self.clock = time.monotonic()
        base = reports.resolve() / (suite + ("-race" if race else ""))
        base.mkdir(parents=True, exist_ok=True)
        prefix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ-")
        self.directory = Path(tempfile.mkdtemp(prefix=prefix, dir=base))
        self.console = (self.directory / "console.log").open("w", encoding="utf-8")
        self.events = (self.directory / "events.json").open("w", encoding="utf-8")
        self.packages = {}
        self.stems = {}
        self.phases = []
        self.commands = []
        self.say(f"Reports: {self.directory}")

    def say(self, value, color=None):
        codes = {"cyan": "0;36", "green": "0;32", "yellow": "1;33"}
        colored = f"\033[{codes[color]}m{value}\033[0m" if color and sys.stdout.isatty() and "NO_COLOR" not in os.environ else value
        print(colored, flush=True)
        self.console.write(value + "\n")
        self.console.flush()

    def package(self, module, name, now):
        key = (module, name)
        if key not in self.packages:
            prefix = MODULE if module == "library" else MODULE + "/" + module.replace("-", "/")
            short = name.removeprefix(prefix).strip("/") or "root"
            slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", short).strip("-") or "root"
            stem = module + "-" + slug
            if stem in self.stems and self.stems[stem] != key:
                stem += "-" + hashlib.sha256(name.encode()).hexdigest()[:8]
            self.stems[stem] = key
            self.packages[key] = Package(module, name, stem, now, self.directory)
            self.say(f"=== {stem}: running")
        return self.packages[key]

    def command(self, module, command, env=None, json_output=True):
        started = time.time()
        self.commands.append({"module": module, "command": command, "start": utc(started)})
        self.say(f"Running {module}: {shlex.join(command)}")
        phase = {"name": module, "start": started, "end": None, "status": "ERROR"}
        self.phases.append(phase)
        error = None
        result = 1
        process = None
        build_output = {}
        keys_before = set(self.packages)
        log = self.directory / f"command-{module}.log"
        try:
            with log.open("w", encoding="utf-8") as output:
                process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                                           errors="replace", start_new_session=True)
                for raw in process.stdout:
                    event = None
                    if json_output:
                        try:
                            candidate = json.loads(raw)
                            if isinstance(candidate, dict) and "Action" in candidate:
                                event = candidate
                        except json.JSONDecodeError:
                            pass
                    text = event.get("Output", "") if event else raw
                    output.write(text)
                    output.flush()
                    self.console.write(text)
                    self.console.flush()
                    if not event:
                        continue
                    self.events.write(json.dumps(event, ensure_ascii=False) + "\n")
                    self.events.flush()
                    name = event.get("Package") or event.get("ImportPath")
                    if not name:
                        continue
                    # Go builds white-box tests as "pkg [pkg.test]" but emits
                    # their final result as "pkg". These belong to one report.
                    name = name.split(" [", 1)[0]
                    now = timestamp(event["Time"]) if event.get("Time") else time.time()
                    key = (module, name)
                    if event["Action"] == "build-output" and key not in self.packages:
                        # Successful compiler diagnostics for dependencies are
                        # retained in command logs without becoming test errors.
                        build_output.setdefault(key, []).append(event)
                        continue
                    package = self.package(module, name, now)
                    for diagnostic in build_output.pop(key, []):
                        package.event(diagnostic, now)
                    package.event(event, now)
                    if event["Action"] == "build-fail":
                        package.status = "FAIL"
                    if event.get("Test") and "/" not in event["Test"] and self.suite in ("cluster", "example"):
                        if event["Action"] == "run":
                            self.say(f"=== {event['Test']}: running")
                        elif event["Action"] in TERMINAL:
                            self.say(f"--- {event['Test']}: {TERMINAL[event['Action']]} ({event.get('Elapsed', 0):.3f}s)")
                    elif not event.get("Test") and event["Action"] in TERMINAL:
                        status = "ok" if event["Action"] != "fail" else "FAIL"
                        self.say(f"--- {package.stem}: {status} ({package.elapsed:.3f}s)")
                result = process.wait()
        except (OSError, KeyboardInterrupt) as exc:
            error = "Run interrupted" if isinstance(exc, KeyboardInterrupt) else str(exc)
            self.say(error)
            with log.open("a", encoding="utf-8") as output:
                output.write(error + "\n")
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            result = 130 if isinstance(exc, KeyboardInterrupt) else 1
        finally:
            if process is not None and process.stdout is not None:
                process.stdout.close()
            ended = time.time()
            keys = set(self.packages) - keys_before
            if json_output and result == 0 and not keys:
                error = "Go produced no package events; cannot report results."
                result = 1
            phase.update(end=ended, status="PASS" if result == 0 else "FAIL")
            self.commands[-1].update(end=utc(ended), exit_code=result)
            if result and not any(self.packages[k].status in (None, "FAIL") for k in keys):
                package = self.package(module, "[command]", started)
                package.log.write(log.read_text(encoding="utf-8"))
                keys.add((module, "[command]"))
            for key in keys:
                package = self.packages[key]
                package.finish(ended, error if package.status is None else None)
                package.write(self.directory)
        return result

    def finish(self, result):
        ended = time.time()
        elapsed = time.monotonic() - self.clock
        packages = sorted(self.packages.values(), key=lambda p: p.stem)
        totals = [sum(p.counts()[i] for p in packages) for i in range(5)]
        result = result or int(totals[2] + totals[3] > 0)
        status = "FAIL" if result else "PASS"
        self.say("")
        self.say("=" * 76, "cyan")
        self.say(f"{self.label} Test Report Summary", "cyan")
        self.say("=" * 76, "cyan")
        row = "{:<50} {:>7} {:>7} {:>7} {:>7} {:>7}"
        self.say(row.format("REPORT", "TESTS", "PASS", "FAIL", "ERR", "SKIP"))
        self.say(row.format("-" * 50, *(["-------"] * 5)))
        lines = [f"# {self.label} Test Report Summary", "", f"**Status:** {status}",
                 f"**Started:** {utc(self.start)}", f"**Finished:** {utc(ended)}",
                 f"**Wall time:** {elapsed:.3f}s", "",
                 "| REPORT | TESTS | PASS | FAIL | ERR | SKIP | Duration | Details |",
                 "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
        for package in packages:
            self.say(row.format(package.stem, *package.counts()))
            values = " | ".join(str(x) for x in package.counts())
            lines.append(f"| {md(package.stem)} | {values} | {package.elapsed:.3f}s | [{package.stem}.md]({package.stem}.md) |")
            if package.status == "FAIL":
                self.say(f"    FAIL log: {self.directory / (package.stem + '.log')}")
                for test in [t for t in package.tests if t.status in ("FAIL", "ERROR")][:10]:
                    self.say(f"    {test.status}: {test.name}")
        self.say(row.format("-" * 50, *(["-------"] * 5)))
        self.say(row.format(f"TOTAL ({len(packages)} reports)", *totals))
        self.say(f"{self.label} tests: {status}", "yellow" if result else "green")
        self.say(f"Wall time: {elapsed:.3f}s")
        values = " | ".join(str(x) for x in totals)
        lines += [f"| **TOTAL ({len(packages)} reports)** | {values} | | |", "",
                  "Counts include parent tests, subtests, repeated invocations and executable examples, as in JUnit.", "",
                  "[Timeline](timeline.md) · [Timeline data](timeline.json) · [Console log](console.log) · [Raw Go events](events.json)", "",
                  "## Commands", ""]
        for command in self.commands:
            lines += [f"- `{shlex.join(command['command'])}` — exit {command['exit_code']}"]
        (self.directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.timeline(packages, ended, elapsed, status)
        self.events.close()
        latest = self.directory.parent / (".latest-" + self.directory.name)
        latest.symlink_to(self.directory.name, target_is_directory=True)
        latest.replace(self.directory.parent / "latest")
        self.say(f"JUnit reports: {self.directory}/<module>-<pkg>.xml")
        self.say(f"Full package logs: {self.directory}/<module>-<pkg>.log")
        self.say(f"Summary: {self.directory / 'report.md'}")
        self.say(f"Timeline: {self.directory / 'timeline.md'}")
        self.console.close()
        return result

    def timeline(self, packages, ended, elapsed, status):
        records = []
        for package in packages:
            for test in package.tests:
                records.append(dict(package=package.name, name=test.name, status=test.status,
                                    start=test.start - self.start, end=test.end - self.start,
                                    elapsed=test.elapsed,
                                    segments=[[a - self.start, b - self.start] for a, b in test.segments]))
        records.sort(key=lambda t: (t["start"], t["package"], t["name"]))
        for index, test in enumerate(records, 1):
            test["id"] = index
        data = dict(suite=self.suite, status=status, started=utc(self.start), finished=utc(ended),
                    wall_time=elapsed, commands=self.commands,
                    phases=[dict(p, start=p["start"] - self.start, end=p["end"] - self.start) for p in self.phases], tests=records)
        (self.directory / "timeline.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        span = max(ended - self.start, 0.001)
        width = 70

        def bar(start, end, char, cells=None):
            cells = list(cells) if cells is not None else [" "] * width
            a = min(width - 1, max(0, int(start / span * width)))
            b = min(width, max(a + 1, int(end / span * width)))
            cells[a:b] = char * (b - a)
            return "".join(cells)

        chart = [f"{'OFFSET':<65} 0s{' ' * 59}{span:7.2f}s", f"{'':65} {'─' * width}"]
        for phase in data["phases"]:
            chart.append(f"{('phase: ' + phase['name']):<65} {bar(phase['start'], phase['end'], '=')} {phase['end'] - phase['start']:.3f}s")
        chart.append(f"{'':65} {'─' * width}")
        for test in records:
            if "/" in test["name"]:
                continue
            cells = bar(test["start"], test["end"], ":")
            char = {"PASS": "#", "FAIL": "!", "ERROR": "!", "SKIP": "s"}[test["status"]]
            for start, end in test["segments"]:
                cells = bar(start, end, char, cells)
            label = f"{test['id']:04} {test['name']}"[:65]
            chart.append(f"{label:<65} {cells} {test['start']:.3f}–{test['end']:.3f}s {test['status']}")
        lines = [f"# {self.label} test timeline", "", f"**Status:** {status}",
                 f"**Started:** {utc(self.start)}", f"**Finished:** {utc(ended)}",
                 f"**Wall time:** {elapsed:.3f}s (includes compilation, setup and teardown)", "",
                 "## Gantt timeline", "", "Offsets are seconds from run start. Top-level tests are shown; IDs refer to the full timing table below.",
                 "`=` command phase · `#` executing · `:` paused/waiting · `!` failure/error · `s` skipped. Very short spans occupy one cell.", "",
                 "```text", *chart, "```", "", "## All test timings", "",
                 "Reported time is Go's `Elapsed`; wall span includes time paused by `t.Parallel()`. Subtests overlap their parents, so durations must not be summed as wall time.", "",
                 "| ID | Package | Test | Status | Start (s) | End (s) | Wall span (s) | Reported time (s) |",
                 "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: |"]
        for test in records:
            lines.append(f"| {test['id']} | {md(test['package'])} | {md(test['name'])} | {test['status']} | {test['start']:.3f} | {test['end']:.3f} | {test['end'] - test['start']:.3f} | {test['elapsed']:.3f} |")
        lines += ["", "[Summary](report.md) · [Raw events](events.json)", ""]
        (self.directory / "timeline.md").write_text("\n".join(lines), encoding="utf-8")


def commands(options, extra):
    flags = ["-json", "-count=1"]
    if options.race:
        flags.append("-race")
    if options.suite in ("unit", "function", "cluster"):
        flags.append(f"-parallel={options.parallel}")
    if options.suite in ("unit", "function"):
        flags.append(f"-p={options.packages}")
    if options.suite == "function":
        flags += ["-tags=functional", f"-timeout={options.timeout}"]
    elif options.suite == "cluster":
        flags += ["-tags=clustertest", f"-timeout={options.timeout}"]
    elif options.suite == "example":
        flags += ["-tags=example", f"-timeout={options.timeout}", "-run=^TestComposeExample"]
    if options.suite in ("cluster", "example"):
        return [("tests-cluster", ["go", "-C", "tests/cluster", "test", *flags, *extra, "."])]
    return [("library", ["go", "test", *flags, *extra, "./..."]),
            ("examples-cluster", ["go", "-C", "examples/cluster", "test", *flags, *extra, "./..."]),
            ("tests-cluster", ["go", "-C", "tests/cluster", "test", *flags, *extra, "./testapp/..."])]


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    extra = []
    if "--" in argv:
        split = argv.index("--")
        extra, argv = argv[split + 1:], argv[:split]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=LABELS)
    parser.add_argument("--reports-dir", type=Path, default=ROOT / "test-reports")
    parser.add_argument("--parallel", type=int, default=8)
    parser.add_argument("--packages", type=int, default=4)
    parser.add_argument("--timeout", default="10m")
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--image", default="gquorum-test:local")
    parser.add_argument("--build-image", action="store_true")
    options = parser.parse_args(argv)
    run = Run(options.suite, options.reports_dir.resolve(), options.race)
    result = 0
    env = dict(os.environ)
    env["GQUORUM_TEST_IMAGE"] = options.image
    if options.build_image:
        result = run.command("docker-build", ["docker", "build", "-f", "tests/cluster/testapp/Dockerfile",
                                             "-t", options.image, "."], json_output=False)
    if not result:
        for module, command in commands(options, extra):
            code = run.command(module, command, env=env)
            result = result or code
            if code == 130:
                break
    return run.finish(result)


if __name__ == "__main__":
    sys.exit(main())
