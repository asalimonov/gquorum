#!/usr/bin/env python3
"""Render Gosim raw/package logs or Go JSON events as an offline HTML replay."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys

from sim_events import START, RESULT

ASSETS = Path(__file__).with_name("sim_visualization")
PREFIX = re.compile(r"^\[(.+/seed=-?\d+)\] (.*)$")
ROLE = re.compile(r"^\w+ became (leader|follower|candidate|pre-candidate) at term (\d+)")
OUTAGE = re.compile(r"cycle=(\d+) isolate=(\S+) interval=(\S+) outage=(\S+)")
SUMMARY = re.compile(r"quorum checks=(\d+) failures=(\d+) cadence=(\S+) deadline=(\S+) slowest=(\S+)")


def duration(value):
    units = {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": .001, "s": 1, "m": 60, "h": 3600}
    parts = re.findall(r"([\d.]+)(ns|µs|us|ms|s|m|h)", value)
    if not parts or "".join(a + b for a, b in parts) != value:
        raise ValueError(f"invalid duration: {value}")
    return sum(float(number) * units[unit] for number, unit in parts)


def parse_log(text, source):
    tests, active, latest = [], None, {}
    captures = {}
    warnings = []

    def create(name, seed):
        test = {"name": name, "seed": seed, "status": "INCOMPLETE", "events": [],
                "duration": 0, "nodes": [], "summary": None}
        tests.append(test)
        latest[f"{name}/seed={seed}"] = test
        return test

    for number, line in enumerate(text.splitlines(), 1):
        label = None
        # Also accept shared runner events.json (one Go event per line).
        try:
            outer = json.loads(line)
        except ValueError:
            outer = None
        if isinstance(outer, dict) and "Action" in outer:
            label = outer.get("Test")
            line = outer.get("Output", "").rstrip("\n")
            if not line:
                continue
        if isinstance(outer, dict) and outer.get("replay") == "gquorum/v1":
            identity = outer.get("id")
            kind = outer.get("event")
            if kind == "start":
                target = create(outer["test"], None)
                target.update(clock="wall", capture_id=identity,
                              origin=outer["time"], scope=outer.get("scope", ""))
                captures[identity] = target
                continue
            target = captures.get(identity)
            if target is None:
                warnings.append(f"Line {number}: replay record without matching start")
                continue
            if kind == "end":
                target["duration"] = float(outer["duration"])
                continue
            if kind == "result":
                target["status"] = outer["status"]
                continue
            if kind != "observation":
                continue
            event = {"at": datetime.fromisoformat(outer["time"].replace("Z", "+00:00")).timestamp(),
                     "message": outer["msg"], "node": outer["machine"], "line": number,
                     "level": "INFO", "step": number, "kind": "status", "source": {},
                     "role": outer["role"], "term": outer.get("term"), "health": outer["health"],
                     "details": outer.get("details", {})}
            target["events"].append(event)
            continue
        prefix = PREFIX.match(line)
        if prefix:
            label, line = prefix.groups()
        start = START.fullmatch(line)
        if start:
            active = create(start[1], int(start[2]))
            continue
        target = latest.get(label) if label else active
        result = RESULT.fullmatch(line)
        if result:
            if target is None or target["name"] != result[2]:
                warnings.append(f"Line {number}: unmatched test result")
            else:
                target["status"], target["duration"] = result[1], float(result[3])
            continue
        try:
            record = json.loads(line)
        except ValueError:
            if target is not None and line.strip() and not line.startswith("Command:"):
                target.setdefault("diagnostics", []).append({"line": number, "text": line})
            continue
        if not isinstance(record, dict) or "time" not in record or "msg" not in record:
            continue
        if target is None:
            # A single JSON-only capture can have no test harness headers.
            target = active = create("Unlabelled capture", None)
        try:
            at = datetime.fromisoformat(record["time"].replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            warnings.append(f"Line {number}: invalid timestamp")
            continue
        event = {"at": at, "message": str(record["msg"]), "node": record.get("machine", "main"),
                 "level": record.get("level", "INFO"), "step": record.get("step", number),
                 "line": number, "kind": "log", "source": record.get("source", {})}
        msg = event["message"]
        role = ROLE.match(msg)
        if role:
            event.update(kind="role", role=role[1], term=int(role[2]))
        if msg == "group health changed":
            event.update(kind="health", health=record.get("state", "unknown"),
                         group=record.get("group", ""), voters=record.get("voters"), quorum=record.get("quorum"))
        outage = OUTAGE.search(msg)
        if outage:
            event.update(kind="partition", target=outage[2], outage=duration(outage[4]), cycle=int(outage[1]))
        summary = SUMMARY.search(msg)
        if summary:
            target["summary"] = {"checks": int(summary[1]), "failures": int(summary[2]),
                                 "cadence": summary[3], "deadline": summary[4], "slowest": summary[5]}
        if "BuildDigest:" in msg:
            match = re.search(r"BuildDigest:([^ ]+)", msg)
            target["build"] = match[1] if match else ""
        target["events"].append(event)
    if not tests:
        raise ValueError("no simulation tests or timestamped Gosim records found")
    for test in tests:
        events = test["events"]
        origin = (datetime.fromisoformat(test.pop("origin").replace("Z", "+00:00")).timestamp()
                  if "origin" in test else min((e["at"] for e in events), default=0))
        test["started"] = datetime.fromtimestamp(origin).astimezone().isoformat() if origin else ""
        nodes = set()
        for event in events:
            event["at"] = round(event["at"] - origin, 9)
            if event["node"] != "main":
                nodes.add(event["node"])
            if event["kind"] == "partition":
                nodes.add(event["target"])
        test["nodes"] = sorted(nodes)
        events.sort(key=lambda event: (event["at"], event["step"], event["line"]))
        test["duration"] = max(test["duration"], max((e["at"] for e in events), default=0), .001)
    return {"source": str(source), "tests": tests, "warnings": warnings}


def render(data):
    template = (ASSETS / "template.html").read_text()
    # Logs are untrusted input, including literal closing script tags.
    payload = json.dumps(data, ensure_ascii=False).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    script = (ASSETS / "player.js").read_text()
    if "</script" in script.lower():
        raise ValueError("player bundle contains a closing script tag")
    return template.replace("/*STYLE*/", (ASSETS / "style.css").read_text()).replace(
        "/*PLAYER*/", script).replace("/*DATA*/", payload)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, nargs="?",
                        help="optional capture; omit to create an empty drag-and-drop viewer")
    parser.add_argument("-o", "--output", type=Path)
    options = parser.parse_args(argv)
    output = options.output or (options.log.with_suffix(".html") if options.log else Path("test-reports/sim/viewer.html"))
    try:
        if options.log and output.resolve() == options.log.resolve():
            raise ValueError("output must differ from the input log")
        data = parse_log(options.log.read_text(encoding="utf-8"), options.log) if options.log else {
            "source": "", "tests": [], "warnings": []}
        page = render(data)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(page, encoding="utf-8")
        print(f"Wrote {output}: {len(data['tests'])} test replays; drag-and-drop enabled")
    except (OSError, ValueError) as exc:
        print(f"sim-visualize: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
