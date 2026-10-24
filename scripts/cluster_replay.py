"""Finalize opt-in Docker observations without changing Go test outcomes."""

import json
from datetime import datetime

from sim_visualize import parse_log, render


def finalize(run):
    """Retain raw files, merge captures and attach actual Go terminal results."""
    captures = sorted((run.directory / "replay").glob("capture-*.jsonl"))
    if not captures:
        run.say("Cluster replay: no acquired test clusters were captured.")
        return
    tests = [test for package in run.packages.values() for test in package.tests]
    output = run.directory / "cluster-replay.jsonl"
    with output.open("w", encoding="utf-8") as merged:
        for path in captures:
            header, ended = None, False
            for line in path.read_text(encoding="utf-8").splitlines():
                merged.write(line + "\n")
                try:
                    record = json.loads(line)
                except ValueError:
                    continue  # A killed writer can leave a partial final line.
                if not isinstance(record, dict):
                    continue
                if record.get("event") == "start":
                    header = record
                if record.get("event") == "end":
                    ended = True
            if not header or not ended:
                continue
            started = datetime.fromisoformat(header["time"].replace("Z", "+00:00")).timestamp()
            matches = [test for test in tests if test.name == header["test"] and
                       test.start <= started and test.end is not None and started <= test.end]
            if len(matches) == 1 and matches[0].status in ("PASS", "FAIL", "SKIP"):
                merged.write(json.dumps({"replay": "gquorum/v1", "id": header["id"],
                                         "event": "result", "status": matches[0].status}) + "\n")
    data = parse_log(output.read_text(encoding="utf-8"), output.name)
    page = run.directory / "cluster-replay.html"
    page.write_text(render(data), encoding="utf-8")
    run.say(f"Cluster replay: {page}")
    run.say(f"Replay logs: {output} ({len(captures)} isolated captures)")
