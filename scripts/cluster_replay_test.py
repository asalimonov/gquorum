"""Docker replay boundaries, final test outcomes and retained observations."""

import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from cluster_replay import finalize
from sim_visualize import parse_log
from test_report import timestamp
import test_report


def line(identity, event, **fields):
    return json.dumps(dict(replay="gquorum/v1", id=identity, event=event, **fields))


def capture(identity, test, second=1):
    return [
        line(identity, "start", test=test, time=f"2026-10-20T12:00:{second:02}Z"),
        line(identity, "observation", time=f"2026-10-20T12:00:{second+1:02}Z",
             machine="node1", msg="observed metadata leader", role="leader", term=4,
             health="startup-ready", details={"runtime": {"session_live": False},
                                            "cluster": {"target_version": 3}}),
        line(identity, "end", duration=4),
    ]


class ClusterReplayTest(unittest.TestCase):
    def test_runner_enables_capture_only_for_explicit_option(self):
        with tempfile.TemporaryDirectory() as root:
            for enabled in ("0", "1"):
                with self.subTest(enabled=enabled):
                    run = MagicMock(directory=Path(root))
                    run.command.return_value = 0
                    run.finish.return_value = 0
                    with patch.object(test_report, "Run", return_value=run), \
                            patch("cluster_replay.finalize") as finalizer, \
                            patch.dict(os.environ, {"GQUORUM_TEST_REPLAY_DIR": "/stale/destination"}):
                        self.assertEqual(test_report.main(["cluster", "--cluster-replay", enabled]), 0)
                    env = run.command.call_args.kwargs["env"]
                    if enabled == "1":
                        self.assertEqual(env["GQUORUM_TEST_REPLAY_DIR"], str(Path(root) / "replay"))
                        finalizer.assert_called_once_with(run)
                    else:
                        self.assertNotIn("GQUORUM_TEST_REPLAY_DIR", env)
                        finalizer.assert_not_called()

    def test_parallel_and_repeated_captures_do_not_share_state(self):
        first = capture("one", "TestA")
        second = capture("two", "TestA")
        data = parse_log("\n".join([first[0], second[0], first[1], second[1],
                                   second[2], first[2],
                                   line("one", "result", status="PASS")]), "replay.jsonl")
        self.assertEqual(len(data["tests"]), 2)
        self.assertEqual([t["status"] for t in data["tests"]], ["PASS", "INCOMPLETE"])
        self.assertEqual([len(t["events"]) for t in data["tests"]], [1, 1])
        test = data["tests"][0]
        self.assertEqual(test["clock"], "wall")
        self.assertEqual(test["events"][0]["at"], 1)
        self.assertEqual(test["events"][0]["details"]["runtime"]["session_live"], False)
        self.assertEqual(test["events"][0]["details"]["cluster"]["target_version"], 3)

    def test_unreachable_observation_clears_prior_role(self):
        records = capture("one", "TestA")[:2]
        records.append(line("one", "observation", time="2026-10-20T12:00:03Z",
                            machine="node1", msg="Runtime status unavailable", role="unknown",
                            term=0, health="unreachable", details={"runtime_error": "timeout"}))
        test = parse_log("\n".join(records), "replay.jsonl")["tests"][0]
        self.assertEqual(test["events"][-1]["role"], "unknown")
        self.assertEqual(test["events"][-1]["health"], "unreachable")
        self.assertEqual(test["status"], "INCOMPLETE")

    def test_finalizer_matches_repeated_tests_by_time_and_keeps_partial_files(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / "replay").mkdir()
            raw = {
                "one": capture("one", "TestA"),
                "two": capture("two", "TestA", 11),
                "partial": capture("partial", "TestA", 11)[:2] + ['{"truncated":'],
            }
            for identity, records in raw.items():
                (directory / "replay" / f"capture-{identity}.jsonl").write_text("\n".join(records))
            tests = [SimpleNamespace(name="TestA", start=timestamp("2026-10-20T12:00:00Z"),
                                     end=timestamp("2026-10-20T12:00:06Z"), status="PASS"),
                     SimpleNamespace(name="TestA", start=timestamp("2026-10-20T12:00:10Z"),
                                     end=timestamp("2026-10-20T12:00:16Z"), status="FAIL")]
            run = SimpleNamespace(directory=directory, packages={"p": SimpleNamespace(tests=tests)}, say=lambda _: None)
            finalize(run)
            data = parse_log((directory / "cluster-replay.jsonl").read_text(), "capture")
            self.assertEqual({t["capture_id"]: t["status"] for t in data["tests"]},
                             {"one": "PASS", "two": "FAIL", "partial": "INCOMPLETE"})
            self.assertTrue((directory / "cluster-replay.html").exists())
            for identity, records in raw.items():
                self.assertEqual((directory / "replay" / f"capture-{identity}.jsonl").read_text(),
                                 "\n".join(records))

    def test_no_acquisition_produces_no_fake_replay(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            finalize(SimpleNamespace(directory=directory, say=lambda _: None))
            self.assertFalse((directory / "cluster-replay.html").exists())
