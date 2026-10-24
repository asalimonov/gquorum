import json
from pathlib import Path
import tempfile
import unittest

from sim_visualize import duration, main, parse_log, render


def record(second, message, **extra):
    return json.dumps({"time": f"2020-01-15T14:10:{second:02}.000001234Z",
                       "machine": "node1", "msg": message, **extra})


class SimulationVisualizationTest(unittest.TestCase):
    def test_empty_viewer_needs_no_input_and_embeds_offline_parser(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "viewer.html"
            self.assertEqual(main(["-o", str(output)]), 0)
            html = output.read_text()
        self.assertIn('"tests": []', html)
        self.assertIn('id="drop-zone"', html)
        self.assertIn('id="file-input"', html)
        self.assertIn("SimLog.parse", html)
        self.assertNotIn("/*PLAYER*/", html)
        self.assertNotIn('<script src=', html)

    def test_multiple_tests_and_repeated_names_keep_their_own_state(self):
        data = parse_log("\n".join([
            "=== RUN TestA (seed 1)", record(3, "1 became leader at term 2"),
            "--- PASS: TestA (8.00s simulated)",
            "=== RUN TestA (seed 2)", record(3, "1 became follower at term 1"),
            "--- FAIL: TestA (2.00s simulated)",
        ]), "capture.log")
        self.assertEqual(len(data["tests"]), 2)
        self.assertEqual(data["tests"][0]["events"][0]["role"], "leader")
        self.assertEqual(data["tests"][1]["events"][0]["role"], "follower")
        self.assertEqual([t["status"] for t in data["tests"]], ["PASS", "FAIL"])
        self.assertEqual([t["seed"] for t in data["tests"]], [1, 2])

    def test_parallel_prefixes_and_go_events_do_not_mix_tests(self):
        raw = [
            "[TestA/seed=1] === RUN TestA (seed 1)",
            "[TestB/seed=1] === RUN TestB (seed 1)",
            "[TestA/seed=1] " + record(3, "1 became leader at term 2"),
            "[TestB/seed=1] " + record(3, "1 became candidate at term 3"),
            "[TestB/seed=1] --- PASS: TestB (3s simulated)",
            "[TestA/seed=1] --- PASS: TestA (4s simulated)",
        ]
        plain = parse_log("\n".join(raw), "parallel.log")
        wrapped = parse_log("\n".join(json.dumps({"Action": "output", "Output": line + "\n"})
                                     for line in raw), "parallel.log")
        self.assertEqual(plain, wrapped)
        self.assertEqual([t["events"][0]["role"] for t in plain["tests"]], ["leader", "candidate"])
        self.assertEqual([t["duration"] for t in plain["tests"]], [4, 3])

    def test_events_sort_by_virtual_time_then_step(self):
        data = parse_log("\n".join([
            "=== RUN TestA (seed 1)",
            record(4, "1 became leader at term 2", step=8),
            record(3, "1 became follower at term 1", step=2),
            record(4, "1 became candidate at term 2", step=7),
        ]), "capture.log")["tests"][0]
        self.assertEqual([e["step"] for e in data["events"]], [2, 7, 8])
        self.assertEqual(data["status"], "INCOMPLETE")
        self.assertEqual(data["events"][0]["at"], 0)

    def test_health_and_outage_do_not_manufacture_readiness_or_repairs(self):
        data = parse_log("\n".join([
            "=== RUN TestA (seed 1)",
            record(3, "group health changed", state="unknown", group="_meta", voters=3, quorum=2),
            record(5, "cycle=0 isolate=node1 interval=3s outage=2s", machine="main"),
            record(9, "quorum checks=60 failures=0 cadence=100ms deadline=5s slowest=2.3s", machine="main"),
            "--- PASS: TestA (8s simulated)",
        ]), "capture.log")["tests"][0]
        self.assertEqual(data["events"][0]["health"], "unknown")
        self.assertEqual(data["events"][1]["outage"], 2)
        self.assertEqual(len(data["events"]), 3)
        self.assertEqual(data["summary"]["checks"], 60)
        self.assertFalse(any(e["kind"] in ("ready", "repair") for e in data["events"]))

    def test_untrusted_messages_cannot_escape_embedded_json(self):
        malicious = "</script><script>window.pwned=true</script>"
        data = parse_log("=== RUN TestA (seed 1)\n" + record(3, malicious), malicious)
        html = render(data)
        self.assertNotIn(malicious, html)
        self.assertIn("\\u003c/script\\u003e", html)
        self.assertNotIn("/*DATA*/", html)
        self.assertIn("INCOMPLETE", html)

    def test_single_raw_capture_and_bad_input(self):
        data = parse_log(record(3, "1 became follower at term 1"), "raw.log")
        self.assertEqual(len(data["tests"]), 1)
        self.assertEqual(data["tests"][0]["nodes"], ["node1"])
        self.assertEqual(data["tests"][0]["status"], "INCOMPLETE")
        with self.assertRaises(ValueError):
            parse_log("not a simulation", "bad.log")

    def test_diagnostics_and_bad_timestamps_remain_visible(self):
        data = parse_log('=== RUN TestA (seed 1)\npanic: boom\n'
                         '{"time":"invalid","msg":"lost event"}', "broken.log")
        self.assertIn("panic", data["tests"][0]["diagnostics"][0]["text"])
        self.assertIn("invalid timestamp", data["warnings"][0])

    def test_compound_durations(self):
        self.assertEqual(duration("1m2.5s"), 62.5)
        self.assertEqual(duration("200ms"), .2)
        with self.assertRaises(ValueError):
            duration("bogus")
