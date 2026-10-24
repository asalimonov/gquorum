import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

from sim_events import Events, LockedOutput, Workers, parse_seeds, run_matrix


class SimulationEventsTest(unittest.TestCase):
    def collect(self, lines, code=0):
        output = io.StringIO()
        events = Events(output)
        for line in lines:
            events.line(line + "\n")
        result = events.finish(code)
        return result, [json.loads(line) for line in output.getvalue().splitlines()]

    def test_seeds_are_separate_tests(self):
        code, events = self.collect([
            "=== RUN   TestQuorum (seed 1)",
            "detail",
            "--- PASS: TestQuorum (60.000s simulated)",
            "=== RUN   TestQuorum (seed 2)",
            "--- PASS: TestQuorum (62.000s simulated)",
        ])
        self.assertEqual(code, 0)
        self.assertEqual([e["Test"] for e in events if e["Action"] == "pass" and "Test" in e],
                         ["TestQuorum/seed=1", "TestQuorum/seed=2"])
        self.assertTrue(any(e.get("Output") == "detail\n" for e in events))
        self.assertEqual(events[-1]["Action"], "pass")

    def test_build_failure_preserves_code_and_output(self):
        code, events = self.collect(["unsupported package grpc"], 7)
        self.assertEqual(code, 7)
        self.assertEqual(events[-1]["Action"], "fail")
        self.assertIn("unsupported package grpc\n", [e.get("Output") for e in events])

    def test_empty_selection_cannot_pass(self):
        code, _ = self.collect([])
        self.assertEqual(code, 1)

    def test_truncated_run_is_failed(self):
        code, events = self.collect(["=== RUN   TestQuorum (seed 12)", "panic: broken"])
        self.assertEqual(code, 1)
        self.assertEqual([e["Test"] for e in events if e["Action"] == "fail" and "Test" in e],
                         ["TestQuorum/seed=12"])

    def test_failed_test_cannot_be_hidden_by_zero_exit(self):
        code, _ = self.collect(["=== RUN   TestQuorum (seed 1)", "--- FAIL: TestQuorum (1.000s simulated)"])
        self.assertEqual(code, 1)

    def test_skip_cannot_claim_simulation_coverage(self):
        code, _ = self.collect(["=== RUN   TestQuorum (seed 1)", "--- SKIP: TestQuorum (0.000s simulated)"])
        self.assertEqual(code, 1)

    def test_mismatched_or_duplicate_result_fails(self):
        for lines in [
            ["=== RUN   TestQuorum (seed 1)", "--- PASS: TestWrong (0.000s simulated)"],
            ["--- PASS: TestQuorum (0.000s simulated)"],
            ["=== RUN   TestQuorum (seed 1)", "--- PASS: TestQuorum (0.000s simulated)",
             "=== RUN   TestQuorum (seed 1)", "--- PASS: TestQuorum (0.000s simulated)"],
        ]:
            with self.subTest(lines=lines):
                code, _ = self.collect(lines)
                self.assertEqual(code, 1)

    def test_exact_worker_identity_and_missing_result(self):
        for lines in ([], ["=== RUN TestWrong (seed 2)", "--- PASS: TestWrong (1s simulated)"]):
            events = Events(io.StringIO(), ("TestExpected", 1))
            for line in lines:
                events.line(line + "\n")
            self.assertNotEqual(events.finish(0), 0)

    def test_seed_ranges_reject_duplicates_and_overflow(self):
        self.assertEqual(parse_seeds("-2--1,1-3"), [-2, -1, 1, 2, 3])
        for value in ("1,1", "3-2", "abc", "", str(2**63), "1-200000"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_seeds(value)

    def test_workers_overlap_but_seeds_do_not(self):
        class Fixture:
            stopped = threading.Event()

            def __init__(self):
                self.lock = threading.Lock()
                self.barrier = threading.Barrier(2)
                self.active = 0
                self.peak = 0
                self.spans = []

            def run(self, binary, name, seed, trace):
                start = time.monotonic()
                with self.lock:
                    self.active += 1
                    self.peak = max(self.peak, self.active)
                self.barrier.wait(timeout=2)
                time.sleep(.01)
                with self.lock:
                    self.active -= 1
                    self.spans.append((seed, start, time.monotonic()))
                return 7 if seed == 1 and name == "TestB" else 0

        fixture = Fixture()
        self.assertEqual(run_matrix(None, ["TestA", "TestB"], [1, 2], 2, fixture), 7)
        self.assertEqual(fixture.peak, 2)
        self.assertLessEqual(max(end for seed, _, end in fixture.spans if seed == 1),
                             min(start for seed, start, _ in fixture.spans if seed == 2))

    def test_timeout_reaps_child_and_marks_unfinished_test_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake"
            binary.write_text(f"#!{sys.executable}\nimport time\n"
                              "print('=== RUN TestWait (seed 1)', flush=True)\ntime.sleep(30)\n")
            binary.chmod(0o700)
            output = io.StringIO()
            workers = Workers(LockedOutput(output), .2)
            self.assertNotEqual(workers.run(binary, "TestWait", 1), 0)
            self.assertFalse(workers.processes)
            events = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertTrue(any(e["Action"] == "fail" and e.get("Test") == "TestWait/seed=1" for e in events))
            self.assertTrue(any("host timeout" in e.get("Output", "") for e in events))

    def test_interruption_reaps_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake"
            binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(30)\n")
            binary.chmod(0o700)
            workers = Workers(LockedOutput(io.StringIO()), 30)
            thread = threading.Thread(target=workers.run, args=(binary, "TestWait", 1))
            thread.start()
            deadline = time.monotonic() + 2
            while not workers.processes and time.monotonic() < deadline:
                time.sleep(.005)
            workers.interrupt()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertFalse(workers.processes)
