"""Runner contract tests; subprocesses are local CLI fixtures, not cluster services."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("quint_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.profile = runner.load_profile(runner.MODEL_ROOT / "profiles/foundation.json")

    def output(self, value):
        path = self.directory / "output.json"
        path.write_text(json.dumps(value))
        return path

    def result(self, code=0, **kwargs):
        return runner.CommandResult(["quint"], code, 0.1, "stdout", "stderr", **kwargs)

    def test_profile_and_explicit_sample_counts(self):
        runner.validate_profile(self.profile, runner.MODEL_ROOT)
        commands = runner.build_commands(self.profile, "simulate", 42)
        command = commands[-1]
        self.assertEqual(command[command.index("--seed") + 1], "42")
        self.assertEqual(command[command.index("--max-samples") + 1], "50")
        self.assertEqual(command[command.index("--max-steps") + 1], "20")
        self.assertIn("--invariant", command)

    def test_invalid_profiles_cannot_silently_skip_checks(self):
        changes = [
            {"tests": []}, {"properties": []}, {"bounds": {}}, {"bounds": {"nodes": True}},
            {"max_samples": 0}, {"max_steps": -1}, {"timeout_seconds": False},
            {"quint_version": "latest"}, {"schema_version": 99}, {"backend": "unknown"},
            {"instance": "../../../go.mod"}, {"main": "x; touch bad"},
            {"temporal": ["progress"], "verification_backend": "apalache"},
            {"tests": [{"file": "instances/foundation.qnt", "main": "foundation", "expected": []}]},
        ]
        for change in changes:
            with self.subTest(change=change):
                profile = copy.deepcopy(self.profile)
                profile.data.update(change)
                with self.assertRaises((runner.ProfileError, ValueError)):
                    runner.validate_profile(profile, runner.MODEL_ROOT)

    def test_unqualified_verification_and_replay_are_refused(self):
        for mode in ("verify", "replay"):
            with self.assertRaises(runner.ProfileError):
                runner.build_commands(self.profile, mode, 1)

    def test_bounds_are_executed_in_concrete_instance(self):
        command = runner.bounds_command(self.profile, 1)
        self.assertIn(str(self.profile.path("instances/foundation.qnt")), command)
        self.assertEqual(command[command.index("--max-steps") + 1], "0")
        self.assertIn('"nodeCount" -> 1', command[command.index("--invariant") + 1])

    def test_expected_tests_are_required_and_zero_is_not_success(self):
        data = {"stage": "testing", "errors": [], "passed": ["M::a"], "failed": [], "ignored": []}
        self.assertEqual(runner.classify(self.result(), self.output(data), "test", "typescript", ["a"]), "tests-pass")
        for field, value in (("passed", []), ("passed", ["M::other"]), ("ignored", ["b"])):
            changed = {**data, field: value}
            self.assertEqual(runner.classify(self.result(), self.output(changed), "test", "typescript", ["a"]),
                             "infrastructure-error")
        changed = {**data, "passed": [], "failed": ["M::a"]}
        self.assertEqual(runner.classify(self.result(1), self.output(changed), "test", "typescript", ["a"]), "test-failure")

    def test_parser_error_is_not_a_counterexample(self):
        data = {"stage": "running", "status": "violation", "errors": [{"code": "QNT404"}]}
        self.assertEqual(runner.classify(self.result(1), self.output(data), "run", "typescript"), "infrastructure-error")
        data["errors"] = []
        self.assertEqual(runner.classify(self.result(1), self.output(data), "run", "typescript"), "counterexample")

    def test_empty_trace_or_unsupported_status_cannot_pass(self):
        for data in ({"stage": "running", "status": "ok", "errors": [], "trace": []},
                     {"stage": "running", "status": "error", "errors": []},
                     {"stage": "running", "status": "ok"}, {}):
            self.assertEqual(runner.classify(self.result(), self.output(data), "run", "typescript"), "infrastructure-error")

    def test_sampled_bounded_exhaustive_are_distinct(self):
        data = {"stage": "running", "status": "ok", "errors": [], "trace": [{}]}
        self.assertEqual(runner.classify(self.result(), self.output(data), "run", "typescript"), "sampled-pass")
        data["stage"] = "verifying"
        self.assertEqual(runner.classify(self.result(), self.output(data), "verify", "apalache"), "bounded-pass")
        self.assertEqual(runner.classify(self.result(), self.output(data), "verify", "tlc"), "exhaustive-complete")
        self.assertEqual(runner.classify(self.result(0, timed_out=True), self.output(data), "verify", "tlc"), "incomplete")

    def test_child_exit_and_separate_logs_are_preserved(self):
        cmd = [sys.executable, "-c", "import sys; print('result'); print('diagnostic', file=sys.stderr); sys.exit(7)"]
        result = runner.run_command(cmd, 5, self.directory / "child")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(Path(result.stdout).read_text().strip(), "result")
        self.assertEqual(Path(result.stderr).read_text().strip(), "diagnostic")

    def test_missing_executable_and_timeout_are_errors(self):
        missing = runner.run_command([str(self.directory / "missing")], 1, self.directory / "missing")
        self.assertIsNone(missing.returncode)
        self.assertTrue(missing.error)
        result = runner.run_command([sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)"],
                                    0.2, self.directory / "timeout")
        self.assertTrue(result.timed_out)
        self.assertLess(result.elapsed_seconds, 3)
        self.assertIsNotNone(result.returncode)
        self.assertIn("started", Path(result.stdout).read_text())
        self.assertEqual(runner.classify(result, self.directory / "absent.json", "run", "typescript"), "incomplete")

    def test_signal_exit_remains_incomplete(self):
        result = runner.run_command([sys.executable, "-c", "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"],
                                    5, self.directory / "signal")
        self.assertEqual(result.returncode, -signal.SIGTERM)
        self.assertEqual(runner.classify(result, self.directory / "absent.json", "run", "typescript"), "incomplete")

    def test_latest_only_moves_after_completed_report(self):
        completed = self.directory / "complete"
        completed.mkdir()
        manifest = {"directory": str(completed), "profile": "foundation", "mode": "test",
                    "result": "tests-pass", "completed": True}
        runner.write_report(manifest, [])
        self.assertEqual((self.directory / "latest").resolve(), completed)
        pending = self.directory / "pending"
        pending.mkdir()
        for status, done in (("typecheck-pass", False), ("incomplete", True)):
            runner.write_report({**manifest, "directory": str(pending), "result": status, "completed": done}, [])
            self.assertEqual((self.directory / "latest").resolve(), completed)
        self.assertTrue((pending / "manifest.json").exists())

    def test_missing_backends_do_not_trigger_downloads(self):
        with mock.patch.dict(os.environ, {"QUINT_HOME": str(self.directory)}):
            self.assertEqual(runner.validate_backend(self.profile, "test")["backend"], "typescript")
            with self.assertRaises(runner.ProfileError):
                runner.validate_backend(self.profile, "verify")
            self.profile.data["backend"] = "rust"
            with self.assertRaises(runner.ProfileError):
                runner.validate_backend(self.profile, "test")
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_invalid_profile_gets_failure_artifacts(self):
        with mock.patch("builtins.print"):
            code = runner.main(["--profile", "absent", "--mode", "test", "--reports-dir", str(self.directory)])
        self.assertNotEqual(code, 0)
        manifest = json.loads((self.directory / "quint/latest/manifest.json").read_text())
        self.assertEqual(manifest["result"], "infrastructure-error")
        self.assertEqual(manifest["commands"], [])

    def test_wrong_cli_version_stops_before_model_execution(self):
        bindir = self.directory / "bin"
        bindir.mkdir()
        binary = bindir / "quint"
        binary.write_text("#!" + sys.executable + "\nprint('0.1.0')\n")
        binary.chmod(0o755)
        with mock.patch.dict(os.environ, {"PATH": str(bindir) + os.pathsep + os.environ["PATH"]}), \
             mock.patch("builtins.print"):
            code = runner.main(["--profile", "foundation", "--mode", "test", "--reports-dir", str(self.directory)])
        self.assertEqual(code, 2)
        data = json.loads((self.directory / "quint/latest/manifest.json").read_text())
        self.assertIn("does not match", data["error"])
        self.assertEqual(len(data["commands"]), 1)

    def test_real_instance_rejects_manifest_bound_drift(self):
        profile = copy.deepcopy(self.profile)
        profile.data["bounds"]["maxRequests"] = 999
        with mock.patch.object(runner, "load_profile", return_value=profile), mock.patch("builtins.print"):
            code = runner.main(["--profile", "foundation", "--mode", "test", "--reports-dir", str(self.directory)])
        self.assertNotEqual(code, 0)
        data = json.loads((self.directory / "quint/latest/manifest.json").read_text())
        self.assertEqual(data["result"], "counterexample")
        self.assertEqual(data["commands"][-1]["name"], "1:run")
        self.assertEqual(data["configuration"]["bounds"]["maxRequests"], 999)
        self.assertTrue(list((self.directory / "quint/latest").glob("trace-*.itf.json")))

    def test_unwritable_report_root_returns_failure(self):
        blocked = self.directory / "not-a-directory"
        blocked.write_text("keep")
        with mock.patch("builtins.print"):
            code = runner.main(["--profile", "foundation", "--mode", "test", "--reports-dir", str(blocked)])
        self.assertEqual(code, 2)
        self.assertEqual(blocked.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
