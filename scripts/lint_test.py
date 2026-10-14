import contextlib
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import lint


class LintTest(unittest.TestCase):
    def test_package_stream_includes_external_tests_and_deduplicates_files(self):
        stream = "\n" + json.dumps({"Dir": "/module/p", "GoFiles": ["p.go"],
                                   "TestGoFiles": ["shared_test.go"],
                                   "XTestGoFiles": ["external_test.go"]})
        stream += "\n" + json.dumps({"Dir": "/module/p", "GoFiles": ["p.go"],
                                    "TestGoFiles": ["functional_test.go"]}) + "\n"
        self.assertEqual(lint.source_files(stream), {
            "/module/p/p.go", "/module/p/shared_test.go", "/module/p/external_test.go",
            "/module/p/functional_test.go",
        })

    def test_both_analyzer_failures_propagate_and_build_variants_are_merged(self):
        for failed_tool in ("revive", "staticcheck", None):
            with self.subTest(failed_tool=failed_tool):
                commands = []

                def run(command, **kwargs):
                    commands.append((command, kwargs))
                    variant = next((v for v in command if v.startswith("-tags=")), "unit")
                    output = json.dumps({"Dir": "/module/p", "GoFiles": ["p.go"],
                                         "TestGoFiles": [variant + "_test.go"]})
                    return subprocess.CompletedProcess(command, int(command[0] == failed_tool), output)

                with patch.object(lint.subprocess, "run", side_effect=run), contextlib.redirect_stdout(io.StringIO()):
                    failed = lint.lint_module(Path("/root"), "tests/cluster",
                                              ("unit", "functional", "clustertest", "example"),
                                              "revive", "staticcheck")
                self.assertEqual(failed, failed_tool is not None)
                revive_command = next(command for command, _ in commands if command[0] == "revive")
                self.assertIn("/module/p/-tags=clustertest_test.go", revive_command)
                self.assertIn("/module/p/-tags=example_test.go", revive_command)
                self.assertEqual(revive_command.count("/module/p/p.go"), 1)
                check_command, options = next(c for c in commands if c[0][0] == "staticcheck")
                self.assertIn("-checks=U1000", check_command)
                self.assertEqual(options["input"], "unit: \nfunctional: -tags=functional\nclustertest: -tags=clustertest\nexample: -tags=example\n")
                self.assertTrue(all(options["cwd"] == Path("/root/tests/cluster") for _, options in commands))

    def test_golangci_uses_production_sources_and_propagates_failures(self):
        for module in (".", "tests/cluster"):
            with self.subTest(module=module):
                commands = []

                def run(command, **kwargs):
                    commands.append(command)
                    output = json.dumps({"Dir": "/module/p", "GoFiles": ["p.go"],
                                         "TestGoFiles": ["variant_test.go"]})
                    return subprocess.CompletedProcess(command, int(command[0] == "golangci"), output)

                with patch.object(lint.subprocess, "run", side_effect=run), contextlib.redirect_stdout(io.StringIO()):
                    failed = lint.lint_module(Path("/root"), module, ("unit", "functional"),
                                              "revive", "staticcheck", "golangci")
                checks = [command for command in commands if command[0] == "golangci"]
                self.assertEqual(failed, module == ".")
                if module == ".":
                    self.assertEqual(len(checks), 1)
                    self.assertIn("--tests=false", checks[0])
                    self.assertIn("/root/.golangci.yml", checks[0])
                    self.assertIn("--max-issues-per-linter=0", checks[0])
                else:
                    self.assertEqual(checks, [])

    def test_build_failure_is_not_reported_as_clean_lint(self):
        with patch.object(lint.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["go", "vet"])), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError):
                lint.lint_module(Path("/root"), ".", ("unit",), "revive", "staticcheck")


if __name__ == "__main__":
    unittest.main()
