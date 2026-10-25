import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import lint
import lint_staged


class StagedLintTest(unittest.TestCase):
    def test_scopes_include_deleted_and_renamed_files_in_their_modules(self):
        self.assertEqual(lint_staged.staged_scopes([
            "README.md", "old file.go", "new file.go", "examples/scheduler/main.go",
            "tests/cluster/testapp/deleted.go", "scripts/revive/main.go",
            "tests/sim/cluster_test.go",
        ]), {
            ".": {"old file.go", "new file.go"},
            "examples/scheduler": {"examples/scheduler/main.go"},
            "tests/cluster": {"tests/cluster/testapp/deleted.go"},
            "scripts/revive": {"scripts/revive/main.go"},
            "tests/sim": {"tests/sim/cluster_test.go"},
        })
        self.assertEqual(lint_staged.staged_scopes(["README.md"]), {})

    def test_config_and_module_changes_expand_scope(self):
        self.assertEqual(lint_staged.staged_scopes([
            "examples/scheduler/main.go", "examples/scheduler/go.mod", "examples/scheduler/task.go",
        ]), {"examples/scheduler": None})
        self.assertEqual(lint_staged.staged_scopes([".golangci.yml"]),
                         {name: None for name, _ in lint.MODULES})

    def test_partial_staging_uses_index_contents_and_preserves_worktree_and_index(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root)

            git("init", "-q")
            file = root / "file with spaces.go"
            file.write_text("package staged\n")
            git("add", "--", file.name)
            staged_tree = git("write-tree")
            file.write_text("unstaged invalid Go\n")
            real_run = subprocess.run
            observed = []

            def run(command, **kwargs):
                if command[0] == "make":
                    return subprocess.CompletedProcess(command, 0)
                return real_run(command, **kwargs)

            def snapshot(actual_root, directory, scopes):
                self.assertEqual(actual_root, root)
                self.assertEqual((directory / file.name).read_text(), "package staged\n")
                self.assertEqual(scopes, {".": {file.name}})
                self.assertNotIn("GIT_INDEX_FILE", os.environ)
                self.assertEqual(os.environ["GOWORK"], "off")
                observed.append(directory)
                return 1

            with patch.dict(os.environ, GIT_INDEX_FILE=str(root / ".git/index")), \
                    patch.object(lint_staged.Path, "cwd", return_value=root), \
                    patch.object(lint_staged.subprocess, "run", side_effect=run), \
                    patch.object(lint_staged, "lint_snapshot", side_effect=snapshot):
                self.assertEqual(lint_staged.main(), 1)
            self.assertEqual(len(observed), 1)
            self.assertFalse(observed[0].exists())
            self.assertEqual(git("write-tree"), staged_tree)
            self.assertEqual(file.read_text(), "unstaged invalid Go\n")

    def test_pending_mode_lints_staged_unstaged_and_untracked_files_in_place(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root)

            git("init", "-q")
            for name in ("staged.go", "unstaged.go", "clean.go", "deleted.go"):
                (root / name).write_text("package pending\n")
            git("add", "--", ".")
            git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "base")
            (root / "staged.go").write_text("package pending // staged\n")
            git("add", "--", "staged.go")
            (root / "unstaged.go").write_text("package pending // unstaged\n")
            (root / "deleted.go").unlink()
            (root / "untracked.go").write_text("package pending\n")
            (root / ".gitignore").write_text("ignored.go\n")
            (root / "ignored.go").write_text("package pending\n")
            index = git("write-tree")
            real_run = subprocess.run
            observed = []

            def run(command, **kwargs):
                if command[0] == "make":
                    return subprocess.CompletedProcess(command, 0)
                return real_run(command, **kwargs)

            def snapshot(actual_root, directory, scopes):
                observed.append((actual_root, directory, scopes))
                return 0

            with patch.object(lint_staged.Path, "cwd", return_value=root), \
                    patch.object(lint_staged.subprocess, "run", side_effect=run), \
                    patch.object(lint_staged, "lint_snapshot", side_effect=snapshot):
                self.assertEqual(lint_staged.main(["--pending"]), 0)
            self.assertEqual(observed, [(root, root, {".": {
                "staged.go", "unstaged.go", "deleted.go", "untracked.go"}})])
            self.assertEqual(git("write-tree"), index)

    def test_pending_mode_without_go_changes_does_not_install_or_run_tools(self):
        with patch.object(lint_staged, "git", side_effect=[b"/repo\n", b"README.md\0", b"", b"notes.txt\0"]), \
                patch.object(lint_staged.subprocess, "run") as run, \
                contextlib.redirect_stdout(io.StringIO()) as captured:
            self.assertEqual(lint_staged.main(["--pending"]), 0)
        run.assert_not_called()
        self.assertIn("Pending lint", captured.getvalue())

    def test_documentation_commit_does_not_install_or_run_tools(self):
        with patch.object(lint_staged, "git", side_effect=[b"/repo\n", b"README.md\0"]), \
                patch.object(lint_staged.subprocess, "run") as run, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(lint_staged.main(), 0)
        run.assert_not_called()

    def test_analyzers_filter_unstaged_files_but_keep_compilation_errors(self):
        for tool, output in (
            ("golangci", '{"Issues":[{"Pos":{"Filename":"other.go","Line":2},'
             '"Text":"bad","FromLinter":"lll"}]}'),
            ("revive", '[{"Position":{"Start":{"Filename":"other.go","Line":2}},'
             '"Failure":"bad","RuleName":"exported"}]'),
            ("staticcheck", '{"location":{"file":"other.go","line":2},'
             '"message":"unused","code":"U1000"}\n'),
        ):
            with self.subTest(tool=tool):
                result = subprocess.CompletedProcess([], 1, output)
                with patch.object(lint.subprocess, "run", return_value=result), \
                        contextlib.redirect_stdout(io.StringIO()) as captured:
                    self.assertFalse(lint.run_analyzer([tool], Path("/repo"), tool,
                                                      {"/repo/staged.go"}, Path("/repo")))
                    self.assertTrue(lint.run_analyzer([tool], Path("/repo"), tool,
                                                     {"/repo/other.go"}, Path("/repo")))
                self.assertIn("other.go:2", captured.getvalue())
        result = subprocess.CompletedProcess([], 1, '{"location":{"file":"other.go"},'
                                             '"message":"cannot compile","code":"compile"}\n')
        with patch.object(lint.subprocess, "run", return_value=result), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(lint.run_analyzer(["staticcheck"], Path("/repo"), "staticcheck",
                                             {"/repo/staged.go"}, Path("/repo")))

    def test_tool_errors_never_pass_as_filtered_findings(self):
        for code, output in ((1, ""), (1, "invalid json"), (2, '{"Issues":[]}')):
            with self.subTest(code=code, output=output), \
                    patch.object(lint.subprocess, "run", return_value=subprocess.CompletedProcess([], code, output)), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(lint.run_analyzer(["golangci"], Path("/repo"), "golangci",
                                                 {"/repo/staged.go"}, Path("/repo")))

    def test_nested_module_uses_absolute_golangci_paths_and_json_without_stats(self):
        commands = []

        def run(command, **kwargs):
            commands.append(command)
            if command[:2] == ["go", "list"]:
                output = '{"Dir":"/repo/examples/cluster","GoFiles":["main.go"]}'
            elif command[0] == "golangci":
                output = '{"Issues":[{"Pos":{"Filename":"/repo/examples/cluster/main.go","Line":4},'
                output += '"Text":"long line","FromLinter":"lll"}]}'
            elif command[0] == "revive":
                output = "[]"
            else:
                output = ""
            return subprocess.CompletedProcess(command, int(command[0] == "golangci"), output)

        with patch.object(lint.subprocess, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()) as captured:
            self.assertTrue(lint.lint_module(Path("/repo"), "examples/cluster", ("unit", "functional"),
                                            "revive", "staticcheck", "golangci", patterns=("./",),
                                            selected={"/repo/examples/cluster/main.go"}))
        command = next(command for command in commands if command[0] == "golangci")
        self.assertIn("--path-mode=abs", command)
        self.assertIn("--output.json.path=stdout", command)
        self.assertIn("--show-stats=false", command)
        self.assertEqual(command[-1], "./")
        self.assertIn("examples/cluster/main.go:4", captured.getvalue())

    def test_removed_package_checks_module_for_broken_importers(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(lint, "lint_module", return_value=False) as check:
            directory = Path(temporary)
            self.assertEqual(lint_staged.lint_snapshot(directory, directory,
                                                      {".": {"deleted/last.go"}}), 0)
        self.assertEqual(check.call_args.kwargs["patterns"], ("./...",))
        self.assertEqual(check.call_args.kwargs["selected"], {str(directory / "deleted/last.go")})


if __name__ == "__main__":
    unittest.main()
