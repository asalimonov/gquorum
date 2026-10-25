#!/usr/bin/env python3
"""Lint changed Go files: staged contents by default, or the working tree.

Without arguments, staged files are linted in an isolated copy of the Git
index, as the pre-commit hook requires. With --pending, staged, unstaged and
untracked files are linted in place using their working-tree contents.
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import lint


TOOLS = {
    "revive": ".tools/lint/revive-v1.17.1/revive-packages",
    "staticcheck": ".tools/lint/staticcheck-v0.8.2/staticcheck",
    "golangci": ".tools/lint/golangci-v2.14.0/golangci-lint",
}


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root)


def staged_scopes(paths):
    """Map paths to their owning module; config changes check the full scope."""
    modules = sorted((name for name, _ in lint.MODULES), key=len, reverse=True)
    scopes = {}
    for path in paths:
        if path in (".golangci.yml", "revive.toml"):
            return {name: None for name in modules}
        module = next((name for name in modules if name != "." and path.startswith(name + "/")), ".")
        relative = path if module == "." else path[len(module) + 1:]
        if relative in ("go.mod", "go.sum"):
            scopes[module] = None
        elif path.endswith(".go") and scopes.get(module, set()) is not None:
            scopes.setdefault(module, set()).add(path)
    return scopes


def lint_snapshot(root, snapshot, scopes):
    tools = {name: str(root / path) for name, path in TOOLS.items()}
    failed = False
    for module, variants in lint.MODULES:
        if module not in scopes:
            continue
        paths = scopes[module]
        if paths is None:
            failed |= lint.lint_module(snapshot, module, variants, **tools)
            continue
        directory = snapshot / module
        # Recursive patterns let Go skip a deleted package or one absent under
        # a build tag. Reports are still limited to the exact staged filenames.
        parents = {str((snapshot / path).parent.relative_to(directory)) for path in paths}
        patterns = tuple("./" if parent == "." else "./" + parent + "/..." for parent in sorted(parents))
        if any(not any((directory / parent).glob("*.go")) for parent in parents):
            # Removing an entire package can break its importers elsewhere.
            patterns = ("./...",)
        selected = {str(snapshot / path) for path in paths}
        failed |= lint.lint_module(snapshot, module, variants, patterns=patterns, selected=selected, **tools)
    return int(failed)


def changed_paths(root, pending):
    """List staged paths, plus unstaged and untracked ones in pending mode."""
    commands = [("diff", "--cached", "--name-only", "--no-renames", "-z", "--diff-filter=ACMRD")]
    if pending:
        commands += [("diff", "--name-only", "--no-renames", "-z", "--diff-filter=ACMRD"),
                     ("ls-files", "--others", "--exclude-standard", "-z")]
    paths = set()
    for command in commands:
        paths.update(os.fsdecode(path) for path in git(root, *command).split(b"\0") if path)
    return sorted(paths)


def main(argv=()):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pending", action="store_true",
                        help="lint staged, unstaged and untracked files in the working tree")
    args = parser.parse_args(argv)
    label = "Pending lint" if args.pending else "Staged lint"
    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").decode().strip()).resolve()
    scopes = staged_scopes(changed_paths(root, args.pending))
    if not scopes:
        print(f"{label}: no Go files or lint configuration changes.")
        return 0
    if args.pending:
        # Working-tree contents are the subject, so analyze them in place.
        subprocess.run(["make", "--no-print-directory", "lint-tools"], cwd=root, check=True)
        return lint_snapshot(root, root, scopes)
    # Capture the index before installing tools; checkout-index never modifies
    # the caller's index or working tree, including partially staged files.
    with tempfile.TemporaryDirectory(prefix="gquorum-staged-lint-") as temporary:
        snapshot = Path(temporary).resolve()
        git(root, "checkout-index", "--all", "--prefix=" + str(snapshot) + os.sep)
        subprocess.run(["make", "--no-print-directory", "lint-tools"], cwd=root, check=True)
        # Git exports repository variables to hooks. The detached snapshot must
        # not discover the real index or a parent workspace during analysis.
        for name in git(root, "rev-parse", "--local-env-vars").decode().splitlines():
            os.environ.pop(name, None)
        os.environ["GOWORK"] = "off"
        return lint_snapshot(root, snapshot, scopes)


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"Changed-file lint failed: {error}")
        raise SystemExit(1)
