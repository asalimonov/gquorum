#!/usr/bin/env python3
"""Vet all builds and lint style and unused code across merged build matrices."""

import argparse
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    (".", ("unit", "functional")),
    ("examples/cluster", ("unit", "functional")),
    ("examples/scheduler", ("unit", "functional")),
    ("examples/inmemory-cache", ("unit", "functional")),
    ("tests/cluster", ("unit", "functional", "clustertest", "example")),
    ("scripts/revive", ("unit",)),
    ("scripts/profiledecode", ("unit",)),
)


def build_flags(variant):
    return [] if variant == "unit" else ["-tags=" + variant]


def source_files(output, include_tests=True):
    """Read Go's concatenated package JSON, including both kinds of tests."""
    decoder = json.JSONDecoder()
    files = set()
    while output.strip():
        package, end = decoder.raw_decode(output.lstrip())
        output = output.lstrip()[end:]
        keys = ("GoFiles", "CgoFiles")
        if include_tests:
            keys += ("TestGoFiles", "XTestGoFiles")
        for key in keys:
            for name in package.get(key, ()):
                files.add(str(Path(package["Dir"]) / name))
    return files


def staged_findings(tool, output):
    """Normalize the pinned analyzers' JSON diagnostics."""
    if tool == "golangci":
        for issue in json.loads(output).get("Issues", ()) or ():
            yield issue["Pos"], issue["Text"], issue["FromLinter"]
    elif tool == "revive":
        for issue in json.loads(output):
            yield issue["Position"]["Start"], issue["Failure"], issue["RuleName"]
    else:
        for line in output.splitlines():
            issue = json.loads(line)
            yield issue["location"], issue["message"], issue["code"]


def run_analyzer(command, directory, tool, selected=None, root=None, **kwargs):
    if selected is None:
        return subprocess.run(command, cwd=directory, **kwargs).returncode != 0
    result = subprocess.run(command, cwd=directory, stdout=subprocess.PIPE, text=True, **kwargs)
    if result.returncode not in (0, 1):
        print(result.stdout, end="")
        return True
    if not result.stdout.strip():
        return result.returncode != 0
    failed = False
    try:
        for position, message, rule in staged_findings(tool, result.stdout):
            filename = position.get("Filename", position.get("file", ""))
            path = Path(filename)
            if not path.is_absolute():
                path = directory / path
            if str(path) not in selected and rule not in ("typecheck", "compile"):
                continue
            line = position.get("Line", position.get("line", 0))
            column = position.get("Column", position.get("column", 0))
            name = path.relative_to(root) if path.is_relative_to(root) else path
            print(f"{name}:{line}:{column}: {message} ({rule})", flush=True)
            failed = True
    except (ValueError, KeyError, TypeError):
        print(result.stdout, end="")
        return True
    return failed


def lint_module(root, module, variants, revive, staticcheck, golangci=None,
                patterns=("./...",), selected=None):
    directory = root / module
    files = set()
    failed = False
    production_variants = set()
    for variant in variants:
        flags = build_flags(variant)
        print(f"Lint {module}: vet {variant}", flush=True)
        subprocess.run(["go", "vet", *flags, *patterns], cwd=directory, check=True)
        packages = subprocess.run(
            ["go", "list", "-json", *flags, *patterns], cwd=directory,
            check=True, stdout=subprocess.PIPE, text=True,
        )
        files.update(source_files(packages.stdout))
        production = frozenset(source_files(packages.stdout, include_tests=False))
        if golangci and module != "tests/cluster" and production and production not in production_variants:
            production_variants.add(production)
            print(f"Lint {module}: golangci production {variant}", flush=True)
            tags = [] if variant == "unit" else ["--build-tags=" + variant]
            output_flags = [] if selected is None else ["--output.text.path=", "--output.json.path=stdout",
                                                       "--show-stats=false", "--path-mode=abs"]
            failed |= run_analyzer(
                [golangci, "run", "--config", str(root / ".golangci.yml"),
                 "--tests=false", "--timeout=5m", "--max-issues-per-linter=0",
                 "--max-same-issues=0", "--uniq-by-line=false", *tags, *output_flags, *patterns],
                directory, "golangci", selected, root,
            )
    # The package-aware revive wrapper accepts explicit files, preserving package
    # comments and cross-file type information across all compiled variants.
    print(f"Lint {module}: revive all selected source files", flush=True)
    if files:
        output_flags = [] if selected is None else ["-formatter", "json"]
        failed |= run_analyzer(
            [revive, "-config", str(root / "revive.toml"), *output_flags, *sorted(files)],
            directory, "revive", selected, root,
        )
    # U1000 merges variants so a shared helper used only by a functional or Docker
    # test is retained. Neither analyzer executes tests or starts Docker services.
    matrix = "".join(f"{variant}: {' '.join(build_flags(variant))}\n" for variant in variants)
    print(f"Lint {module}: merged unused declarations", flush=True)
    output_flags = [] if selected is None else ["-f", "json"]
    failed |= run_analyzer(
        [staticcheck, "-checks=U1000", "-matrix", *output_flags, *patterns],
        directory, "staticcheck", selected, root, input=matrix,
        **({"text": True} if selected is None else {}),
    )
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revive", required=True)
    parser.add_argument("--staticcheck", required=True)
    parser.add_argument("--golangci", required=True)
    args = parser.parse_args()
    revive, staticcheck = str(Path(args.revive).resolve()), str(Path(args.staticcheck).resolve())
    golangci = str(Path(args.golangci).resolve())
    failed = False
    for module, variants in MODULES:
        failed |= lint_module(ROOT, module, variants, revive, staticcheck, golangci)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
