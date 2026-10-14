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


def lint_module(root, module, variants, revive, staticcheck, golangci=None):
    directory = root / module
    files = set()
    failed = False
    production_variants = set()
    for variant in variants:
        flags = build_flags(variant)
        print(f"Lint {module}: vet {variant}", flush=True)
        subprocess.run(["go", "vet", *flags, "./..."], cwd=directory, check=True)
        packages = subprocess.run(
            ["go", "list", "-json", *flags, "./..."], cwd=directory,
            check=True, stdout=subprocess.PIPE, text=True,
        )
        files.update(source_files(packages.stdout))
        production = frozenset(source_files(packages.stdout, include_tests=False))
        if golangci and module != "tests/cluster" and production not in production_variants:
            production_variants.add(production)
            print(f"Lint {module}: golangci production {variant}", flush=True)
            tags = [] if variant == "unit" else ["--build-tags=" + variant]
            result = subprocess.run(
                [golangci, "run", "--config", str(root / ".golangci.yml"),
                 "--tests=false", "--timeout=5m", "--max-issues-per-linter=0",
                 "--max-same-issues=0", "--uniq-by-line=false", *tags, "./..."],
                cwd=directory,
            )
            failed |= result.returncode != 0
    # The package-aware revive wrapper accepts explicit files, preserving package
    # comments and cross-file type information across all compiled variants.
    print(f"Lint {module}: revive all selected source files", flush=True)
    result = subprocess.run(
        [revive, "-config", str(root / "revive.toml"), *sorted(files)], cwd=directory,
    )
    failed |= result.returncode != 0
    # U1000 merges variants so a shared helper used only by a functional or Docker
    # test is retained. Neither analyzer executes tests or starts Docker services.
    matrix = "".join(f"{variant}: {' '.join(build_flags(variant))}\n" for variant in variants)
    print(f"Lint {module}: merged unused declarations", flush=True)
    result = subprocess.run(
        [staticcheck, "-checks=U1000", "-matrix", "./..."], cwd=directory,
        input=matrix, text=True,
    )
    return failed or result.returncode != 0


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
