#!/usr/bin/env python3
"""Vet all builds and lint unused code across each module's merged build matrix."""

import argparse
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    (".", ("unit", "functional")),
    ("examples/cluster", ("unit", "functional")),
    ("tests/cluster", ("unit", "functional", "clustertest", "example")),
)


def build_flags(variant):
    return [] if variant == "unit" else ["-tags=" + variant]


def source_files(output):
    """Read Go's concatenated package JSON, including both kinds of tests."""
    decoder = json.JSONDecoder()
    files = set()
    while output.strip():
        package, end = decoder.raw_decode(output.lstrip())
        output = output.lstrip()[end:]
        for key in ("GoFiles", "CgoFiles", "TestGoFiles", "XTestGoFiles"):
            for name in package.get(key, ()):
                files.add(str(Path(package["Dir"]) / name))
    return files


def lint_module(root, module, variants, revive, staticcheck):
    directory = root / module
    files = set()
    failed = False
    for variant in variants:
        flags = build_flags(variant)
        print(f"Lint {module}: vet {variant}", flush=True)
        subprocess.run(["go", "vet", *flags, "./..."], cwd=directory, check=True)
        packages = subprocess.run(
            ["go", "list", "-json", *flags, "./..."], cwd=directory,
            check=True, stdout=subprocess.PIPE, text=True,
        )
        files.update(source_files(packages.stdout))
    # Revive's CLI does not expose build tags. Explicit source paths bypass its
    # default go/build selection; these AST-only rules see every compiled variant.
    print(f"Lint {module}: revive all selected source files", flush=True)
    result = subprocess.run(
        [revive, "-config", str(root / "revive.toml"), "-set_exit_status",
         "-formatter", "friendly", *sorted(files)], cwd=directory,
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
    args = parser.parse_args()
    revive, staticcheck = str(Path(args.revive).resolve()), str(Path(args.staticcheck).resolve())
    failed = False
    for module, variants in MODULES:
        failed |= lint_module(ROOT, module, variants, revive, staticcheck)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
