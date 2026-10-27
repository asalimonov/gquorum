#!/usr/bin/env python3
"""Run pinned Quint profiles; keep model guarantees separate from tool failures."""

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional
import uuid


MODEL_ROOT = Path(__file__).resolve().parent
REPO_ROOT = MODEL_ROOT.parent.parent
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
QUINT_VERSION = "0.33.0"
EVALUATOR_VERSION = "v0.7.0"


class ProfileError(ValueError):
    pass


class CommandTimeout(Exception):
    pass


class Interrupted(Exception):
    def __init__(self, signum):
        self.signum = signum


@dataclass
class Profile:
    name: str
    data: Dict[str, Any]
    root: Path

    def path(self, relative):
        result = (self.root / relative).resolve()
        if not result.is_relative_to(self.root.resolve()) or not result.is_file():
            raise ProfileError("model input must be an existing file inside scripts/quint: " + relative)
        return result


@dataclass
class CommandResult:
    argv: List[str]
    returncode: Optional[int]
    elapsed_seconds: float
    stdout: str
    stderr: str
    timed_out: bool = False
    interrupted: Optional[int] = None
    error: Optional[str] = None


def _read_json(path):
    return json.loads(Path(path).read_text())


def load_profile(path):
    path = Path(path)
    try:
        data = _read_json(path)
    except (OSError, ValueError) as exc:
        raise ProfileError("cannot read profile: " + str(exc)) from exc
    return Profile(path.stem, data, path.parent.parent.resolve())


def _names(value, label, nonempty=True):
    if not isinstance(value, list) or (nonempty and not value):
        raise ProfileError(label + " must be a list" + (" with entries" if nonempty else ""))
    if any(not isinstance(x, str) or not IDENTIFIER.fullmatch(x) for x in value):
        raise ProfileError(label + " contains an invalid definition name")
    if len(value) != len(set(value)):
        raise ProfileError(label + " contains duplicates")


def validate_profile(profile, root):
    p = profile.data
    if not isinstance(p, dict) or p.get("schema_version") != 1:
        raise ProfileError("unsupported profile schema")
    if profile.root != Path(root).resolve() or not IDENTIFIER.fullmatch(profile.name):
        raise ProfileError("invalid profile location/name")
    if p.get("quint_version") != QUINT_VERSION:
        raise ProfileError("profile must pin Quint " + QUINT_VERSION)
    for field in ("main", "init", "step"):
        _names([p.get(field)], field)
    profile.path(p.get("instance", ""))
    bounds = p.get("bounds")
    if not isinstance(bounds, dict) or not bounds:
        raise ProfileError("bounds must be a nonempty map")
    for key, value in bounds.items():
        _names([key], "bound")
        if type(value) is not int or value < 0:
            raise ProfileError("bounds must be nonnegative integers")
    for field in ("max_steps", "max_samples", "test_samples", "timeout_seconds"):
        if type(p.get(field)) is not int or p[field] <= 0:
            raise ProfileError(field + " must be a positive integer")
    _names(p.get("properties"), "properties")
    _names(p.get("witnesses", []), "witnesses", False)
    _names(p.get("temporal", []), "temporal", False)
    if p.get("backend") not in ("typescript", "rust"):
        raise ProfileError("unsupported simulation/test backend")
    if p.get("verification_backend") not in ("apalache", "tlc"):
        raise ProfileError("unsupported verification backend")
    if type(p.get("verification_qualified")) is not bool:
        raise ProfileError("verification_qualified must be explicit")
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(p.get("apalache_version", ""))):
        raise ProfileError("invalid Apalache version")
    if p.get("temporal") and p["verification_backend"] != "tlc":
        raise ProfileError("temporal properties require a qualified TLC profile")
    tests = p.get("tests")
    if not isinstance(tests, list) or not tests:
        raise ProfileError("tests must have explicit nonempty entry points")
    for entry in tests:
        if not isinstance(entry, dict):
            raise ProfileError("test entry must be a record")
        profile.path(entry.get("file", ""))
        _names([entry.get("main")], "test main")
        _names(entry.get("expected"), "expected tests")
    if p.get("tlc_config"):
        config = _read_json(profile.path(p["tlc_config"]))
        if not isinstance(config, dict) or set(config) - {"maxHeap", "stackSize", "workers"}:
            raise ProfileError("invalid TLC configuration keys")
        for key in ("maxHeap", "stackSize"):
            pattern = r"-Xmx\d+[mMgG]" if key == "maxHeap" else r"-Xss\d+[mMgG]"
            if key in config and not re.fullmatch(pattern, str(config[key])):
                raise ProfileError("invalid TLC JVM size")
        workers = config.get("workers", 1)
        if workers != "auto" and (type(workers) is not int or workers < 1):
            raise ProfileError("invalid TLC workers")


def build_commands(profile, mode, seed):
    p = profile.data
    common = [str(profile.path(p["instance"])), "--main", p["main"]]
    commands = [["quint", "typecheck", str(profile.path(p["instance"]))]]
    if mode == "test":
        for entry in p["tests"]:
            commands.append(["quint", "test", str(profile.path(entry["file"])), "--main", entry["main"],
                             "--backend", p["backend"], "--seed", str(seed),
                             "--max-samples", str(p["test_samples"])])
    elif mode == "simulate":
        args = ["quint", "run"] + common + ["--init", p["init"], "--step", p["step"],
                "--backend", p["backend"], "--seed", str(seed), "--max-samples", str(p["max_samples"]),
                "--max-steps", str(p["max_steps"]), "--invariant", " and ".join(p["properties"])]
        if p.get("witnesses"):
            args += ["--witnesses"] + p["witnesses"]
        commands.append(args)
    elif mode == "verify":
        if not p["verification_qualified"]:
            raise ProfileError("profile verification is not qualified; complete its backend calibration first")
        args = ["quint", "verify"] + common + ["--init", p["init"], "--step", p["step"],
                "--backend", p["verification_backend"], "--apalache-version", p["apalache_version"],
                "--max-steps", str(p["max_steps"]), "--invariant", ",".join(p["properties"])]
        if p.get("temporal"):
            args += ["--temporal", ",".join(p["temporal"])]
        if p.get("tlc_config"):
            args += ["--tlc-config", str(profile.path(p["tlc_config"]))]
        commands.append(args)
    else:
        # TODO(not planned): automatic Go trace replay.
        raise ProfileError("unsupported run mode: " + mode)
    return commands


def run_command(argv, timeout, artifacts):
    prefix = Path(artifacts)
    out, err = prefix.with_suffix(".stdout.log"), prefix.with_suffix(".stderr.log")
    start = time.monotonic()
    result = CommandResult(list(argv), None, 0, str(out), str(err))
    proc = None
    try:
        with out.open("wb") as stdout, err.open("wb") as stderr:
            proc = subprocess.Popen(argv, cwd=REPO_ROOT, stdin=subprocess.DEVNULL,
                                    stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                result.timed_out = True
            except (KeyboardInterrupt, Interrupted) as exc:
                result.interrupted = getattr(exc, "signum", signal.SIGINT)
            finally:
                # Own the whole subprocess group, including verifier descendants.
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                result.returncode = proc.wait()
    except OSError as exc:
        result.error = str(exc)
    finally:
        result.elapsed_seconds = round(time.monotonic() - start, 6)
    return result


def classify(result, json_path, kind, backend, expected=()):
    if result.timed_out or result.interrupted or (result.returncode is not None and result.returncode < 0):
        return "incomplete"
    if result.error:
        return "infrastructure-error"
    try:
        data = _read_json(json_path)
    except (OSError, ValueError):
        return "infrastructure-error"
    if not isinstance(data, dict) or not isinstance(data.get("errors"), list) or data["errors"]:
        return "infrastructure-error"
    stage = {"typecheck": "typechecking", "test": "testing", "run": "running", "verify": "verifying"}[kind]
    if data.get("stage") != stage:
        return "infrastructure-error"
    if kind == "test":
        passed, failed, ignored = data.get("passed"), data.get("failed"), data.get("ignored")
        if not isinstance(passed, list) or not isinstance(failed, list) or not isinstance(ignored, list):
            return "infrastructure-error"
        if failed:
            return "test-failure"
        names = [name.split("::")[-1] for name in passed]
        if not passed or ignored or sorted(names) != sorted(expected):
            return "infrastructure-error"
        return "tests-pass" if result.returncode == 0 else "infrastructure-error"
    if result.returncode != 0:
        return "counterexample" if data.get("status") == "violation" else "infrastructure-error"
    if kind == "typecheck":
        return "typecheck-pass"
    if data.get("status") != "ok":
        return "infrastructure-error"
    if kind == "run":
        return "sampled-pass" if isinstance(data.get("trace"), list) and data["trace"] else "infrastructure-error"
    # Qualified in Task 17 with known passing/failing cases; never infer from a timeout.
    return "exhaustive-complete" if backend == "tlc" else "bounded-pass"


def write_report(manifest, results):
    directory = Path(manifest["directory"])
    manifest["commands"] = results
    temp = directory / "manifest.tmp"
    temp.write_text(json.dumps(manifest, indent=2) + "\n")
    temp.replace(directory / "manifest.json")
    lines = ["# Quint run", "", "Profile: " + manifest["profile"], "Mode: " + manifest["mode"],
             "Result: " + manifest["result"], "", manifest.get("error", ""), "",
             "| Command | Outcome | Exit | Seconds |", "| --- | --- | --- | --- |"]
    for item in results:
        lines.append("| {} | {} | {} | {} |".format(item["name"], item["outcome"],
                                                  item["returncode"], item["elapsed_seconds"]))
    (directory / "report.md").write_text("\n".join(lines) + "\n")
    if manifest.get("completed") and manifest["result"] != "incomplete":
        pending = directory.parent / (".latest-" + directory.name)
        pending.symlink_to(directory.name, target_is_directory=True)
        pending.replace(directory.parent / "latest")


def validate_backend(profile, mode):
    p = profile.data
    home = Path(os.environ.get("QUINT_HOME", str(Path.home() / ".quint")))
    info = {"quint": QUINT_VERSION, "backend": p["backend"]}
    if p["backend"] == "rust":
        binary = home / ("rust-evaluator-" + EVALUATOR_VERSION) / "quint_evaluator"
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ProfileError("pinned Rust evaluator missing; provision it explicitly: " + str(binary))
        info.update(evaluator_version=EVALUATOR_VERSION, evaluator_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    if mode == "verify":
        jar = home / ("apalache-dist-" + p["apalache_version"]) / "apalache/lib/apalache.jar"
        launcher = jar.parent.parent / "bin/apalache-mc"
        if not jar.is_file() or not launcher.is_file() or not shutil.which("java"):
            raise ProfileError("pinned verifier/JVM missing; provision explicitly before verification: " + str(jar))
        info.update(backend=p["verification_backend"], apalache_version=p["apalache_version"],
                    verifier_sha256=hashlib.sha256(jar.read_bytes()).hexdigest())
    return info


def bounds_command(profile, seed):
    pairs = ", ".join(json.dumps(k) + " -> " + str(v) for k, v in sorted(profile.data["bounds"].items()))
    p = profile.data
    return ["quint", "run", str(profile.path(p["instance"])), "--main", p["main"],
            "--init", p["init"], "--step", p["step"], "--invariant", "modelBounds == Map(" + pairs + ")",
            "--backend", p["backend"], "--seed", str(seed), "--max-samples", "1", "--max-steps", "0"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--mode", required=True, choices=("test", "simulate", "verify"))
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--reports-dir", default=os.environ.get("TEST_REPORTS_DIR", str(REPO_ROOT / "test-reports")))
    args = parser.parse_args(argv)
    report_root = Path(args.reports_dir).resolve() / "quint"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    directory = report_root / run_id
    try:
        directory.mkdir(parents=True)
    except OSError as exc:
        print("Cannot create report directory: " + str(exc), file=sys.stderr)
        return 2
    manifest = {"schema_version": 1, "directory": str(directory), "profile": args.profile,
                "mode": args.mode, "seed": args.seed, "result": "incomplete", "completed": False}
    results = []
    exit_code = 2
    handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            handlers[signum] = signal.signal(signum, lambda sig, frame: (_ for _ in ()).throw(Interrupted(sig)))
        if not IDENTIFIER.fullmatch(args.profile) or args.seed < 0:
            raise ProfileError("invalid profile name or negative seed")
        profile = load_profile(MODEL_ROOT / "profiles" / (args.profile + ".json"))
        validate_profile(profile, MODEL_ROOT)
        manifest["configuration"] = profile.data
        commands = build_commands(profile, args.mode, args.seed)
        manifest["model_hashes"] = {str(p.relative_to(MODEL_ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in sorted(MODEL_ROOT.rglob("*"))
                                    if p.is_file() and p.suffix in (".qnt", ".py", ".json")}
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True)
        manifest["revision"] = revision.stdout.strip()
        manifest["dirty"] = bool(subprocess.run(["git", "status", "--porcelain"], cwd=REPO_ROOT,
                                                capture_output=True, text=True, check=True).stdout)
        manifest["tools"] = validate_backend(profile, args.mode)
        version = run_command(["quint", "--version"], 10, directory / "version")
        version_data = asdict(version)
        version_data.update(name="version", outcome="infrastructure-error")
        results.append(version_data)
        if version.interrupted:
            raise Interrupted(version.interrupted)
        if version.timed_out:
            raise CommandTimeout("Quint version probe timed out")
        if version.returncode != 0:
            raise ProfileError("Quint CLI unavailable or version probe failed")
        if Path(version.stdout).read_text().strip() != QUINT_VERSION:
            raise ProfileError("installed Quint does not match " + QUINT_VERSION)
        version_data["outcome"] = "version-pass"
        commands.insert(1, bounds_command(profile, args.seed))
        manifest["witnesses"] = {"requested": profile.data["witnesses"],
                                 "counts": None, "note": "Quint 0.33 JSON omits witness counts; deterministic tests are separate evidence."}
        for index, command in enumerate(commands):
            prefix = directory / ("command-" + str(index))
            output = prefix.with_suffix(".json")
            command += ["--out", str(output)]
            kind = command[1]
            if kind in ("test", "run"):
                command += ["--out-itf", str(directory / ("trace-" + str(index) + "-{test}-{seq}.itf.json")
                                          if kind == "test" else directory / ("trace-" + str(index) + "-{seq}.itf.json"))]
            expected = []
            if kind == "test":
                expected = profile.data["tests"][index - 2]["expected"]
            result = run_command(command, profile.data["timeout_seconds"], prefix)
            outcome = classify(result, output, kind, profile.data["verification_backend"], expected)
            item = asdict(result)
            item.update(name=str(index) + ":" + kind, outcome=outcome, output=str(output))
            results.append(item)
            manifest["result"] = outcome
            write_report(manifest, results)
            if outcome not in ("typecheck-pass", "tests-pass", "sampled-pass", "bounded-pass", "exhaustive-complete"):
                if result.interrupted:
                    exit_code = 128 + result.interrupted
                elif result.timed_out:
                    exit_code = 124
                elif result.returncode is not None and result.returncode < 0:
                    exit_code = 128 - result.returncode
                else:
                    exit_code = result.returncode or 1
                break
        else:
            exit_code = 0
    except CommandTimeout as exc:
        manifest.update(result="incomplete", error=str(exc))
        exit_code = 124
    except (KeyboardInterrupt, Interrupted) as exc:
        manifest.update(result="incomplete", error="interrupted")
        exit_code = 128 + getattr(exc, "signum", signal.SIGINT)
    except (ProfileError, OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        manifest.update(result="infrastructure-error", error=str(exc))
    finally:
        for signum, prior in handlers.items():
            signal.signal(signum, prior)
        manifest["completed"] = True
        try:
            write_report(manifest, results)
        except OSError as exc:
            print("Cannot finish report: " + str(exc), file=sys.stderr)
            exit_code = 2
        print("Quint " + manifest["result"] + ": " + str(directory / "report.md"))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
