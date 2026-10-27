# Quint cluster-management execution progress

Plan: `docs/plans/20261027-quint-cluster-management.md`
Target: `docs/brainstorms/20261027-quint-cluster-management-target.md`
Branch: `feat/quint-spec`

## Task 1 — Executable typed Choreo foundation
Date: 2026-10-27

Implemented the shared state vocabulary, exact identity/proof distinctions, one application-owned Choreo GlobalContext, explicit request queue/delivery/handling and a concrete one-node foundation. Vendored unchanged Choreo/basicSpells and the Apache license from Quint commit bd327e32df01a578a0f7270ed7e80d0e4b0b0c6e, with hashes. Coverage enumerates 82 public API entries and marks unimplemented scope explicitly.

Verified:
- Quint 0.33.0 typecheck of instances/foundation.qnt.
- `quint test ... --main foundation --seed 1 --max-samples 10`: four passing tests (one assumption test and ten executions each of observation, invalid-refusal and request-bound scenarios).
- `quint run ... --invariant safety --witnesses deliveredRequest --seed 1 --max-samples 50 --max-steps 20`: no sampled violation, reply witnessed in all 50 traces.
- Vendor SHA-256/license/import provenance and document links checked; staged whitespace validation before commit.

Implementation detail: Quint's `action` keyword cannot name a record field, so the schema uses `kind`. Explicit re-export of cluster actions is required through the test wrapper. We use Choreo's pure cue/context/effect functions; its unused native state/display variables are not part of the model. No behavior beyond the documented foundation is claimed complete. No Go tests or formal verification were run for this model-only checkpoint.

## Task 2 — Profile runner and honest manifests
Date: 2026-10-27 (Task 1 commit: cdc21f8)

Implemented a Python 3.9-compatible profile runner, pinned foundation JSON, explicit instance-bound checks, test-name coverage checks, bounded child process groups, raw exit/log/trace preservation, source/vendor hashes, and separate sampled/bounded/exhaustive/incomplete classifications. Verification is gated on backend qualification; its first real execution remains Task 17. Missing pinned dependencies refuse without installation. Default profile uses the bundled TypeScript evaluator.

Verified:
- 17 runner unit/contract tests pass, including real Quint detection of manifest-bound drift, wrong CLI version, zero/missing tests, malformed profiles, absent executable/backend, subprocess nonzero exit, timeout, signal and latest-report publication rules.
- `python3 -m py_compile scripts/quint/run.py` and whitespace validation pass.
- Real foundation test profile: all four deterministic tests pass; real simulation profile: no sampled safety violation, 50 samples/depth 20/seed 1. Reports: test-reports/quint/20261027T115342384324Z-2c7c87e7 and 20261027T115346605296Z-b32bcea1.

Evidence changed the bounds-check implementation: importing/re-exporting an already concretely instantiated module exposed unbound consts in Quint. A zero-step invariant run against the original concrete instance verifies modelBounds without a duplicate instantiation. The failed prototype is removed. Quint's JSON run format omits witness counts; reports explicitly preserve that limitation rather than fabricate coverage. All handlers/protocol scope beyond Task 1 remain pending.
