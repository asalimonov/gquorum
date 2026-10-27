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
