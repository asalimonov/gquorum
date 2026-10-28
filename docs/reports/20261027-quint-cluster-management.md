# Quint cluster-management findings

Plan: [cluster-management model](../plans/20261027-quint-cluster-management.md). Design: [target architecture](../brainstorms/20261027-quint-cluster-management-target.md).

## Status

QCM-001 reproduces a blocked automatic admission continuation in the source-grounded Quint model. Go reproduction is pending; no production defect is claimed as Go-confirmed. Implementation and validation are ongoing.

## Reporting rule

Record a finding here when an implemented gquorum scenario is fully represented within the agreed abstraction and violates a required property. Establish source-to-model correspondence for every transition relevant to the failure, including guards, commit/apply boundaries, identities and effect evidence. Check that the failure does not depend on an omitted mechanism, a harness bound, an incorrect fixture or an expected availability blocker.

Preserve the failing property and reproducible counterexample. Do not weaken the model or relabel the result as a successful safety check. A source-faithful model counterexample is evidence of suspect library logic; Go reproduction, when available, supplies separate implementation-conformance evidence. Production library changes remain outside the model implementation plan unless separately authorized.

For each finding include:

- Stable finding ID, affected scenario and impact.
- Expected behavior and violated property, with observed behavior and the shortest useful action sequence.
- Corresponding Go paths/functions and source revision; evidence that the relevant implementation is faithfully modeled.
- Model revision or file hashes, profile, constants/bounds, assumptions, tool/backend versions, exact command and seed.
- Retained trace/report paths and reproducibility result.
- Go regression recipe or reproduction result, proposed correction and unresolved questions.
- Status: model counterexample confirmed, Go reproduction confirmed, correction pending, or resolved with evidence.

## Findings

- [QCM-001: prepared admission restart before Ready](20261028-quint-admission-prepared-restart.md): model reproduction retained, Go regression pending.
