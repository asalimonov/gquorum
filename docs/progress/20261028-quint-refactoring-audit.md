# Quint refactoring architectural delta

Baseline: `85cd75d`; mechanical extraction: `c5ece71`. This audit covers the helper checkpoint and uses the [action ledger](20261028-quint-action-map.json), [module map](20261028-quint-module-map.json) and [helper ledger](20261028-quint-helper-map.json). Peer review completed after this audit; see the outcome below.

## Ownership and dependencies

The import audit checked all 62 non-vendor modules: every import resolves, the graph is acyclic, production never imports tests, fixtures contain no `var` or `run`, and dependencies satisfy the plan's family table. `cluster.qnt` is the sole mutable-state declaration. Scenario suites retain the linear import chain and one instance-level binding; seven distinct stage-sharing checks connect their effects. Pure fixtures are independent of scenario state.

`types.qnt` is byte-identical to the baseline. Initialization, properties and original rule bodies were relocated with import changes; World/Context/Transition/HostEffect aliases moved to `runtime/world.qnt`. No function is stored in World. Helpers receive callbacks only while computing a post-state. Domain code retains guards and confirmed/local evidence; framing introduces no authoritative read.

## Actions and framing

All 95 baseline actions remain in the facade. Seventy-five delegate to explicit Enabled/Next functions; the other 20 retain selection, initialization or delegation. Mechanical audit at `c5ece71` checks all 95 with zero normalized differences. The four internal-choice actions retain their choice in the facade; selectors and choice domains are unchanged.

The subsequent helper ledger records 90 changed pure definitions. Changes frame the same update or remove bindings unused in that guard/post half. Actual step-module usage is 10 withNode, 42 withProcess and 10 withOwner calls. The facade adds guarded, whose false branch cannot stutter and does not evaluate the post callback. A missing-owner callback test checks that evaluation boundary. Three pure frame checks preserve unrelated nodes, durable/volatile state, messages, extensions and events.

Reviewed sensitive updates include reserveOwner (which must retain all changes in the accepted node), releaseOwner (which updates starts as well as owners), receiveGrant (which combines frame update and Consume), session renewal/revoke (which retains exact messages and gate state), and host fencing (which updates durable and volatile evidence together). Queue/Consume continue to use the original canonical helper and explicit delivery IDs.

## Protocol boundaries and wiring

Request, read, captured CAS, submit, commit, local apply and reply remain separate actions. Authority dispatch, confirmed receive, consume and report remain separate. Factory/replay/authorization/publication and stop/join/Close/release retain their phases. Session renewal, expiry, probes and revocation remain independently selectable. No scenario script is used as the production step relation.

Every extracted step module is imported by cluster and each extracted definition has its facade consumer in the action ledger. Frame helpers have concrete callers. Existing scope exclusions and property selections remain intact. There are no replacement forwarding leaf modules or inactive duplicate model; the stable facade and instance paths remain compatibility entry points.

## Validation and limits

All four profile control objects (excluding explicit test inventories) match the baseline, including bounds, initialization, step, properties, samples and backend. Every baseline test name remains selected. Current counts: foundation 10; smoke3 50 + core 94; authority3 65 + core 94; lifetime3 85 + core 94. Suites overlap and must not be added into a unique-test total.

The execution log records final tests and smoke3/lifetime3 sampled simulations. Twelve temporary differential checks compare old/new lifecycle guards and full next-world values across 50 constructed owner/Close/join combinations; all pass. These input states are frame-comparison fixtures, not a claim that every combination is reachable. These 12 cases cover a lifecycle subset of the 90 changed definitions; node/process conversions rely on source review and suites. The successful log and source are available locally only (gitignored; absent from a clean checkout) under `test-reports/quint/refactoring-evidence`; the probe uses a scratch checkout of c5ece71 with renamed baseline modules to prevent import shadowing.

No Go, vendor, runner, dependency, state-schema or exploration-bound change is part of this refactor. This evidence combines mechanical comparison, source review, regression tests and sampled simulation; it is not an exhaustive equivalence proof or new verification-backend qualification. No faithful library scenario failure was found, so there is no library-defect report to add. Parent protocol Tasks 8–18 remain paused.

## Peer review outcome

Agent e77ec353-b3eb-4158-9cb2-c3a160ddd0c2 reviewed 85cd75d..5ea256c using the requested paseo-peer workflow (local branch file, thread 2). No semantic regression or blocking issue found. The reviewer independently checked facade argument correspondence, assigned fields/effect ordering, unused-binding removal, fixture references, inventories and suite composition. Static checks and a focused laziness probe supplement the existing runs; reachable-state equivalence, Apalache behavior and sampling-distribution preservation were not established.

Low findings were resolved in f8cd519: explicit local-only differential-evidence wording, a useful mechanical-audit default/diagnostic and consistent relative import spelling. The root sharing test is documented as the root-stage baseline. Keeping the audit tool inside the recursively hashed model tree is intentional provenance for maintenance tooling. No substantive model change required another peer round or full suite run. No unresolved critical/major finding remains.
