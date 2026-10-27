# Quint module refactoring execution

Plan: [refactoring](../plans/20261028-quint-module-refactoring.md). Protocol target: [management](../brainstorms/20261027-quint-cluster-management-target.md).
Branch: `feat/quint-spec`. Execution baseline: `85cd75d`; model baseline: `b39764c`.

## Task 1 — in progress

Applied the agreed peer-review corrections: keep staged suites, use distinct cross-stage effect checks, separate mechanical extraction from helper adoption, retain the correct LocalNode type, simplify directories and document allowed imports. The user explicitly requested the existing colleague for the commit review. Protocol Tasks 8–18 remain paused.

All 53 baseline model/config hashes match the Task 7 test manifest. Baseline inventories: foundation 7; smoke3 43 + core 91; authority3 57 + core 91; lifetime3 76 + core 91. Prior evidence is `test-reports/quint/20261027T184618336599Z-6d59ee80` (tests), `20261027T184745295488Z-daa25d54` (simulation), and `20261027T184745295788Z-7b998eaf` (foundation). No baseline behavior was changed.

### Task 1 completed

Moved 36 leaf/test modules into responsibility directories, leaving only `cluster.qnt` and the cohesive `types.qnt` at the top level. Extracted the Choreo aliases into `runtime/world.qnt` and moved the existing Queue/Consume implementation unchanged into `runtime/messages.qnt`. Initializers/properties/execution no longer depend on handlers just for World types. Extracted five pure fixture providers; no aliased fixture import points at a test module.

Retained the staged runtime suite chain and added seven distinctly named cross-stage effect/type checks. Each included stage connects to the preceding stage and core initialization; no sibling stateful instantiations were introduced. The previously verified LocalNode higher-order prototype remains applicable because the record shapes did not change. All original test names remain selected; foundation now has 8 cases and lifetime3 has 83 plus the unchanged 91 core checks. Module paths and additional cases are recorded in `20261028-quint-module-map.json`.

Validation: lifetime3 typecheck; coreChecks and historical topologyChecks typechecks; foundation tests (`test-reports/quint/20261027T195430962703Z-55af8afe`); lifetime3 + core tests (`test-reports/quint/20261027T195430962591Z-720d071b`); import-path inventory and diff whitespace check passed. Runner and vendor files are unchanged. No protocol transition body was rewritten in this task.

## Task 2 — mechanical transition extraction

Extracted 75 actions into nine cohesive step modules, retaining 20 selectors/delegating facade actions. Guard/post functions take World explicitly; candidate selection for handle/applyMeta/dispatchGrantFrom stays in the facade. finishTransfer already delegates to elect and retains its choice in the facade. All seven step selectors and the management/step composition remain token-identical. The facade retains all 95 action names and now has 813 lines versus 1,544 immediately before extraction.

`20261028-quint-action-map.json` records every action, its owner and constant dependencies. `scripts/quint/refactor_audit.py` mechanically reconstructs guard/post transforms and verifies candidates, adapters and retained bodies: 95 mappings, zero differences. It checks token-preserving extraction, not full semantic equivalence. After Task 3 it can audit this mechanical commit via `--revision`. The two relocated stateless predicates have public facade forwards. Consensus election still seeds the metadata leader's lease cache using the existing pure session rule, so the dependency table explicitly permits that existing read-only dependency.

Validation: lifetime3 typecheck; lifetime3 83 + core 91 tests (`test-reports/quint/20261027T201153399221Z-f773f23f`); smoke3 48 + core 91 (`20261027T201153399177Z-93e5f6eb`); foundation 8 (`20261027T201153399059Z-39f5bd67`); mechanical audit and whitespace checks passed. An omitted import of the existing isReseat helper was caught by typecheck and restored; no protocol logic was changed.
