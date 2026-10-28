# Task 8: Quint trace-export limits

This is a tooling report, not a gquorum protocol defect. Quint 0.33.0 and its preinstalled Rust evaluator v0.7.0 remain pinned; no upstream/vendor files were changed.

## Observations

The complete `scaleThreeFourFiveFourThreeTest` executed via `quint test --backend rust --seed 1 --max-samples 1` reached the frontend's JSON-line size limit (`RangeError: Invalid string length`). The TypeScript evaluator fallback exhausted Node's default heap while converting the long trace. These runs were incomplete, not evidence of a model violation or success. Local development logs are `/tmp/quint-scaling-fixed.log` and `/tmp/quint-scaling-ts.log`; those scratch paths are not clean-checkout artifacts.

The ordinary admission profile later exhausted the default approximately 4 GiB frontend heap. The failure is retained with exact source hashes, command/exit information and stderr in `test-reports/quint/20261027T221306820333Z-ea77e1f9/`: the test child exited by SIGABRT, classified incomplete. It did not pass.

## Adopted execution path

Long named witnesses use `quint repl --backend rust --seed 1 --quiet -r <instance>.qnt::<module> <test-name>` from the instance's directory. This invokes the same model test through the pinned Rust evaluator without exporting every intermediate state. REPL can exit zero after an evaluation error, so the runner checks every nonempty output line: exactly one `true` per named witness is required. False, missing/extra output, parse/runtime errors, child failure, timeout and interruption cannot pass. Full trace export is explicitly absent; the source, seed, expression, result and raw logs remain reproducible.

Profile exclusions must be paired with REPL entries for the same instance. Ordinary tests still enumerate all remaining test definitions and require the exact passed-name inventory. Long checks are single-execution reachability/regression witnesses; they do not claim the ordinary suite's sample count or exhaustive coverage.

The large profiles explicitly set `node_heap_mb: 16384`. This only adjusts their child Node frontend heap, with the setting recorded in reports; bounds and samples are unchanged. The host has 64 GiB physical RAM. Default profiles keep the existing resource settings. Runner unit checks cover REPL true/false/error/missing output, exclusion routing and invalid profile settings, alongside the existing process/error contracts.

## Model results and limits

The complete scaling witness returned true through the Rust REPL, and the admission profile passed with the explicit resource setting in `test-reports/quint/20261027T230322746189Z-bad9e96f/`. Further profile results are recorded in the Task 8 execution log. Scenario and sampled passes remain distinct from bounded/exhaustive checking; verification-backend qualification remains pending.
