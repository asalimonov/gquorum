# Write receipts and safe retries

`Group.Propose` and `Leadership.Propose` now return `(load.Submission, error)`.
This is a breaking change, including for Standalone groups. Supply a stable,
nonempty RequestID and read business results from `submission.Result` when
`ResultAvailable` is true. Applications still own deduplication.

Receipts distinguish applied writes, proven non-submission and unknown outcomes.
Cancellation can leave an unknown outcome; inspect the receipt alongside errors.
Stopped replicas and expired leadership handles are terminal.

`load.ProposeRetry` now takes `load.ProposalRetryOptions`. It repeats only proven
transient refusals, with three attempts by default, and stops on uncertainty.
Supply an overall caller deadline. `Submission.CanResubmit(err)` exposes the same
write-specific policy; an error code alone cannot establish safe repetition.

Remote outcome resolution and configured write-result size limits remain separate
work.
