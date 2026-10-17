# Frozen authority journal schema 2 fixture

Captured deliberately on 2026-10-17 during Task 78 on `feat/failure-isolation-and-api`
(base commit `bae0fb2`, with the schema cutover and OperationID changes applied).
`capture_store.go.txt` preserves the one-time fresh-journal capture source; it is
inert text, not an automatic update mode.

The journal pin is `cluster/n1/1`. Exact retirement effects for work replicas 7
and 8 use operation `exec-v2-a1`, authority revisions 10 and 11, and durable host
sequences 1 and 2. The collection bucket has a prepared row for replica 7 and a
collected row for replica 8 (first completion at Unix time 20). Its sequence is
2. Grant and action fields use `operation_id`. The current completed host fence
is revision 11; revision 10 retirement proof remains independently retrievable.

The functional test verifies the frozen hash, opens a temporary copy with a new
process instance, checks both collection milestones and retirement proofs,
asserts reads did not rewrite any bytes, and repeats the completed receipt
without replacing its original timestamp. Never regenerate this file merely to
make a failing test pass.

| Artifact | SHA-256 |
| --- | --- |
| `authority.bolt` | `0a638c5e52984a8e8eab09faa6df8be91c54a4564bf1d3785ed5705361b88ce0` |
