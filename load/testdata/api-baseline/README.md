# Load storage before the public API refactor

The `work` directory was produced by a running baseline `load.Group` at
`75ba513`: node `n1`, cluster `compat-cluster`, replica 7. It includes etcd WAL,
bbolt applied-state bookkeeping, the exact identity pin and full snapshots.
Request `one=first` is in snapshot index 3; `two=tail` is committed at index 4
and must be replayed from the WAL. The machine snapshot is a JSON request map.

The test copies `work` into a temporary caller-owned root and recovers it through
`load.NewStandalone`; it checks both request identities, deduplication, native
membership and byte-for-byte preservation of the identity pin.

Do not regenerate with current code. See
[physical compatibility provenance](../../../docs/testing/api-baseline-physical.md).
