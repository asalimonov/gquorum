# Meta format before the public API rename

Generated with the unchanged meta engine at `9c8d138` (the same codec/FSM source
as baseline `75ba513`). Do not regenerate these files using the refactored codec.

- `proposals.json`: original serialized envelopes for every command kind, with
  identity/authorization/audit fields populated.
- `apply-result.json`: an original successful approval result, including the
  historically capitalized result keys and nested admission record.
- `snapshot.json`: an actual FSM snapshot after founding a three-node topology,
  approving and activating it, binding an admission, granting a session and
  acquiring its node lock. Applied index 7, term 2, schema 1.

Tests require exact round trips as well as restored documents, operator,
admission and lock state. Names in fixture bytes are storage/protocol contracts,
not deprecated public API aliases.
