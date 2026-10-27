# Vendored Quint helpers

Files listed in `provenance.json` are copied without modifications from the Quint repository at the recorded commit. Its Apache-2.0 license is retained as `LICENSE`. The helpers are used with Quint 0.33.0.

No Choreo source adapter patch is necessary: the management model uses its pure context/cue/effect functions with one application-owned world variable. Network enqueue/delivery lives in `../cluster.qnt`, and inbox consumption is a custom effect in `../handlers.qnt`. Built-in direct mailbox Send/Broadcast is not used as network delivery.
