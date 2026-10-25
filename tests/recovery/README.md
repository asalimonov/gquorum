# Public recovery integration tests

This package belongs to the root module. It exercises the public recovery facade
and one real Node at a time, with isolated temporary roots, loopback transport
and joined cleanup. It uses caller-owned options and public status/lifecycle
methods rather than assertions against the root Node implementation.

Tests are grouped by contract: API construction, source inspection/approval,
archive/cancellation, network precheck, startup, application publication and
retained restart. Functional tests use the functional tag. Unit constructor tests
acquire no services or storage. Resource benchmarks are opt-in functional
benchmarks and never run in the ordinary unit suite.

Private codec, journal, startup-control, receipt-selection, engine and WAL tests
remain beside their implementations under internal packages. Input-corruption
fixtures here use private storage helpers to construct inputs; assertions cover
the public recovery workflow. Multi-process recovery remains under tests/cluster.
The replicated document/label scenario is under tests/sim.

The fixture's Node wrapper records only the options supplied by the test. Its
facade wrapper records source paths and the application hook supplied by the
caller; neither wrapper exposes implementation state or installs production test
hooks. Recreated nodes use fresh listeners and process identities.
