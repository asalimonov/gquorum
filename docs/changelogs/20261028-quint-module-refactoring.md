# Quint module organization

The model now groups rules and transitions by topology, metadata, service coordination, managed replicas and abstract consensus. Runtime helpers centralize repeated state updates, and stateless fixtures are separate from the scenario suites. The single-world facade retains every original action and interruption point.

The README documents where to extend the model and how to preserve suite state sharing. Migration ledgers and a mechanical-extraction audit make the moves reviewable. Existing tests remain selected; new checks cover state sharing, frame preservation and disabled callbacks. All four test profiles and two sampled simulation profiles passed at the helper checkpoint. Colleague review found no semantic regression; minor tooling and evidence-description findings were addressed.

This changes model organization, not the Go library or modeled protocol scope. It makes no exhaustive-equivalence or simulator-speed claim. Membership/scaling, full group cleanup, scheduling and the remaining parent Tasks 8–18 stay paused.
