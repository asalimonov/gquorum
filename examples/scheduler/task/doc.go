// Package task defines the scheduler's replicated business model. Commands are
// deterministic: randomness, clocks and execution happen outside Transition.
// Read helpers go through the public load.Group quorum API, never local SQL.
package task
