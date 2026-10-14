package lifecycle

// Options selects local lifecycle callbacks. Custom Hooks and nonzero Default
// settings are mutually exclusive. Node serializes and owns these callbacks.
type Options struct {
	Hooks   Hooks
	Default DefaultOptions
}
