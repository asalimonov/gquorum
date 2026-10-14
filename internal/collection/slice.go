// Package collection provides allocation helpers that preserve empty collection encodings.
package collection

// Slice reserves capacity while retaining nil for an empty collection.
func Slice[T any](capacity int) []T {
	if capacity == 0 {
		return nil
	}
	return make([]T, 0, capacity)
}
