package cache

// Count is the number of affected keys returned by a write.
type Count struct {
	N       int `json:"n"`
	Evicted int `json:"evicted,omitempty"`
}
