//go:build !functional

package topology

import (
	"encoding/json"
	"testing"
)

func serializedObservation(t *testing.T, health map[string]Health) map[string]any {
	t.Helper()
	doc := healthDocument()
	s := ClusterStatus{Observation: HealthObservation{Version: doc.Version, Groups: EvaluateAllGroupHealth(doc, health)}}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}
func TestClusterStatusHealthSerializationUnknown(t *testing.T) {
	t.Parallel()
	got := serializedObservation(t, nil)
	observation := got["observation"].(map[string]any)
	for _, value := range observation["groups"].([]any) {
		e := value.(map[string]any)
		h := e["observed_health"].(map[string]any)
		if e["voters"] != float64(3) || e["quorum"] != float64(2) || h["state"] != "unknown" {
			t.Fatal(e)
		}
		for _, key := range []string{"healthy_voters", "failure_tolerance", "below_min_replicas"} {
			v, exists := h[key]
			if !exists || v != nil {
				t.Fatal("unknown measurement must be explicit null", key, h)
			}
		}
	}
}
func TestClusterStatusHealthSerializationKnown(t *testing.T) {
	t.Parallel()
	got := serializedObservation(t, map[string]Health{"a": HealthDead, "b": HealthDead, "c": HealthDead})
	observation := got["observation"].(map[string]any)
	for _, value := range observation["groups"].([]any) {
		e := value.(map[string]any)
		h := e["observed_health"].(map[string]any)
		if h["state"] != "quorum_lost" || h["healthy_voters"] != float64(0) || h["failure_tolerance"] != float64(0) {
			t.Fatal("measured zero must remain numeric", e)
		}
		if e["group_id"] == MetaGroupID {
			if h["below_min_replicas"] != nil {
				t.Fatal("meta has no placement minimum", e)
			}
		} else if h["below_min_replicas"] != true {
			t.Fatal(e)
		}
	}
}
func TestClusterStatusNoHealthKnownField(t *testing.T) {
	t.Parallel()
	got := serializedObservation(t, nil)
	if _, exists := got["health_known"]; exists {
		t.Fatal("cluster health must be expressed per observation", got)
	}
}
