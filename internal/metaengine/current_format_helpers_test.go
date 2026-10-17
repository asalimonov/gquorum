package metaengine

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func currentFormatBytes(t *testing.T, name string) []byte {
	t.Helper()
	want := map[string]string{
		"snapshot.json":           "be91b10e7a18c306bba1ac5230777df36b8b0f325e7d8ced91bd558de07f43ac",
		"collection-command.json": "100c0f7f62c166d9d5a13a954165c7e0d45114604b78d1ed6f8e490a492c917a",
		"collection-result.json":  "f4b5cc5b846699a2e2d52dd64bac3b88435f9f848af896154385ad39ca48ce6f",
		"meta.bolt":               "dc26a383bf55f803879b43e54616e069b62f0c950e4ed311c2df0082d855281b",
	}[name]
	data, err := os.ReadFile(filepath.Join("testdata", "scoped-v4", name))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != want {
		t.Fatal("frozen schema-4 fixture changed; inspect provenance rather than regenerating", name)
	}
	return data
}
