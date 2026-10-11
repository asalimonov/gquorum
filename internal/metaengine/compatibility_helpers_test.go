package metaengine

import (
	"os"
	"path/filepath"
	"testing"
)

func baselineBytes(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "api-baseline", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
