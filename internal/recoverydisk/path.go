package recoverydisk

import (
	"errors"
	"os"
	"path/filepath"
)

// resolveRootPath resolves the nearest existing ancestor before any mkdir. This
// prevents an alias from creating directories inside a private recovery archive.
func resolveRootPath(path string) (string, error) {
	ancestor := path
	var missing []string
	for {
		_, err := os.Lstat(ancestor)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		missing = append(missing, filepath.Base(ancestor))
		ancestor = parent
	}
	resolved, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, missing[i])
	}
	return resolved, nil
}
