package disk

import (
	"context"
	"os"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// RecoveryNames returns only backup names bound to validated system journals.
func (l *Lease) RecoveryNames() ([]string, error) {
	if err := l.CheckRoot(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.history))
	for name := range l.history {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// OpenRecoveryRoot opens a separate descriptor under the retained ownership lock.
// The caller closes this descriptor; the lease continues to own exclusivity.
func (l *Lease) OpenRecoveryRoot(name string) (*os.Root, error) {
	names, err := l.RecoveryNames()
	if err != nil {
		return nil, err
	}
	if !slices.Contains(names, name) {
		return nil, fault.ErrOperationAuthorization
	}
	return os.OpenRoot(l.path)
}

// EnsureRecoveryDirectory creates and synchronizes an owned private directory.
func EnsureRecoveryDirectory(root *os.Root, relative string) error {
	return mkdirAllDurable(root, relative)
}

// ValidateRecoveryArtifact rejects aliases, links and nested mounts before I/O.
func (l *Lease) ValidateRecoveryArtifact(ctx context.Context, relative string, maxNames uint64) error {
	owner := &Owner{system: l}
	return owner.ValidateArtifact(ctx, "system", relative, maxNames)
}

// RecoveryManifest returns the accepted journal captured before WAL lease transfer.
func (l *Lease) RecoveryManifest(name string) (model.LocalManifest, error) {
	if err := l.CheckRoot(); err != nil {
		return model.LocalManifest{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	value, ok := l.manifests[name]
	if !ok {
		return model.LocalManifest{}, fault.ErrOperationAuthorization
	}
	return model.CloneManifest(value), nil
}
