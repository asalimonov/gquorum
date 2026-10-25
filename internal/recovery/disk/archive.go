package disk

import "context"

// ValidateNamespace checks system control records; WAL archive names additionally
// require the matching system journal proof in CheckStartup. It moves no data.
func ValidateNamespace(ctx context.Context, root string) error {
	_, err := scanManifests(ctx, root)
	return err
}
