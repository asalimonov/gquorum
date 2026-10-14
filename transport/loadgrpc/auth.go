package loadgrpc

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	maximumTokenBytes   = 4096
	firstTokenCharacter = 33
	lastTokenCharacter  = 126
	bearerPrefix        = "Bearer "
)

// SharedToken authenticates possession of a shared cluster secret, not a unique
// node identity. Use it only among mutually trusted participants; use a custom
// Options.Authenticate callback to bind individual certificates to identities.
// It implements credentials.PerRPCCredentials for outbound calls and provides
// Authenticate for inbound load RPCs. Never log its metadata.
type SharedToken struct {
	token      string
	digest     [32]byte
	requireTLS bool
}

var _ credentials.PerRPCCredentials = (*SharedToken)(nil)

// NewSharedToken copies a printable ASCII token (1..4096 bytes). requireTLS
// prevents sending it on plaintext client connections. The caller must also
// configure server TLS; false explicitly allows a plaintext demo deployment.
func NewSharedToken(token string, requireTLS bool) (*SharedToken, error) {
	if len(token) == 0 || len(token) > maximumTokenBytes {
		return nil, fault.New(fault.Invalid, "invalid load credential length", nil)
	}
	for _, b := range []byte(token) {
		if b < firstTokenCharacter || b > lastTokenCharacter {
			return nil, fault.New(fault.Invalid, "load credential must be printable ASCII without spaces", nil)
		}
	}
	return &SharedToken{token: token, digest: sha256.Sum256([]byte(token)), requireTLS: requireTLS}, nil
}

// GetRequestMetadata implements per-call outbound authentication.
func (s *SharedToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": bearerPrefix + s.token}, nil
}

// RequireTransportSecurity reports the explicit constructor setting.
func (s *SharedToken) RequireTransportSecurity() bool { return s.requireTLS }

// Authenticate verifies exactly one bearer header in constant time after
// hashing. The supplied sender is deliberately not treated as proven identity.
func (s *SharedToken) Authenticate(ctx context.Context, _ topology.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || len(values[0]) > maximumTokenBytes+len(bearerPrefix) ||
		!strings.HasPrefix(values[0], bearerPrefix) {
		return fault.ErrIdentityMismatch
	}
	digest := sha256.Sum256([]byte(strings.TrimPrefix(values[0], bearerPrefix)))
	if subtle.ConstantTimeCompare(digest[:], s.digest[:]) != 1 {
		return fault.ErrIdentityMismatch
	}
	return nil
}
