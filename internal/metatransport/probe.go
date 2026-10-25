package metatransport

import (
	"context"
	"crypto/tls"
	"encoding/hex"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// ProbeOptions selects only client networking; probes own no listener or server.
type ProbeOptions struct {
	Header         *metapb.Header
	Address        string
	ClientTLS      *tls.Config
	AllowPlaintext bool
	Credential     []byte
}

// ProbeIdentity invokes the existing nil-payload IdentityPing RPC without a Node.
// A structured reply passed server credential/cluster checks; even Unavailable
// proves an old-cluster endpoint answered. Explicit binding rejection is separate.
func ProbeIdentity(ctx context.Context, options ProbeOptions) (*metapb.Reply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Header == nil || options.Header.ClusterId == "" || options.Header.NodeName == "" ||
		options.Address == "" || len(options.Credential) == 0 || options.ClientTLS == nil && !options.AllowPlaintext {
		return nil, fault.ErrInvalid
	}
	connection, err := controlConnection(options.Address, options.ClientTLS)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	// Nil-payload IdentityPing must remain side-effect-free: the synthetic
	// inspection header must never teach routes or establish a process session.
	authenticated := metadata.AppendToOutgoingContext(ctx, credentialKey, hex.EncodeToString(options.Credential))
	return metapb.NewMetaClient(connection).IdentityPing(authenticated,
		&metapb.MetaRequest{Header: proto.CloneOf(options.Header)})
}
