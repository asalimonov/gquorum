package main

import (
	"encoding/json"
	"fmt"

	"github.com/alecthomas/kong"

	"github.com/asalimonov/gquorum/topology"
)

type config struct {
	Node       string       `env:"GQUORUM_NODE" required:"true" help:"This participant's name in the peer manifest."`
	Cluster    string       `env:"GQUORUM_CLUSTER" default:"example" help:"Cluster identity."`
	Data       string       `env:"GQUORUM_DATA" default:"/data" type:"path" help:"${data}"`
	MetaListen string       `env:"GQUORUM_META_LISTEN" default:":9000" help:"Meta protocol listen address."`
	HTTPListen string       `env:"GQUORUM_HTTP_LISTEN" default:":8080" help:"HTTP API listen address."`
	Credential string       `env:"GQUORUM_CREDENTIAL" required:"true" help:"Shared meta protocol credential."`
	Peers      peerManifest `env:"GQUORUM_PEERS" required:"true" help:"Founding peers as a JSON array."`
}

type peer struct {
	Name        string            `json:"name"`
	Type        topology.NodeType `json:"type"`
	MemberID    uint64            `json:"member_id"`
	MetaAddress string            `json:"meta_address"`
}

// A struct implementing TextUnmarshaler lets Kong decode the whole JSON value
// from a flag or environment variable without splitting its commas as a slice.
type peerManifest struct {
	Members []peer
}

func (m *peerManifest) UnmarshalText(data []byte) error {
	if err := json.Unmarshal(data, &m.Members); err != nil {
		return fmt.Errorf("peer manifest: %w", err)
	}
	return nil
}

func cliHelp() kong.Vars {
	return kong.Vars{
		"data": "Durable node directory for _meta.bolt and _authority.bolt.",
	}
}
