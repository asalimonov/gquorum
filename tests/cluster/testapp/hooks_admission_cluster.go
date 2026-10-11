package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Simulate a crash between durable identity binding and engine startup. This
// protocol driver exists only in the fixture image. A replacement container
// subsequently runs the ordinary public Node join path against the retained pin.
func (c config) prepareAdmissionPin() error {
	if c.AdmissionPinPhase != "pending" && c.AdmissionPinPhase != "ready" {
		return fault.ErrInvalid
	}
	if !c.Join || len(c.Peers) == 0 {
		return fault.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "gquorum-cluster-credential", hex.EncodeToString([]byte(c.Credential)))
	identity := topology.Identity{NodeName: c.NodeName, ClusterID: c.ClusterID, InstanceID: c.InstanceID}
	header := &metapb.Header{ClusterId: c.ClusterID, NodeName: c.NodeName, InstanceId: c.InstanceID}
	data, err := json.Marshal(map[string]any{"identity": identity, "node": c.nodeForPeer(peer{Name: c.NodeName, Type: c.NodeType, MetaAddress: c.MetaAddress, LoadAddress: c.LoadAddress, HTTPAddress: c.HTTPAddress}), "address": c.MetaAddress})
	if err != nil {
		return err
	}
	address := c.Peers[0].MetaAddress
	for {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		rpc := metapb.NewMetaClient(conn)
		reply, err := rpc.Join(ctx, &metapb.MetaRequest{Header: header, Data: data})
		if err != nil {
			conn.Close()
			return err
		}
		if err := metatransport.ReplyError(reply); err != nil {
			conn.Close()
			if !errors.Is(err, fault.ErrNotLeader) || reply.LeaderAddress == "" {
				return err
			}
			address = reply.LeaderAddress
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		err = func() error {
			defer conn.Close()
			var grant struct {
				MemberID  uint64 `json:"member_id"`
				Admission struct {
					OperatorID topology.OperatorID `json:"execution_id"`
				} `json:"admission"`
			}
			if err := json.Unmarshal(reply.Data, &grant); err != nil {
				return err
			}
			if grant.MemberID == 0 || grant.Admission.OperatorID == "" {
				return fmt.Errorf("join returned no exact pending grant")
			}
			store, err := metastore.Open(filepath.Join(c.DataDir, "meta.bolt"))
			if err != nil {
				return err
			}
			err = errors.Join(store.EnsureIdentity(c.NodeName, c.ClusterID, grant.MemberID), store.Close())
			if err != nil {
				return err
			}
			if c.AdmissionPinPhase == "ready" {
				header.MemberId = grant.MemberID
				ready, _ := json.Marshal(map[string]any{"NodeName": c.NodeName, "InstanceID": c.InstanceID, "Address": c.MetaAddress, "MemberID": grant.MemberID, "ExecutionID": grant.Admission.OperatorID})
				reply, err := rpc.JoinReady(ctx, &metapb.MetaRequest{Header: header, Data: ready})
				if err != nil {
					return err
				}
				return metatransport.ReplyError(reply)
			}
			return nil
		}()
		return err
	}
}
