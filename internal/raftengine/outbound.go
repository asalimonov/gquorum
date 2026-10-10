package raftengine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const outboundPeerQueueCapacity = 64

type outboundPeer struct {
	queue  chan []raftpb.Message
	cancel context.CancelFunc
	// Includes the in-flight batch, so pruning cannot cancel a final removal
	// commit before its bounded transport attempt has finished.
	pending   atomic.Int64
	wasMember bool // sender-owned
	retired   atomic.Bool
}

func (g *Group) sender() {
	defer g.wg.Done()
	peers := map[uint64]*outboundPeer{}
	var workers sync.WaitGroup
	defer func() {
		for _, peer := range peers {
			peer.cancel()
		}
		workers.Wait()
	}()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			cs := g.Status().ConfState
			active := make(map[uint64]bool)
			for _, ids := range [][]uint64{cs.Voters, cs.Learners, cs.VotersOutgoing, cs.LearnersNext} {
				for _, id := range ids {
					active[id] = true
				}
			}
			for id, peer := range peers {
				if active[id] {
					peer.wasMember = true
					peer.retired.Store(false)
				} else if peer.wasMember || len(active) > 0 {
					peer.retired.Store(true)
				}
				if !active[id] && peer.pending.Load() == 0 {
					peer.cancel()
					delete(peers, id)
				}
			}
		case messages := <-g.sendC:
			batches := map[uint64][]raftpb.Message{}
			var order []uint64
			for _, message := range messages {
				if _, found := batches[message.To]; !found {
					order = append(order, message.To)
				}
				batches[message.To] = append(batches[message.To], message)
			}
			for _, id := range order {
				peer := peers[id]
				if peer == nil {
					ctx, cancel := context.WithCancel(g.ctx)
					peer = &outboundPeer{queue: make(chan []raftpb.Message, outboundPeerQueueCapacity), cancel: cancel}
					cs := g.Status().ConfState
					for _, ids := range [][]uint64{cs.Voters, cs.Learners, cs.VotersOutgoing, cs.LearnersNext} {
						for _, configured := range ids {
							if configured == id {
								peer.wasMember = true
							}
						}
					}
					// Joining replicas can reply before receiving any configuration. Once
					// membership exists, an unconfigured destination only needs the final
					// committed removal, including when an earlier idle worker was pruned.
					if !peer.wasMember && len(cs.Voters)+len(cs.Learners)+len(cs.VotersOutgoing)+len(cs.LearnersNext) > 0 {
						peer.retired.Store(true)
					}
					peers[id] = peer
					g.sendWorkers.Add(1)
					workers.Go(func() {
						defer g.sendWorkers.Add(-1)
						g.sendPeer(ctx, peer)
					})
				}
				peer.pending.Add(1)
				select {
				case peer.queue <- batches[id]:
				default:
					peer.pending.Add(-1)
					g.reportUndelivered(batches[id])
				}
			}
		}
	}
}

func (g *Group) sendPeer(ctx context.Context, peer *outboundPeer) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case messages := <-peer.queue:
			if peer.retired.Load() {
				retained := make([]raftpb.Message, 0, len(messages))
				for _, message := range messages {
					if committedRemoval(message) {
						retained = append(retained, message)
					} else {
						g.reportUndelivered([]raftpb.Message{message})
					}
				}
				messages = retained
			}
			if len(messages) == 0 {
				peer.pending.Add(-1)
				continue
			}
			sendCtx, cancel := context.WithTimeout(ctx, g.opts.Timing.TickInterval*time.Duration(g.opts.Timing.ElectionTicks)/2)
			err := g.opts.Transport.Send(sendCtx, messages)
			cancel()
			if err != nil {
				g.reportUndelivered(messages)
			}
			peer.pending.Add(-1)
		}
	}
}

func (g *Group) reportUndelivered(messages []raftpb.Message) {
	for _, message := range messages {
		g.ReportUnreachable(message.To)
		if message.Type == raftpb.MsgSnap {
			g.ReportSnapshot(message.To, raft.SnapshotFailure)
		}
	}
}

// Keep the exact final removal append, while retiring buffered heartbeats and
// old replication work. A never-admitted joiner's replies are not retired.
func committedRemoval(message raftpb.Message) bool {
	if message.Type != raftpb.MsgApp {
		return false
	}
	for _, entry := range message.Entries {
		if entry.Type != raftpb.EntryConfChangeV2 || message.Commit < entry.Index {
			continue
		}
		var change raftpb.ConfChangeV2
		if change.Unmarshal(entry.Data) != nil {
			continue
		}
		for _, member := range change.Changes {
			if member.Type == raftpb.ConfChangeRemoveNode && member.NodeID == message.To {
				return true
			}
		}
	}
	return false
}
