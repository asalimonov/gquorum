package metatransport

import "go.etcd.io/raft/v3/raftpb"

// Drain only work already queued, without delaying the first message or
// allowing concurrent producers to grow the batch indefinitely. A round trip
// per message otherwise saturates a peer once RTT exceeds the heartbeat tick.
func drainPeerMessages(first raftpb.Message, queue <-chan raftpb.Message) []raftpb.Message {
	messages := make([]raftpb.Message, 1, 1+len(queue))
	messages[0] = first
	for len(messages) < cap(messages) {
		select {
		case message := <-queue:
			messages = append(messages, message)
		default:
			return messages
		}
	}
	return messages
}

// Snapshots retain their separate chunked protocol and acknowledgement. Keep
// their position between ordinary frames; report only the failed suffix so an
// acknowledged snapshot is never subsequently reported as failed.
func (t *Transport) sendPeerMessages(
	p *peer, connection *peerConnection, messages []raftpb.Message,
) ([]raftpb.Message, error) {
	remaining := messages
	for len(remaining) > 0 {
		end := 1
		if remaining[0].Type != raftpb.MsgSnap {
			for end < len(remaining) && remaining[end].Type != raftpb.MsgSnap {
				end++
			}
		}
		batch := remaining[:end]
		if err := t.sendPeerBatch(p, connection, batch); err != nil {
			return remaining, err
		}
		for _, message := range batch {
			t.finishSnapshot(message)
		}
		remaining = remaining[end:]
	}
	return nil, nil
}

func (t *Transport) failMessages(messages []raftpb.Message) {
	for _, message := range messages {
		t.fail(message)
	}
}
