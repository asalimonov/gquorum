package loadgrpc

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/protobuf/proto"
)

type snapshotAckPhase uint8

const (
	snapshotAckOpening snapshotAckPhase = iota
	snapshotAckStreaming
)

func (s *snapshotClientState) observe(reply *loadpb.SnapshotAck, phase snapshotAckPhase) (snapshot.Ack, error) {
	if reply == nil || reply.DeliveryId != s.send.Manifest.Key.DeliveryID {
		return snapshot.Ack{}, snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if proto.Size(reply) > s.t.options.MaxMessageBytes {
		return snapshot.Ack{}, snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	ack, err := decodeSnapshotAck(reply)
	if err != nil {
		return ack, err
	}
	ack.Session.Key = snapshot.Key{From: s.send.Manifest.Key.From, To: s.send.Manifest.Key.To,
		DeliveryID: ack.Session.Key.DeliveryID}
	s.mu.Lock()
	now := time.Now()
	if err := s.timeoutLocked(now); err != nil {
		s.mu.Unlock()
		return ack, err
	}
	err = s.validateAckLocked(ack, phase)
	if err != nil {
		s.mu.Unlock()
		return ack, err
	}
	first := !s.admitted
	advanced := ack.NextOffset > s.ack.NextOffset
	terminal := ack.State == snapshot.DeliveryAccepted || ack.State == snapshot.DeliveryNotNeeded
	s.receiverRun = ack.ReceiverRunID
	if ack.State != snapshot.DeliveryNotNeeded {
		s.chunk = ack.ChunkBytes
	}
	s.admitted = true
	if advanced {
		s.lastProgress = now
	}
	s.deadline = minSnapshotTime(s.deadline, now.Add(ack.RemainingTotal))
	s.progressTimeout = min(s.progressTimeout, ack.ProgressTimeout)
	ack.RemainingTotal = max(time.Duration(0), s.deadline.Sub(now))
	ack.ProgressTimeout = s.progressTimeout
	s.ack = ack
	s.mu.Unlock()
	signalSnapshotClient(s.clockWake)
	signalSnapshotClient(s.wake)
	if (first || advanced || terminal) && s.send.ReportProgress != nil {
		s.send.ReportProgress(snapshot.Progress{ReceiverRunID: ack.ReceiverRunID,
			VerifiedBytes: ack.NextOffset, State: ack.State,
			RemainingTotal: ack.RemainingTotal, ProgressTimeout: ack.ProgressTimeout})
	}
	return ack, nil
}
func (s *snapshotClientState) validateAckLocked(ack snapshot.Ack, phase snapshotAckPhase) error {
	if ack.Session.Key != s.send.Manifest.Key || ack.NextOffset > s.send.Manifest.TotalBytes ||
		ack.NextOffset < s.ack.NextOffset {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if s.receiverRun != "" && s.receiverRun != ack.ReceiverRunID {
		return snapshotWireError(snapshot.ReasonRestartRequired, nil)
	}
	if ack.State == snapshot.DeliveryNotNeeded {
		if phase != snapshotAckOpening || ack.NextOffset != 0 {
			return snapshotWireError(snapshot.ReasonMalformed, nil)
		}
		return nil
	}
	if err := s.validateChunkBounds(ack); err != nil {
		return err
	}
	if err := s.validateAckSession(ack, phase); err != nil {
		return err
	}
	return s.validateAckPosition(ack, phase)
}
func (s *snapshotClientState) validateAckPosition(ack snapshot.Ack, phase snapshotAckPhase) error {
	limit := s.highestSent
	if phase == snapshotAckStreaming {
		limit = s.attemptSent
	}
	if ack.NextOffset > limit ||
		(ack.NextOffset < s.send.Manifest.TotalBytes && ack.NextOffset%uint64(ack.ChunkBytes) != 0) {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	if ack.State == snapshot.DeliveryAccepted && (ack.NextOffset != s.send.Manifest.TotalBytes || !s.finishIssued) {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	return nil
}
func (s *snapshotClientState) validateChunkBounds(ack snapshot.Ack) error {
	if s.options.ChunkBytes <= 0 || ack.ChunkBytes > s.send.Manifest.ChunkBytes ||
		uint64(ack.ChunkBytes) > uint64(s.options.ChunkBytes) {
		return snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	capacity, err := snapshotChunkCapacity(s.t.options.MaxMessageBytes, int(ack.ChunkBytes))
	if err != nil || capacity <= 0 {
		return snapshotWireError(snapshot.ReasonFrameLimit, err)
	}
	if uint64(capacity) < uint64(ack.ChunkBytes) {
		return snapshotWireError(snapshot.ReasonFrameLimit, nil)
	}
	if s.chunk != 0 && s.chunk != ack.ChunkBytes {
		return snapshotWireError(snapshot.ReasonTransferConflict, nil)
	}
	return nil
}
func (s *snapshotClientState) validateAckSession(ack snapshot.Ack, phase snapshotAckPhase) error {
	if phase == snapshotAckStreaming && ack.Session != s.ack.Session {
		return snapshotWireError(snapshot.ReasonRestartRequired, nil)
	}
	if phase == snapshotAckOpening && s.admitted && ack.State != snapshot.DeliveryAccepted &&
		ack.Session.Attachment <= s.ack.Session.Attachment {
		return snapshotWireError(snapshot.ReasonRestartRequired, nil)
	}
	switch ack.State {
	case snapshot.DeliveryReceiving, snapshot.DeliveryVerifying, snapshot.DeliveryHandingOff, snapshot.DeliveryAccepted:
		return nil
	default:
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
}
func (s *snapshotClientState) sendChunks(ctx context.Context) error {
	if s.options.WindowChunks <= 0 {
		return snapshotWireError(snapshot.ReasonMalformed, nil)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		sent, verified := s.attemptSent, s.ack.NextOffset
		total, chunk := s.send.Manifest.TotalBytes, uint64(s.chunk)
		if sent == total && verified == total {
			s.finishIssued = true
			session := s.ack.Session
			s.mu.Unlock()
			return s.sendFinish(session)
		}
		if sent == total || (sent-verified)/chunk >= uint64(s.options.WindowChunks) {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.wake:
			}
			continue
		}
		n := min(chunk, total-sent)
		s.attemptSent = sent + n
		s.highestSent = max(s.highestSent, s.attemptSent)
		session := s.ack.Session
		s.mu.Unlock()
		data := s.send.Data[sent : sent+n]
		sum := sha256.Sum256(data)
		frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Data{Data: &loadpb.SnapshotData{
			Attachment: session.Attachment,
			Ordinal:    sent / chunk, Offset: sent, Data: data, Sha256: sum[:]}}}
		if err := s.stream.Send(frame); err != nil {
			return err
		}
	}
}
func (s *snapshotClientState) sendFinish(session snapshot.Session) error {
	frame := &loadpb.SnapshotFrame{Body: &loadpb.SnapshotFrame_Finish{
		Finish: &loadpb.SnapshotFinish{Attachment: session.Attachment}}}
	if err := s.stream.Send(frame); err != nil {
		return err
	}
	return s.stream.CloseSend()
}
func signalSnapshotClient(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (s *snapshotClientState) monitor(ctx context.Context, cancel context.CancelCauseFunc) {
	timer := time.NewTimer(s.options.ProgressTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.clockWake:
		case <-timer.C:
		}
		s.mu.Lock()
		due, reason := s.nextDeadlineLocked()
		s.mu.Unlock()
		if !time.Now().Before(due) {
			cancel(snapshotWireError(reason, nil))
			return
		}
		timer.Reset(time.Until(due))
	}
}

func (s *snapshotClientState) nextDeadlineLocked() (time.Time, string) {
	due, reason := s.deadline, snapshot.ReasonTotalTimeout
	if s.ack.NextOffset < s.send.Manifest.TotalBytes {
		progressDue := s.lastProgress.Add(s.progressTimeout)
		if progressDue.Before(due) {
			due, reason = progressDue, snapshot.ReasonProgressTimeout
		}
	}
	return due, reason
}
func (s *snapshotClientState) timeoutLocked(now time.Time) error {
	due, reason := s.nextDeadlineLocked()
	if !now.Before(due) {
		return snapshotWireError(reason, nil)
	}
	return nil
}
