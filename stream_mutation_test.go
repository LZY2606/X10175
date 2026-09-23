package ttrpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// TestContractCloseSendFlagSetAfterSend is a mutation contract test: it
// pins the update order in clientStream.CloseSend, where the local
// closed flag is only set after the close frame has been handed to the
// transport. If that order is swapped (flag first, send second), a
// concurrent second CloseSend observes the flag and returns
// ErrStreamClosed without ever reaching the transport, and this test
// fails.
func TestContractCloseSendFlagSetAfterSend(t *testing.T) {
	sender := &contractBlockingSender{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	cs := &clientStream{
		ctx:  context.Background(),
		s:    newStream(1, sender, 1),
		desc: &StreamDesc{StreamingClient: true},
	}

	first := make(chan error, 1)
	go func() { first <- cs.CloseSend() }()
	contractAwaitSignal(t, sender.entered, "first CloseSend reaching transport")
	select {
	case err := <-first:
		t.Fatalf("first CloseSend completed while transport was blocked: %v", err)
	default:
	}

	// While the first CloseSend is in flight, a second one must also
	// reach the transport: the flag is not set until the send returns.
	second := make(chan error, 1)
	go func() { second <- cs.CloseSend() }()
	select {
	case <-sender.entered:
	case err := <-second:
		t.Fatalf("second CloseSend returned %v without reaching the transport; the closed flag was set before the send", err)
	case <-time.After(contractWaitDeadline):
		t.Fatal("second CloseSend neither reached the transport nor returned")
	}

	// Release the sends one at a time so the flag writes do not race.
	sender.release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	sender.release <- struct{}{}
	if err := <-second; err != nil {
		t.Fatalf("second CloseSend: %v", err)
	}

	if !cs.localClosed {
		t.Fatal("localClosed not set after successful CloseSend")
	}
	// With the flag now set, further CloseSend calls are local errors.
	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("CloseSend after close: got %v, want ErrStreamClosed", err)
	}
}

// contractBlockingSender blocks every send until one token is received
// on release, and reports each entered send on entered.
type contractBlockingSender struct {
	entered chan struct{}
	release chan struct{}
}

func (s *contractBlockingSender) send(uint32, messageType, uint8, []byte) error {
	s.entered <- struct{}{}
	<-s.release
	return nil
}

// TestContractStreamRemovedAfterFinalFrame is a mutation contract test:
// it pins that the client removes a stream from its table only after
// the final frame has been consumed by RecvMsg. If the stream is
// removed from the map earlier (e.g. deleteStream moved ahead of the
// final-frame processing), the intermediate frames can no longer be
// delivered and this test fails.
func TestContractStreamRemovedAfterFinalFrame(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", &internal.EchoPayload{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// Park the server's third write: the final closing data frame.
	h.serverConn.holdWriteAt(2)
	close(h.s2cStart)

	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("first RecvMsg: %v", err)
	}
	if msg.Seq != 1 {
		t.Fatalf("first message seq %d, want 1", msg.Seq)
	}
	// An intermediate data frame must not remove the stream.
	if n := h.clientStreamCount(); n != 1 {
		t.Fatalf("stream removed after intermediate frame: count %d, want 1", n)
	}

	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("second RecvMsg: %v", err)
	}
	if msg.Seq != 2 {
		t.Fatalf("second message seq %d, want 2", msg.Seq)
	}
	if n := h.clientStreamCount(); n != 1 {
		t.Fatalf("stream removed after intermediate frame: count %d, want 1", n)
	}

	// The final frame has been attempted but not delivered; the stream
	// must still be registered.
	h.serverConn.waitParked(t)
	if n := h.clientStreamCount(); n != 1 {
		t.Fatalf("stream removed before final frame was consumed: count %d, want 1", n)
	}

	// Release the final frame: it is consumed and only then is the
	// stream removed.
	h.serverConn.releaseWrite()
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("final RecvMsg: %v", err)
	}
	if msg.Seq != 99 {
		t.Fatalf("final message seq %d, want 99", msg.Seq)
	}
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final frame: count %d, want 0", n)
	}
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after final frame: got %v, want io.EOF", err)
	}
}
