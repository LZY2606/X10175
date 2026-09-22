/*
   Copyright The containerd Authors.
*/

package ttrpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
)

var (
	descBidi         = &StreamDesc{StreamingClient: true, StreamingServer: true}
	descClientStream = &StreamDesc{StreamingClient: true, StreamingServer: false}
	descServerStream = &StreamDesc{StreamingClient: false, StreamingServer: true}
)

// ---------------------------------------------------------------------------
// client-streaming / bidi: CloseSend state
// ---------------------------------------------------------------------------

// TestSMCloseSendTwice verifies that a second CloseSend never reaches the
// wire and returns ErrStreamClosed.
func TestSMCloseSendTwice(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	if err := cs.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	closef := h.conn.nextWrite(t)
	if closef.typ() != messageTypeData ||
		closef.flags()&flagRemoteClosed == 0 ||
		closef.flags()&flagNoData == 0 {
		t.Fatalf("close frame malformed: type=%s flags=%#x", closef.typ(), closef.flags())
	}
	if closef.streamID() != sid {
		t.Fatalf("close frame sid=%d want %d", closef.streamID(), sid)
	}

	err := cs.CloseSend()
	assertExactError(t, err, ErrStreamClosed)
	h.conn.assertNoWrite(t)
}

// TestSMHalfCloseWithoutData verifies a client-streaming RPC may send no
// data and half-close immediately; the first wire frame after the request
// is the RC|NoData close frame.
func TestSMHalfCloseWithoutData(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, _ := h.newStream(ctx, descClientStream, "Upload", nil)

	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	f := h.conn.nextWrite(t)
	if f.typ() != messageTypeData {
		t.Fatalf("type=%s", f.typ())
	}
	if f.flags() != flagRemoteClosed|flagNoData {
		t.Fatalf("flags=%#x", f.flags())
	}

	// Server unary-ish final response ends the RPC.
	h.conn.enqueue(responseFrame(t, f.streamID(), codes.OK, "", &internal.TestPayload{Foo: "done"}))
	var resp internal.TestPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if resp.Foo != "done" {
		t.Fatalf("resp=%q", resp.Foo)
	}
	if err := cs.RecvMsg(&resp); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after final response, got %v", err)
	}
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set=%d want 0", got)
	}
}

// TestSMCloseSendMutationFlagOrder is a mutation-oriented contract test.
//
// Contract: CloseSend sets the local-closed flag only *after* the close
// frame has been written. While the close-frame write is in flight the
// stream must still appear open; after the write returns the flag is set.
//
// A mutation that moves `cs.localClosed = true` ahead of the send makes
// the flag observable as true while the write is parked, failing this
// test. The channel handshakes establish happens-before edges so reading
// the private field is race-free under -race.
func TestSMCloseSendMutationFlagOrder(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, _ := h.newStream(ctx, descBidi, "Bidi", nil)
	impl := cs.(*clientStream)

	hold := h.conn.pauseNextWrite()
	closeErr := closeSendAsync(cs)
	waitSignal(t, hold.entered, "CloseSend to park on write")

	// Parked mid-write: the frame is being sent but localClosed must not
	// yet be observable.
	if impl.localClosed {
		t.Fatal("localClosed observed before the close frame finished writing")
	}

	hold.releaseWrite()
	if err := waitErr(t, closeErr, "CloseSend"); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	f := h.conn.nextWrite(t)
	if f.typ() != messageTypeData || f.flags()&flagRemoteClosed == 0 {
		t.Fatalf("close frame malformed: %+v", f.header)
	}
	if !impl.localClosed {
		t.Fatal("localClosed not observed after successful close write")
	}

	// The next call is now the duplicate-CloseSend path.
	assertExactError(t, cs.CloseSend(), ErrStreamClosed)
	h.conn.assertNoWrite(t)
}

// TestSMBidiDataOrdering replays request -> data -> close on the client
// and asserts the exact wire sequence, then consumes a server data frame
// followed by the remote-closed marker and asserts data-before-EOF.
func TestSMBidiDataOrdering(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "a"}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	d1 := h.conn.nextWrite(t)
	if d1.typ() != messageTypeData || d1.flags() != 0 {
		t.Fatalf("data frame malformed: %+v", d1.header)
	}

	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	d2 := h.conn.nextWrite(t)
	if d2.flags() != flagRemoteClosed|flagNoData {
		t.Fatalf("close flags=%#x", d2.flags())
	}

	// Server sends one data frame carrying RC (final data) and no
	// separate marker. RecvMsg must return the payload once, then EOF.
	h.conn.enqueue(dataFrame(t, sid, flagRemoteClosed, &internal.EchoPayload{Seq: 2, Msg: "b"}))
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("RecvMsg data: %v", err)
	}
	if got.Seq != 2 || got.Msg != "b" {
		t.Fatalf("data payload seq=%d msg=%q", got.Seq, got.Msg)
	}
	// The RC flag removes the stream from the set before the final data
	// is returned; remoteClosed still makes the *next* RecvMsg EOF.
	if gotCount := h.streamCount(); gotCount != 0 {
		t.Fatalf("stream set=%d want 0 after RC data frame", gotCount)
	}
	if err := cs.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("after last data RecvMsg=%v want io.EOF", err)
	}
	if gotCount := h.streamCount(); gotCount != 0 {
		t.Fatalf("stream set=%d want 0 after EOF", gotCount)
	}
	// Every subsequent RecvMsg keeps returning EOF.
	if err := cs.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("post-EOF RecvMsg=%v", err)
	}
}

// TestSMServerStreamDataThenEOF covers the flag conventions where the
// server emits data frames without RC and finishes with a separate
// RC|NoData marker.
func TestSMServerStreamDataThenEOF(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descServerStream, "Watch", &internal.TestPayload{})
	sid := reqf.streamID()
	if reqf.flags()&flagRemoteClosed == 0 {
		t.Fatalf("non-streaming-client request must carry RC, flags=%#x", reqf.flags())
	}

	h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{Seq: 1}))
	h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{Seq: 2}))
	h.conn.enqueue(dataFrame(t, sid, flagRemoteClosed|flagNoData, nil))

	for i := int64(1); i <= 2; i++ {
		var got internal.EchoPayload
		if err := cs.RecvMsg(&got); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if got.Seq != i {
			t.Fatalf("seq=%d want %d", got.Seq, i)
		}
	}
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("final RecvMsg=%v want io.EOF", err)
	}
	if h.streamCount() != 0 {
		t.Fatal("stream not cleaned up after EOF")
	}
}

// TestSMFinalDataThenExtraFrames verifies messages arriving after the
// terminal RC frame are ignored (the stream is already deleted from the
// client set), and a later stream continues to function.
func TestSMFinalDataThenExtraFrames(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	h.conn.enqueue(dataFrame(t, sid, flagRemoteClosed|flagNoData, nil))
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("RecvMsg=%v want io.EOF", err)
	}
	if h.streamCount() != 0 {
		t.Fatal("stream not removed at remote close")
	}

	// Extra late frames on the closed id are dropped by the receive loop.
	h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{Seq: 99}))
	h.conn.enqueue(responseFrame(t, sid, codes.OK, "", &internal.TestPayload{}))

	// A new stream is unaffected.
	cs2, reqf2 := h.newStream(ctx, descBidi, "Bidi", nil)
	if reqf2.streamID() <= sid {
		t.Fatalf("stream ids must increase: %d <= %d", reqf2.streamID(), sid)
	}
	h.conn.enqueue(dataFrame(t, reqf2.streamID(), flagRemoteClosed|flagNoData, nil))
	if err := cs2.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("second stream RecvMsg=%v want io.EOF", err)
	}
}

// TestSMNonStreamingServerDataRejected verifies a data frame on a
// unary-like stream (StreamingServer=false) is a protocol error and
// removes the stream.
func TestSMNonStreamingServerDataRejected(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descClientStream, "Upload", nil)
	sid := reqf.streamID()

	h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{}))
	var got internal.EchoPayload
	err := cs.RecvMsg(&got)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("RecvMsg=%v want ErrProtocol", err)
	}
	if h.streamCount() != 0 {
		t.Fatal("stream not removed after protocol error")
	}
	// Remote is considered closed by the protocol error.
	if err := cs.RecvMsg(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("post-error RecvMsg=%v want io.EOF", err)
	}
}

// TestSMNonStreamingClientSendRejected verifies SendMsg/CloseSend on a
// stream whose client is non-streaming fail locally without touching the
// wire.
func TestSMNonStreamingClientSendRejected(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, _ := h.newStream(ctx, descServerStream, "Watch", &internal.TestPayload{})

	if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("SendMsg=%v want ErrProtocol", err)
	}
	if err := cs.CloseSend(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("CloseSend=%v want ErrProtocol", err)
	}
	h.conn.assertNoWrite(t)
}

// TestSMUnexpectedMessageType verifies a frame of an unknown/unexpected
// type produces a wrapped ErrProtocol and does not panic.
func TestSMUnexpectedMessageType(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	h.conn.enqueue(marshalFrame(t, sid, messageType(0x7f), 0, &internal.EchoPayload{}))
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); !errors.Is(err, ErrProtocol) {
		t.Fatalf("RecvMsg=%v want ErrProtocol", err)
	}
}

// ---------------------------------------------------------------------------
// backpressure and context-vs-status precedence
// ---------------------------------------------------------------------------

// TestSMClientQueueFullThenCancel verifies that when the receive loop is
// parked delivering a frame to a full stream buffer, cancelling the
// client context unblocks it with context.Canceled; the receive loop then
// runs cleanup and all streams observe ErrClosed. Pending messages remain
// drained before the closed error by RecvMsg.
func TestSMClientQueueFullThenCancel(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()
	s := h.getStream(streamID(sid))
	parked := waitRecvParked(s)

	// Fill the buffer (capacity streamRecvBufferSize) without consuming.
	for i := range streamRecvBufferSize {
		h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{Seq: int64(i)}))
	}
	// This extra frame must park the receive loop.
	h.conn.enqueue(dataFrame(t, sid, 0, &internal.EchoPayload{Seq: -1}))
	waitSignal(t, parked, "receive loop parking on full buffer")

	// While parked, cancel the client.
	h.c.closed()

	// RecvMsg drains the buffered messages in order first.
	var got internal.EchoPayload
	for i := range streamRecvBufferSize {
		if err := cs.RecvMsg(&got); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if got.Seq != int64(i) {
			t.Fatalf("seq=%d want %d", got.Seq, i)
		}
	}

	// After buffered data drains, the stream was closed with the cleanup
	// error (ErrClosed). The parked frame was not delivered.
	if err := cs.RecvMsg(&got); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-drain RecvMsg=%v want ErrClosed", err)
	}
	waitClosed(t, h.runDone(), "client run loop")
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d want 0 after cleanup", h.streamCount())
	}
}

// TestSMRecvClosedDrainsPending verifies the documented precedence: when
// the stream is closed but a message is buffered, RecvMsg returns the
// pending message rather than the close error; only the call after the
// drained one surfaces ErrClosed.
func TestSMRecvClosedDrainsPending(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	// Unit-level check of the RecvMsg select contract, driven directly so
	// no receive-loop scheduling is involved: buffer one message, then
	// close the stream with ErrClosed. RecvMsg must drain the pending
	// message before reporting the close error.
	s := h.getStream(streamID(sid))
	df := dataFrame(t, sid, 0, &internal.EchoPayload{Seq: 7})
	if err := s.receive(ctx, &streamMessage{header: df.header, payload: df.payload}); err != nil {
		t.Fatalf("buffer data: %v", err)
	}
	s.closeWithError(ErrClosed)

	var first internal.EchoPayload
	if err := cs.RecvMsg(&first); err != nil {
		t.Fatalf("RecvMsg should drain pending message, got %v", err)
	}
	if first.Seq != 7 {
		t.Fatalf("drained seq=%d want 7", first.Seq)
	}
	var after internal.EchoPayload
	if err := cs.RecvMsg(&after); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecvMsg after drain=%v want ErrClosed", err)
	}
}

// TestSMCtxCanceledBeforeStatus pins the observation rule at the
// cancellation / terminal-status boundary.
//
// Before the status arrives, cancellation is unambiguous: RecvMsg returns
// context.Canceled. Once the terminal status is delivered *while the
// context is already canceled*, both select cases are simultaneously
// ready and Go selects one at random - the implementation intentionally
// does not promise a winner there. What IS deterministic is the state
// around that race:
//
//   - the status message is delivered into the stream buffer by the
//     receive loop (it is not lost),
//   - the canceled stream entry lingers until the connection closes,
//   - closing the connection reaps it.
//
// The other deterministic orderings - status consumed before cancel, and
// cancel before status arrival - are covered by
// TestSMStatusConsumedBeforeCancel and the first half of this test.
func TestSMCtxCanceledBeforeStatus(t *testing.T) {
	h := newClientSMHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	cancel()
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecvMsg before status=%v want context.Canceled", err)
	}

	// Deliver the terminal status after cancellation: it must still reach
	// the stream (not be dropped), observable deterministically through the
	// delivery hook and buffer length.
	s := h.getStream(streamID(sid))
	delivered := waitDeliver(s)
	h.conn.enqueue(responseFrame(t, sid, codes.ResourceExhausted, "late", nil))
	select {
	case m := <-delivered:
		if m.header.Type != messageTypeResponse {
			t.Fatalf("delivered type=%s want response", m.header.Type)
		}
	case <-time.After(smWait):
		t.Fatal("terminal status not delivered into stream buffer")
	}
	if len(s.recv) != 1 {
		t.Fatalf("stream buffer len=%d want 1 (status must not be lost)", len(s.recv))
	}
	if h.streamCount() != 1 {
		t.Fatalf("stream set=%d want 1 (canceled ctx cannot reap it)", h.streamCount())
	}

	// Connection cleanup reaps the lingering stream regardless of which
	// select branch a future RecvMsg would take.
	h.c.Close()
	waitClosed(t, h.runDone(), "client run loop")
	h.waitStreamSet(func(n int) bool { return n == 0 }, "stream cleanup on close")
}

// TestSMStatusConsumedBeforeCancel pins the terminal-status ordering:
// when RecvMsg consumes a terminal Response *before* the context is
// canceled, the exact status code is observed (never collapsed into
// context.Canceled or into a generic error). The production non-OK
// response path returns before removing the stream, so that lingering
// entry is reaped by connection cleanup.
func TestSMStatusConsumedBeforeCancel(t *testing.T) {
	h := newClientSMHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cs, reqf := h.newStream(ctx, descBidi, "Bidi", nil)
	sid := reqf.streamID()

	s := h.getStream(streamID(sid))
	delivered := waitDeliver(s)
	h.conn.enqueue(responseFrame(t, sid, codes.PermissionDenied, "denied", nil))
	select {
	case <-delivered:
	case <-time.After(smWait):
		t.Fatal("terminal status not delivered into stream buffer")
	}

	// Consume the exact status while the context is still alive.
	var got internal.EchoPayload
	assertStatusCode(t, cs.RecvMsg(&got), codes.PermissionDenied)

	// Cancel afterwards cannot retroactively change what was observed.
	cancel()

	// Connection cleanup reaps the lingering non-OK stream entry.
	h.c.Close()
	waitClosed(t, h.runDone(), "client run loop")
	h.waitStreamSet(func(n int) bool { return n == 0 }, "stream cleanup on close")
}
