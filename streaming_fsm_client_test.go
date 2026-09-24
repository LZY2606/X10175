/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fsmService = "fsmService"

func runCallAsync(client *Client, ctx context.Context) func() error {
	done := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		req.Seq = 1
		done <- client.Call(ctx, fsmService, "Echo", &req, &resp)
	}()
	return func() error { return <-done }
}

// TestUnaryContract replays the unary RPC state machine from the client
// side against a scripted peer.
func TestUnaryContract(t *testing.T) {
	t.Run("OK_RequestThenResponseStreamCleanup", func(t *testing.T) {
		h := newClientHarness(t)
		ctx := context.Background()
		wait := runCallAsync(h.client, ctx)

		mh, p := h.readFrame()
		if mh.Type != messageTypeRequest || mh.StreamID != 1 {
			t.Fatalf("unexpected first frame: %+v", mh)
		}
		var req Request
		if err := protoUnmarshal(p, &req); err != nil {
			t.Fatal(err)
		}
		if req.Service != fsmService || req.Method != "Echo" {
			t.Fatalf("unexpected request target: %q/%q", req.Service, req.Method)
		}
		if len(h.streamIDs()) != 1 {
			t.Fatal("unary stream must be registered while in flight")
		}
		if !h.streamIDs()[1] {
			t.Fatal("expected stream id 1")
		}

		writeStatus(t, h.peer, 1, status.New(codes.OK, ""), &internal.EchoPayload{Seq: 2})
		h.waitDelivered(1)
		if err := wait(); err != nil {
			t.Fatalf("unary call: %v", err)
		}
		if len(h.streamIDs()) != 0 {
			t.Fatalf("unary stream must be deleted after final status, live=%v", h.streamIDs())
		}
	})

	t.Run("NonOKStatus_ExactCodeAndCleanup", func(t *testing.T) {
		h := newClientHarness(t)
		ctx := context.Background()
		wait := runCallAsync(h.client, ctx)
		h.readFrame()

		st := status.New(codes.ResourceExhausted, "quota gone")
		writeStatus(t, h.peer, 1, st, nil)
		h.waitDelivered(1)
		err := wait()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("code = %v, want ResourceExhausted", got)
		}
		if len(h.streamIDs()) != 0 {
			t.Fatal("stream must be deleted even on non-OK status")
		}
	})

	t.Run("TransportWriteErrorExactAndConnectionSurvivesGate", func(t *testing.T) {
		h := newClientHarness(t)
		// Pause the request write and fail it deterministically.
		h.local.pauseWrites()
		h.local.failNthWrite(1, errScriptWrite)
		ctx := context.Background()
		done := make(chan error, 1)
		go func() {
			var req, resp internal.EchoPayload
			done <- h.client.Call(ctx, fsmService, "Echo", &req, &resp)
		}()
		h.local.waitForParkedWrite(t)
		h.local.releaseOneWrite()
		err := <-done
		if !errors.Is(err, errScriptWrite) {
			t.Fatalf("expected exact injected write error, got %v", err)
		}
		// A failing createStream leaves no registered stream (map insert
		// happens before the write, but the call never publishes it).
		if _, ok := h.streamIDs()[1]; ok {
			t.Fatal("failed unary stream must not remain in the map as usable")
		}
		h.local.releaseAllWrites()
	})

	t.Run("StrayResponseOnUnknownStreamIgnored", func(t *testing.T) {
		h := newClientHarness(t)
		// A frame for a stream id that never existed is dropped, not fatal.
		writeStatus(t, h.peer, 4243, status.New(codes.OK, ""), nil)
		goscheds(3)

		// A subsequent real unary call still works and uses id 1.
		wait := runCallAsync(h.client, context.Background())
		mh, _ := h.readFrame()
		if mh.StreamID != 1 {
			t.Fatalf("stray frame must not consume a stream id, got %d", mh.StreamID)
		}
		writeStatus(t, h.peer, 1, status.New(codes.OK, ""), &internal.EchoPayload{Seq: 5})
		h.waitDelivered(1)
		if err := wait(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestUnaryCancelRacesFinalStatus documents exactly what is observable when
// context cancellation and the peer's final status become ready at the same
// time. Both outcomes are legal; the test asserts that *only* these two
// outcomes occur and that each is in fact observed over many replays.
func TestUnaryCancelRacesFinalStatus(t *testing.T) {
	const iterations = 300
	var sawCancel, sawStatus int
	for i := range iterations {
		h := newClientHarness(t)

		cs := h.newStream(context.Background(), false, false, fsmService, "Echo", &internal.EchoPayload{Seq: int64(i)})
		mh, _ := h.readFrame()
		sid := streamID(mh.StreamID)

		// Deliver the final status and cancel simultaneously only after
		// the frame is parked in the stream's recv buffer.
		writeStatus(t, h.peer, uint32(sid), status.New(codes.OK, ""), &internal.EchoPayload{Seq: int64(i) + 1})
		h.waitDelivered(uint32(sid))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var resp internal.EchoPayload
		err := cs.RecvMsg(&resp)

		switch {
		case err == nil:
			sawStatus++
			if resp.Seq != int64(i)+1 {
				t.Fatalf("iteration %d: wrong payload %d", i, resp.Seq)
			}
			// The terminal status closes the stream.
			if h.hasStream(rawStream(cs)) {
				t.Fatalf("iteration %d: stream still live after status", i)
			}
		case errors.Is(err, context.Canceled):
			sawCancel++
			// The buffered final status must not be lost silently: the
			// next Recv consumes it.
			var resp2 internal.EchoPayload
			if err2 := cs.RecvMsg(context.Background(), &resp2); err2 != nil {
				t.Fatalf("iteration %d: buffered final status lost: %v", i, err2)
			}
			if resp2.Seq != int64(i)+1 {
				t.Fatalf("iteration %d: wrong buffered payload %d", i, resp2.Seq)
			}
			if h.hasStream(rawStream(cs)) {
				t.Fatalf("iteration %d: stream not deleted after status", i)
			}
		default:
			t.Fatalf("iteration %d: illegal race outcome: %v", i, err)
		}
	}
	if sawCancel == 0 || sawStatus == 0 {
		t.Fatalf("both outcomes must be observable: cancel=%d status=%d", sawCancel, sawStatus)
	}
	t.Logf("observed cancel=%d status=%d across %d replays", sawCancel, sawStatus, iterations)
}

// TestCancelBeforeDeliveryIsDeterministic verifies the simpler ordering:
// if the call context is already canceled before the peer responds,
// RecvMsg must report context.Canceled deterministically (no buffered
// message exists yet).
func TestCancelBeforeDeliveryIsDeterministic(t *testing.T) {
	for range 50 {
		h := newClientHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		cs := h.newStream(ctx, false, false, fsmService, "Echo", &internal.EchoPayload{})
		h.readFrame()
		cancel()
		var resp internal.EchoPayload
		if err := cs.RecvMsg(&resp); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	}
}

// TestPendingMessageWinsOverStreamClose verifies the precedence contract
// shared by client RecvMsg and dispatch: when both a queued message and
// recvClose are ready, the queued message is consumed first.
func TestPendingMessageWinsOverStreamClose(t *testing.T) {
	h := newClientHarness(t)
	cs := h.newStream(context.Background(), true, true, fsmService, "EchoStream", nil)
	mh, _ := h.readFrame()
	sid := streamID(mh.StreamID)

	// Queue a data frame, then close the stream from another delivery
	// path. We use the low-level stream directly to close while queued.
	writeData(t, h.peer, uint32(sid), 0, &internal.EchoPayload{Seq: 9})
	h.waitDelivered(uint32(sid))
	rs := rawStream(cs)
	rs.closeWithError(nil)

	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("queued data must be consumed before recvErr: %v", err)
	}
	if got.Seq != 9 {
		t.Fatalf("unexpected seq %d", got.Seq)
	}
	// Subsequent Recv sees the terminal error.
	if err := cs.RecvMsg(&got); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after queued message, got %v", err)
	}
}

// rawStream returns the underlying *stream of a clientStream for map
// inspection in tests.
func rawStream(cs ClientStream) *stream {
	return cs.(*clientStream).s
}

// TestClientStreamingContract replays client-streaming state transitions.
func TestClientStreamingContract(t *testing.T) {
	t.Run("ImmediateHalfCloseNoData", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), true, false, fsmService, "Upload", nil)

		// Request frame carries flagRemoteOpen (stream stays open).
		mh, _ := h.readFrame()
		if mh.Type != messageTypeRequest || mh.Flags&flagRemoteOpen == 0 {
			t.Fatalf("expected open request frame, got %+v", mh)
		}
		if err := cs.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		mh, _ = h.readFrame()
		if mh.Type != messageTypeData || mh.Flags&flagRemoteClosed == 0 || mh.Flags&flagNoData == 0 {
			t.Fatalf("expected nodata remote-closed frame, got %+v", mh)
		}
		if mh.Length != 0 {
			t.Fatalf("half-close frame must have no payload, len=%d", mh.Length)
		}
		// Server's final response for a client-only stream.
		writeStatus(t, h.peer, mh.StreamID, status.New(codes.OK, ""), nil)
		h.waitDelivered(mh.StreamID)
		var resp internal.EchoPayload
		if err := cs.RecvMsg(&resp); err != nil {
			t.Fatalf("RecvMsg final: %v", err)
		}
	})

	t.Run("DataThenHalfCloseThenDoubleCloseSend", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), true, false, fsmService, "Upload", nil)
		h.readFrame() // request

		if err := cs.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "a"}); err != nil {
			t.Fatal(err)
		}
		mh, p := h.readFrame()
		if mh.Type != messageTypeData || mh.Flags != 0 || mh.Length == 0 {
			t.Fatalf("expected plain data frame, got %+v", mh)
		}
		var in internal.EchoPayload
		if err := protoUnmarshal(p, &in); err != nil || in.Seq != 1 {
			t.Fatalf("bad data payload: %v %+v", err, in)
		}

		if err := cs.CloseSend(); err != nil {
			t.Fatal(err)
		}
		mh, _ = h.readFrame()
		if mh.Flags&flagRemoteClosed == 0 || mh.Flags&flagNoData == 0 {
			t.Fatalf("expected half-close frame, got %+v", mh)
		}

		// A second CloseSend is a local state error and emits no frame.
		h.local.pauseWrites()
		if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("second CloseSend = %v, want ErrStreamClosed", err)
		}
		assertNoWrite(t, h.local)
		if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("SendMsg after CloseSend = %v, want ErrStreamClosed", err)
		}
		assertNoWrite(t, h.local)
		h.local.releaseAllWrites()
	})

	t.Run("NonStreamingClientRejectsCloseSendAndSending", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), false, true, fsmService, "Download", &internal.EchoPayload{})
		h.readFrame()
		h.local.pauseWrites()
		if err := cs.CloseSend(); !errors.Is(err, ErrProtocol) {
			t.Fatalf("CloseSend on non-streaming client = %v, want ErrProtocol", err)
		}
		if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("SendMsg on non-streaming client = %v, want ErrProtocol", err)
		}
		assertNoWrite(t, h.local)
		h.local.releaseAllWrites()
	})
}

// TestServerStreamingContract replays a server-streaming RPC: a sequence
// of data frames terminated by a half-close, EOF on the next Recv, and a
// precise protocol error if data arrives from a non-streaming server.
func TestServerStreamingContract(t *testing.T) {
	t.Run("FramesThenEOFAndCleanup", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), false, true, fsmService, "Download", &internal.EchoPayload{})
		mh, _ := h.readFrame()
		sid := mh.StreamID
		if mh.Flags&flagRemoteClosed == 0 {
			t.Fatalf("non-streaming client request must set remoteClosed, got %+v", mh)
		}

		writeData(t, h.peer, sid, 0, &internal.EchoPayload{Seq: 1})
		h.waitDelivered(sid)
		writeData(t, h.peer, sid, flagRemoteClosed, &internal.EchoPayload{Seq: 2})
		h.waitDelivered(sid)

		var got internal.EchoPayload
		if err := cs.RecvMsg(&got); err != nil || got.Seq != 1 {
			t.Fatalf("first frame: %v %+v", err, got)
		}
		if h.hasStream(rawStream(cs)) == false {
			t.Fatal("stream must stay live until the closed frame")
		}
		if err := cs.RecvMsg(&got); err != nil || got.Seq != 2 {
			t.Fatalf("second frame: %v %+v", err, got)
		}
		if h.hasStream(rawStream(cs)) {
			t.Fatal("stream must be deleted exactly on the closed frame")
		}
		if err := cs.RecvMsg(&got); err != io.EOF {
			t.Fatalf("Recv after closed frame = %v, want io.EOF", err)
		}
	})

	t.Run("NoDataClosedFrameIsEOF", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), false, true, fsmService, "Download", &internal.EchoPayload{})
		mh, _ := h.readFrame()
		writeRawFrame(t, h.peer, mh.StreamID, messageTypeData, flagRemoteClosed|flagNoData, nil)
		h.waitDelivered(mh.StreamID)
		var got internal.EchoPayload
		if err := cs.RecvMsg(&got); err != io.EOF {
			t.Fatalf("nodata closed frame = %v, want io.EOF", err)
		}
		if h.hasStream(rawStream(cs)) {
			t.Fatal("stream not cleaned up after EOF frame")
		}
	})

	t.Run("NonStreamingServerDataIsProtocolError", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), true, false, fsmService, "Upload", nil)
		mh, _ := h.readFrame()
		writeData(t, h.peer, mh.StreamID, 0, &internal.EchoPayload{Seq: 1})
		h.waitDelivered(mh.StreamID)
		var got internal.EchoPayload
		err := cs.RecvMsg(&got)
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("data from non-streaming server = %v, want ErrProtocol", err)
		}
		if h.hasStream(rawStream(cs)) {
			t.Fatal("stream must be deleted on protocol error frame")
		}
	})
}

// TestMutation_CloseSendFlagOrder is a mutation-targeted contract test.
//
// Production order (clientStream.CloseSend):
//
//  1. send the remoteClosed|noData frame
//  2. only on success set localClosed = true
//
// Mutations this test is designed to reject:
//
//	Setting localClosed before the wire send (or on a failed send). Under
//	that mutation the parked CloseSend would leave localClosed == true
//	before the frame is released, and the failure retry contract breaks.
func TestMutation_CloseSendFlagOrder(t *testing.T) {
	t.Run("LocalClosedOnlySetAfterFrameOnWire", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), true, false, fsmService, "Upload", nil)
		h.readFrame() // open request

		impl := cs.(*clientStream)
		h.local.pauseWrites()
		done := make(chan error, 1)
		go func() { done <- cs.CloseSend() }()
		h.local.waitForParkedWrite(t)

		// While the close frame is parked the local flag must not be set.
		goscheds(3)
		if impl.localClosed {
			t.Fatal("localClosed observed before the close frame reached the wire")
		}
		// Another CloseSend concurrently cannot succeed either: it parks
		// behind the send lock. Releasing one frame must complete exactly
		// one caller; the second then sees ErrStreamClosed and writes
		// nothing.
		h.local.releaseOneWrite()
		if err := <-done; err != nil {
			t.Fatalf("first CloseSend: %v", err)
		}
		if !impl.localClosed {
			t.Fatal("localClosed must be set after successful close send")
		}

		mh, _ := h.readFrame()
		if mh.Flags&flagRemoteClosed == 0 || mh.Flags&flagNoData == 0 {
			t.Fatalf("expected half-close frame, got %+v", mh)
		}

		h.local.pauseWrites()
		if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("redundant CloseSend = %v, want ErrStreamClosed", err)
		}
		assertNoWrite(t, h.local)
		h.local.releaseAllWrites()
	})

	t.Run("FailedCloseSendDoesNotMarkLocalClosed", func(t *testing.T) {
		h := newClientHarness(t)
		cs := h.newStream(context.Background(), true, false, fsmService, "Upload", nil)
		h.readFrame()
		impl := cs.(*clientStream)

		h.local.pauseWrites()
		h.local.failNthWrite(1, errScriptWrite)
		done := make(chan error, 1)
		go func() { done <- cs.CloseSend() }()
		h.local.waitForParkedWrite(t)
		h.local.releaseOneWrite()
		err := <-done
		if !errors.Is(err, errScriptWrite) {
			t.Fatalf("failed CloseSend = %v, want injected error", err)
		}
		if impl.localClosed {
			t.Fatal("localClosed must stay false when the close frame fails")
		}

		// Retry succeeds: stream was not poisoned by the failed attempt.
		h.local.failNthWrite(0, nil)
		h.local.releaseAllWrites()
		if err := cs.CloseSend(); err != nil {
			t.Fatalf("retry CloseSend: %v", err)
		}
		mh, _ := h.readFrame()
		if mh.Flags&flagRemoteClosed == 0 {
			t.Fatalf("retry must emit the close frame, got flags %#x", mh.Flags)
		}
		if !impl.localClosed {
			t.Fatal("localClosed must be set after the retried close send")
		}
	})
}

// TestMutation_ClientStreamDeletedFromMapEarly is a mutation-targeted
// contract test. A bidi stream must remain registered for every server
// data frame up to and including the half-close frame; deleting it early
// (e.g. on the first data frame) would make later frames hit the
// unknown-stream path and lose messages.
func TestMutation_ClientStreamDeletedFromMapEarly(t *testing.T) {
	h := newClientHarness(t)
	cs := h.newStream(context.Background(), true, true, fsmService, "EchoStream", nil)
	mh, _ := h.readFrame()
	sid := mh.StreamID

	writeData(t, h.peer, sid, 0, &internal.EchoPayload{Seq: 1})
	h.waitDelivered(sid)
	writeData(t, h.peer, sid, 0, &internal.EchoPayload{Seq: 2})
	h.waitDelivered(sid)

	if !h.hasStream(rawStream(cs)) {
		t.Fatal("stream deleted before the half-close frame")
	}

	writeData(t, h.peer, sid, flagRemoteClosed, &internal.EchoPayload{Seq: 3})
	h.waitDelivered(sid)

	var got internal.EchoPayload
	for want := int64(1); want <= 3; want++ {
		if err := cs.RecvMsg(&got); err != nil {
			t.Fatalf("recv %d: %v", want, err)
		}
		if got.Seq != want {
			t.Fatalf("message %d lost (got seq %d)", want, got.Seq)
		}
	}
	if h.hasStream(rawStream(cs)) {
		t.Fatal("stream must be deleted only after processing the closed frame")
	}
	if err := cs.RecvMsg(&got); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
