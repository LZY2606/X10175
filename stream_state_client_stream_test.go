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

// openClientStream starts NewStream in a goroutine (its opening write blocks
// until the peer reads), receives the opening request frame and returns the
// established stream plus that frame.
func openClientStream(t *testing.T, h *clientHarness, desc *StreamDesc, method string) (ClientStream, frame) {
	t.Helper()
	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := h.cl.NewStream(context.Background(), desc, stateTestService, method, nil)
		streamCh <- streamResult{cs, err}
	}()
	open := h.peer.recv()
	res := <-streamCh
	if res.err != nil {
		t.Fatalf("NewStream: %v", res.err)
	}
	if open.header.Type != messageTypeRequest {
		t.Fatalf("opening frame type = %v, want request", open.header.Type)
	}
	return res.cs, open
}

// TestStateCloseSendTwiceContract: the first CloseSend emits exactly one
// flagRemoteClosed|flagNoData data frame; a second CloseSend returns
// ErrStreamClosed and emits nothing further on the wire.
func TestStateCloseSendTwiceContract(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingClient: true, StreamingServer: true}, "Bidi")
	if open.header.Flags&flagRemoteOpen == 0 {
		t.Fatalf("streaming open flags = %#x, want flagRemoteOpen", open.header.Flags)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	halfClose := h.peer.recv()
	if err := waitErr(t, closeDone); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	if halfClose.header.Type != messageTypeData {
		t.Fatalf("half-close type = %v, want data", halfClose.header.Type)
	}
	if halfClose.header.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("half-close flags = %#x, want %#x", halfClose.header.Flags, flagRemoteClosed|flagNoData)
	}
	if halfClose.header.Length != 0 {
		t.Fatalf("half-close carried %d payload bytes, want 0", halfClose.header.Length)
	}

	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend err = %v, want ErrStreamClosed", err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendMsg after CloseSend err = %v, want ErrStreamClosed", err)
	}

	// No further frame may appear: park the next write and prove none is
	// attempted by observing the gate stays un-tripped while the peer
	// receives the terminal response instead.
	h.peer.send(dataFrame(open.header.StreamID, nil, flagRemoteClosed|flagNoData))
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("RecvMsg after remote close err = %v, want io.EOF", err)
	}
}

// TestStateHalfCloseBeforeAnyData: CloseSend as the very first operation
// after the opening request must still produce exactly the empty half-close
// frame, with no preceding data frames.
func TestStateHalfCloseBeforeAnyData(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingClient: true, StreamingServer: true}, "Bidi")

	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	first := h.peer.recv()
	if err := waitErr(t, closeDone); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if first.header.StreamID != open.header.StreamID {
		t.Fatalf("half-close stream id = %d, want %d", first.header.StreamID, open.header.StreamID)
	}
	if first.header.Type != messageTypeData ||
		first.header.Flags != flagRemoteClosed|flagNoData ||
		first.header.Length != 0 {
		t.Fatalf("first post-open frame is not an empty half-close: %+v", first.header)
	}
}

// TestStateCloseSendWriteFailureOrder is mutation contract M1: the local
// closed flag must latch only after the close frame is written successfully.
// If the order were swapped (latch first, write second), the failed first
// CloseSend would still mark the stream closed and the retry would report
// ErrStreamClosed instead of re-surfacing the transport error.
func TestStateCloseSendWriteFailureOrder(t *testing.T) {
	h := newClientHarness(t)

	cs, _ := openClientStream(t, h, &StreamDesc{StreamingClient: true, StreamingServer: true}, "Bidi")

	parked := make(chan *parkedWrite, 1)
	h.sc.failNextWrite(parked, errScriptedWrite)
	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	pw := <-parked
	close(pw.release)

	if err := waitErr(t, closeDone); !errors.Is(err, errScriptedWrite) {
		t.Fatalf("first CloseSend err = %v, want scripted write error", err)
	}

	// The failed write must not have latched localClosed: a retry attempts
	// the wire write again and surfaces the (persisted) transport error,
	// never ErrStreamClosed.
	if err := cs.CloseSend(); !errors.Is(err, errScriptedWrite) {
		t.Fatalf("retry CloseSend err = %v, want scripted write error", err)
	}
}

// TestStateServerStreamContract replays a server-streaming RPC: data frames
// keep the stream registered, the terminal flagRemoteClosed data frame
// deletes it exactly once, and RecvMsg afterwards reports io.EOF.
func TestStateServerStreamContract(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	sid := open.header.StreamID
	if open.header.Flags&flagRemoteOpen != 0 {
		t.Fatalf("non-streaming client must not set flagRemoteOpen, got %#x", open.header.Flags)
	}

	h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: 1, Msg: "one"}, 0))
	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("RecvMsg data: %v", err)
	}
	if got.Seq != 1 || got.Msg != "one" {
		t.Fatalf("unexpected data seq=%d msg=%q", got.Seq, got.Msg)
	}
	if !h.hasStream(streamID(sid)) {
		t.Fatal("stream deleted before terminal frame")
	}

	h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: 2, Msg: "two"}, flagRemoteClosed))
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("RecvMsg terminal data: %v", err)
	}
	if got.Seq != 2 {
		t.Fatalf("terminal data seq = %d, want 2", got.Seq)
	}
	h.log.waitEvent(t, evClientDeleted)
	if h.hasStream(streamID(sid)) {
		t.Fatal("stream still registered after terminal data frame")
	}

	if err := cs.RecvMsg(&got); err != io.EOF {
		t.Fatalf("RecvMsg after terminal err = %v, want io.EOF", err)
	}
	if err := cs.RecvMsg(&got); err != io.EOF {
		t.Fatalf("repeated RecvMsg err = %v, want io.EOF", err)
	}
	if h.log.count(evClientDeleted) != 1 {
		t.Fatalf("stream deleted %d times, want exactly 1", h.log.count(evClientDeleted))
	}
}

// TestStateServerStreamEmptyTerminal covers the flagNoData terminal variant:
// the close frame carries no payload and RecvMsg reports io.EOF immediately.
func TestStateServerStreamEmptyTerminal(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	sid := open.header.StreamID

	h.peer.send(dataFrame(sid, nil, flagRemoteClosed|flagNoData))
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("RecvMsg err = %v, want io.EOF", err)
	}
	h.log.waitEvent(t, evClientDeleted)
	if h.hasStream(streamID(sid)) {
		t.Fatal("stream still registered after empty terminal frame")
	}
}

// TestStateFramesAfterFinalStatusRejected: once the final status removed the
// stream from the table, further frames for that id are rejected by the
// receive loop; the stream must not be resurrected or re-deleted, and a
// concurrent healthy stream on the same connection is unaffected.
func TestStateFramesAfterFinalStatusRejected(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	sid := open.header.StreamID

	h.peer.send(dataFrame(sid, nil, flagRemoteClosed|flagNoData))
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("RecvMsg err = %v, want io.EOF", err)
	}
	h.log.waitEvent(t, evClientDeleted)

	// Late frames on the retired id are dropped by the receive loop.
	h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: 99}, 0))
	h.peer.send(responseFrame(t, sid, status.New(codes.OK, ""), &internal.EchoPayload{}))

	// A second stream on the same connection still works, proving the
	// rejected frames did not poison the receive loop.
	cs2, open2 := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	if open2.header.StreamID != sid+2 {
		t.Fatalf("second stream id = %d, want %d", open2.header.StreamID, sid+2)
	}
	h.peer.send(dataFrame(open2.header.StreamID, &internal.EchoPayload{Seq: 5}, flagRemoteClosed))
	var got internal.EchoPayload
	if err := cs2.RecvMsg(&got); err != nil {
		t.Fatalf("RecvMsg on second stream: %v", err)
	}
	if got.Seq != 5 {
		t.Fatalf("second stream seq = %d, want 5", got.Seq)
	}

	if h.log.count(evClientDeleted) != 2 {
		t.Fatalf("deleted count = %d, want 2 (one per stream)", h.log.count(evClientDeleted))
	}
	if h.hasStream(streamID(sid)) {
		t.Fatal("late frame resurrected a retired stream")
	}
}

// TestStateBidiContract replays a full bidirectional exchange: interleaved
// data frames in both directions, local half-close, remote terminal frame,
// then io.EOF.
func TestStateBidiContract(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingClient: true, StreamingServer: true}, "Bidi")
	sid := open.header.StreamID

	// Client sends two data frames; peer echoes them back.
	for i := int64(1); i <= 2; i++ {
		sendDone := make(chan error, 1)
		go func(seq int64) {
			sendDone <- cs.SendMsg(&internal.EchoPayload{Seq: seq, Msg: "c2s"})
		}(i)
		out := h.peer.recv()
		if err := waitErr(t, sendDone); err != nil {
			t.Fatalf("SendMsg %d: %v", i, err)
		}
		if out.header.Type != messageTypeData || out.header.Flags != 0 {
			t.Fatalf("client data frame %d wrong: %+v", i, out.header)
		}
		if got := decodeEcho(t, out); got.Seq != i || got.Msg != "c2s" {
			t.Fatalf("client data %d = seq %d msg %q", i, got.Seq, got.Msg)
		}
		h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: i, Msg: "s2c"}, 0))
		var in internal.EchoPayload
		if err := cs.RecvMsg(&in); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if in.Seq != i || in.Msg != "s2c" {
			t.Fatalf("server data %d = seq %d msg %q", i, in.Seq, in.Msg)
		}
	}

	// Local half-close, then remote terminal frame.
	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	halfClose := h.peer.recv()
	if err := waitErr(t, closeDone); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if halfClose.header.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("half-close flags = %#x", halfClose.header.Flags)
	}

	h.peer.send(dataFrame(sid, nil, flagRemoteClosed|flagNoData))
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("final RecvMsg err = %v, want io.EOF", err)
	}
	h.log.waitEvent(t, evClientDeleted)
	if h.hasStream(streamID(sid)) {
		t.Fatal("bidi stream still registered after terminal frame")
	}
}

// TestStateStreamIDMonotonicAcrossKinds verifies that unary calls and
// streams share one strictly increasing odd id sequence on a connection.
func TestStateStreamIDMonotonicAcrossKinds(t *testing.T) {
	h := newClientHarness(t)

	errs := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errs <- h.cl.Call(context.Background(), stateTestService, "Echo", &internal.EchoPayload{}, &resp)
	}()
	unary := h.peer.recv()
	h.peer.send(responseFrame(t, unary.header.StreamID, status.New(codes.OK, ""), &internal.EchoPayload{}))
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("Call: %v", err)
	}

	_, open := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	if open.header.StreamID != unary.header.StreamID+2 {
		t.Fatalf("stream id = %d, want %d", open.header.StreamID, unary.header.StreamID+2)
	}
}
