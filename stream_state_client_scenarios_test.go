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
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStateUnaryContract replays a unary RPC at the state-machine level:
// request frame leaves with flagRemoteClosed, final status comes back, the
// stream is removed from the table exactly when the response is consumed,
// and a follow-up call uses the next odd stream id.
func TestStateUnaryContract(t *testing.T) {
	h := newClientHarness(t)

	errs := make(chan error, 1)
	var resp internal.EchoPayload
	go func() {
		errs <- h.cl.Call(context.Background(), stateTestService, "Echo",
			&internal.EchoPayload{Seq: 1, Msg: "ping"}, &resp)
	}()

	reqFrame := h.peer.recv()
	if reqFrame.header.Type != messageTypeRequest {
		t.Fatalf("first frame type = %v, want request", reqFrame.header.Type)
	}
	if reqFrame.header.StreamID != 1 {
		t.Fatalf("first stream id = %d, want 1", reqFrame.header.StreamID)
	}
	if reqFrame.header.Flags != 0 {
		t.Fatalf("unary request flags = %#x, want 0", reqFrame.header.Flags)
	}
	h.log.waitEvent(t, evClientRegistered)
	if !h.hasStream(1) {
		t.Fatal("stream 1 must be registered before the response")
	}

	h.peer.send(responseFrame(t, 1, status.New(codes.OK, ""), &internal.EchoPayload{Seq: 2, Msg: "pong"}))
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Seq != 2 || resp.Msg != "pong" {
		t.Fatalf("unexpected response seq=%d msg=%q", resp.Seq, resp.Msg)
	}
	h.log.waitEvent(t, evClientDeleted)
	if h.hasStream(1) {
		t.Fatal("stream 1 must be removed after the final response")
	}
	if len(h.streams()) != 0 {
		t.Fatalf("stream table must be empty after unary completion, got %v", h.streams())
	}

	go func() {
		errs <- h.cl.Call(context.Background(), stateTestService, "Echo",
			&internal.EchoPayload{}, &resp)
	}()
	second := h.peer.recv()
	if second.header.StreamID != 3 {
		t.Fatalf("second stream id = %d, want 3", second.header.StreamID)
	}
	h.peer.send(responseFrame(t, 3, status.New(codes.OK, ""), &internal.EchoPayload{}))
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("second Call: %v", err)
	}
}

// TestStateUnaryStatusPropagation asserts that a non-OK final status is the
// exact error surfaced to Call and that the stream is still cleaned up.
func TestStateUnaryStatusPropagation(t *testing.T) {
	h := newClientHarness(t)

	errs := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errs <- h.cl.Call(context.Background(), stateTestService, "Echo", &internal.EchoPayload{}, &resp)
	}()

	reqFrame := h.peer.recv()
	h.peer.send(responseFrame(t, reqFrame.header.StreamID,
		status.New(codes.ResourceExhausted, "quota used"), nil))

	err := waitErr(t, errs)
	if statusCode(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted (err=%v)", statusCode(err), err)
	}
	h.log.waitEvent(t, evClientDeleted)
	if len(h.streams()) != 0 {
		t.Fatalf("streams leaked after status error: %v", h.streams())
	}
}

// TestStateUnaryContextCanceledBeforeStatus: when the caller's context is
// canceled before any frame arrives, Call returns context.Canceled; the
// late-arriving response then targets a stream the caller abandoned and must
// not resurrect or re-register it.
func TestStateUnaryContextCanceledBeforeStatus(t *testing.T) {
	h := newClientHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errs <- h.cl.Call(ctx, stateTestService, "Echo", &internal.EchoPayload{}, &resp)
	}()

	reqFrame := h.peer.recv()
	h.log.waitEvent(t, evClientRegistered)
	cancel()
	if err := waitErr(t, errs); !errors.Is(err, context.Canceled) {
		t.Fatalf("Call err = %v, want context.Canceled", err)
	}

	h.peer.send(responseFrame(t, reqFrame.header.StreamID, status.New(codes.OK, ""), &internal.EchoPayload{}))
	if h.hasStream(streamID(reqFrame.header.StreamID)) {
		t.Fatal("canceled unary stream must not be re-registered by a late response")
	}
}

// TestStateUnaryFinalStatusBeatsClose verifies the receive rule that a
// message already buffered is consumed even when the stream is concurrently
// closed with an error: the final status, not the close error, is observable.
func TestStateUnaryFinalStatusBeatsClose(t *testing.T) {
	h := newClientHarness(t)

	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := h.cl.NewStream(context.Background(), &StreamDesc{},
			stateTestService, "Echo", &internal.EchoPayload{})
		streamCh <- streamResult{cs, err}
	}()
	reqFrame := h.peer.recv()
	res := <-streamCh
	if res.err != nil {
		t.Fatalf("NewStream: %v", res.err)
	}
	cs := res.cs

	h.peer.send(responseFrame(t, reqFrame.header.StreamID,
		status.New(codes.OK, ""), &internal.EchoPayload{Msg: "final"}))
	h.waitRecvBuffer(t, streamID(reqFrame.header.StreamID), 1)

	h.getStreamForTest(streamID(reqFrame.header.StreamID)).closeWithError(ErrClosed)

	var out internal.EchoPayload
	if err := cs.RecvMsg(&out); err != nil {
		t.Fatalf("buffered final status must win over close, got %v", err)
	}
	if out.Msg != "final" {
		t.Fatalf("payload = %q, want %q", out.Msg, "final")
	}
	h.log.waitEvent(t, evClientDeleted)
	if h.hasStream(streamID(reqFrame.header.StreamID)) {
		t.Fatal("stream must be removed after consuming the final response")
	}
}

// TestStateUnaryWriteErrorContract injects a deterministic transport failure
// into the request write and asserts the exact error is surfaced (not
// collapsed into ErrClosed) and the connection-wide teardown sweeps the
// table.
func TestStateUnaryWriteErrorContract(t *testing.T) {
	h := newClientHarness(t)

	parked := make(chan *parkedWrite, 1)
	h.sc.failNextWrite(parked, errScriptedWrite)

	errs := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errs <- h.cl.Call(context.Background(), stateTestService, "Echo", &internal.EchoPayload{}, &resp)
	}()
	pw := <-parked
	h.log.waitEvent(t, evClientRegistered)
	if !h.hasStream(1) {
		t.Fatal("stream must be registered even when its first write is parked")
	}
	close(pw.release)

	err := waitErr(t, errs)
	if !errors.Is(err, errScriptedWrite) {
		t.Fatalf("Call err = %v, want scripted write error", err)
	}
	// The failed write leaves the stream registered; only connection
	// teardown sweeps it.
	if !h.hasStream(1) {
		t.Fatal("stream must remain registered until connection teardown")
	}
	if err := h.sc.Conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}
	h.log.waitEvent(t, evClientRun)
	if got := len(h.streams()); got != 0 {
		t.Fatalf("after connection teardown table must be empty, got %v", h.streams())
	}
}
