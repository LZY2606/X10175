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
)

// TestStateCancelWhileReceiveQueueFull fills the client-side receive buffer
// of a server-streaming stream (capacity streamRecvBufferSize) so the
// receive loop parks in its backpressure slow path, then cancels the whole
// connection. The contract: cancellation unblocks the receive loop, the
// connection tears down exactly once, buffered messages remain consumable in
// order, and the terminal error after draining is ErrClosed.
func TestStateCancelWhileReceiveQueueFull(t *testing.T) {
	h := newClientHarness(t)

	cs, open := openClientStream(t, h, &StreamDesc{StreamingServer: true}, "ServerStream")
	sid := open.header.StreamID

	// Fill the buffer and park the receive loop on the (N+1)-th frame.
	for i := int64(1); i <= streamRecvBufferSize+1; i++ {
		h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: i}, 0))
	}
	h.log.waitEvent(t, evClientBlocked)
	if !h.hasStream(streamID(sid)) {
		t.Fatal("stream must stay registered while the receive loop is parked")
	}

	// Cancel the connection: the parked slow-path delivery observes
	// ctx.Done and the receive loop terminates.
	h.cl.Close()
	h.log.waitEvent(t, evClientRun)
	if got := len(h.streams()); got != 0 {
		t.Fatalf("stream table must be swept on connection close, got %v", h.streams())
	}

	// Buffered messages are still delivered in order; the (N+1)-th frame
	// was dropped by the canceled delivery.
	var got internal.EchoPayload
	for i := int64(1); i <= streamRecvBufferSize; i++ {
		if err := cs.RecvMsg(&got); err != nil {
			t.Fatalf("RecvMsg %d after close: %v", i, err)
		}
		if got.Seq != i {
			t.Fatalf("RecvMsg %d seq = %d", i, got.Seq)
		}
	}
	if err := cs.RecvMsg(&got); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecvMsg after draining err = %v, want ErrClosed", err)
	}
}

// TestStateStreamRecvMsgContextCanceled pins the deterministic corner of the
// ctx-cancel race: with nothing buffered and the stream still open, a
// canceled stream context is the only ready case and RecvMsg must return
// the context error.
func TestStateStreamRecvMsgContextCanceled(t *testing.T) {
	h := newClientHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := h.cl.NewStream(ctx, &StreamDesc{StreamingServer: true}, stateTestService, "ServerStream", nil)
		streamCh <- streamResult{cs, err}
	}()
	h.peer.recv()
	res := <-streamCh
	if res.err != nil {
		t.Fatalf("NewStream: %v", res.err)
	}

	cancel()
	var got internal.EchoPayload
	if err := res.cs.RecvMsg(&got); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecvMsg err = %v, want context.Canceled", err)
	}
}

// TestStateStreamCancelVsBufferedStatus documents the genuinely simultaneous
// case: a final status is buffered, the stream context is canceled and the
// stream is closed, all before RecvMsg runs. The implementation's select
// does not deterministically prefer one ready case, so the contract is an
// explicit outcome set: the buffered status payload, context.Canceled, or
// the close error - and never any other error. The connection must stay
// healthy regardless of which outcome is observed.
func TestStateStreamCancelVsBufferedStatus(t *testing.T) {
	h := newClientHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := h.cl.NewStream(ctx, &StreamDesc{StreamingServer: true}, stateTestService, "ServerStream", nil)
		streamCh <- streamResult{cs, err}
	}()
	open := h.peer.recv()
	res := <-streamCh
	if res.err != nil {
		t.Fatalf("NewStream: %v", res.err)
	}
	cs := res.cs
	sid := open.header.StreamID

	h.peer.send(dataFrame(sid, &internal.EchoPayload{Seq: 1, Msg: "buffered"}, 0))
	h.waitRecvBuffer(t, streamID(sid), 1)
	cancel()
	h.getStreamForTest(streamID(sid)).closeWithError(ErrClosed)

	var got internal.EchoPayload
	err := cs.RecvMsg(&got)
	switch {
	case err == nil:
		if got.Seq != 1 || got.Msg != "buffered" {
			t.Fatalf("buffered outcome corrupted: seq=%d msg=%q", got.Seq, got.Msg)
		}
	case errors.Is(err, context.Canceled), errors.Is(err, ErrClosed):
		// Allowed: the cancel and the close were both ready.
	default:
		t.Fatalf("RecvMsg err = %v, outside the allowed outcome set", err)
	}

	// The connection survives any of the allowed outcomes.
	callErr := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		callErr <- h.cl.Call(context.Background(), stateTestService, "Echo", &internal.EchoPayload{}, &resp)
	}()
	req := h.peer.recv()
	h.peer.send(responseFrame(t, req.header.StreamID, nil, &internal.EchoPayload{}))
	if err := waitErr(t, callErr); err != nil {
		t.Fatalf("connection unhealthy after simultaneous cancel/close: %v", err)
	}
}
