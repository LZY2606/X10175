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
	"io"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// Queue-full backpressure with cancellation on the client. The receive
// loop parks deterministically in stream.receive's slow path (evBlocked).
// A RecvMsg racing a cancelled context while a frame is immediately
// available must prefer the pending frame; afterwards connection teardown
// unblocks the parked delivery and terminates the loop.
func TestStateQueueFullCancelClient(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mBlockChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitEntered()

	// Exactly fill the client stream's recv buffer, then add one more so
	// the receive loop parks in the slow path.
	for i := 0; i < streamRecvBufferSize; i++ {
		injectData(h.remote, 1, 0, &internal.EchoPayload{Seq: int64(i)})
	}
	h.sink.waitFor(t, 0, func(e hEvent) bool {
		return e.kind == evFrame && e.id == 1 && e.msg == messageTypeData.String()
	})
	injectData(h.remote, 1, 0, &internal.EchoPayload{Seq: streamRecvBufferSize})
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evBlocked && e.id == 1 })

	// A cancelled call context must not mask an immediately available
	// frame (pending-message-first ordering on the client recv side).
	ready, cancel := context.WithCancel(context.Background())
	cancel()
	var first internal.EchoPayload
	if err := stream.RecvMsg(&first); err != nil {
		t.Fatalf("RecvMsg with buffered frame + canceled ctx: %v", err)
	}
	if first.Seq != 0 {
		t.Fatalf("first seq = %d, want 0", first.Seq)
	}
	_ = ready

	// Drain the remaining buffered frames in exact order while the
	// overflow frame is still parked in the receive loop.
	for i := 1; i < streamRecvBufferSize; i++ {
		var p internal.EchoPayload
		if err := stream.RecvMsg(&p); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
		if p.Seq != int64(i) {
			t.Fatalf("drain %d got seq %d", i, p.Seq)
		}
	}

	// Once the buffer drains, the parked overflow is delivered (seq=64).
	var overflow internal.EchoPayload
	if err := stream.RecvMsg(&overflow); err != nil {
		t.Fatalf("overflow frame: %v", err)
	}
	if overflow.Seq != streamRecvBufferSize {
		t.Fatalf("overflow seq = %d, want %d", overflow.Seq, streamRecvBufferSize)
	}

	// Now close the client: the next RecvMsg surfaces ErrClosed and the
	// receive loop terminates (loop-done is asserted by cleanup).
	if err := h.client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.wire.Close(); err != nil {
		t.Fatal(err)
	}
}

// Queue-full backpressure with cancellation on the server. The overflow
// frame parks in streamHandler.data's slow path; when the handler returns
// its context is cancelled, the parked frame fails delivery with an
// InvalidArgument status that races the terminal response. The terminal
// response is enqueued by the handler goroutine before the parked
// receive goroutine wakes, so the client observes response then orphan.
func TestStateQueueFullCancelServer(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descCS, svcState, mSlowUp, nil)
	if err != nil {
		t.Fatal(err)
	}
	cur := 0
	_, cur = h.waitKind(cur, evCreated)
	_, cur = h.waitKind(cur, evReg)

	if err := stream.SendMsg(&internal.EchoPayload{Seq: 41}); err != nil {
		t.Fatal(err)
	}
	h.waitEntered()

	// Fill recvBuf slots; the next send parks in the slow path.
	for i := 0; i < streamRecvBufferSize; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	overDone := make(chan struct{})
	go func() {
		defer close(overDone)
		if err := stream.SendMsg(&internal.EchoPayload{Seq: streamRecvBufferSize}); err != nil {
			t.Errorf("overflow send: %v", err)
		}
	}()
	h.sink.waitFor(t, 0, func(e hEvent) bool {
		return e.kind == evBlocked && e.from == "server"
	})

	// Releasing the handler deterministically wakes the parked receive
	// goroutine via ctx cancellation after the terminal response was
	// already queued.
	h.releaseBlock()
	select {
	case <-overDone:
	case <-time.After(5 * time.Second):
		t.Fatal("overflow send never returned")
	}

	var resp internal.EchoPayload
	if err := stream.RecvMsg(&resp); err != nil {
		t.Fatalf("terminal response: %v", err)
	}
	if resp.Seq != 41 {
		t.Fatalf("terminal seq = %d, want 41", resp.Seq)
	}

	// The late InvalidArgument status lands on an already-deleted client
	// stream and is rejected as an orphan, but the connection survives.
	h.sink.waitFor(t, 0, func(e hEvent) bool {
		return e.kind == evOrphan && e.from == "client" && e.id == 1
	})
	h.sink.assertAbsent(t, func(e hEvent) bool { return e.kind == evLoopDone })

	stream2, err := h.client.NewStream(context.Background(), descCS, svcState, mUpload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream2.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var r2 internal.EchoPayload
	if err := stream2.RecvMsg(&r2); err != nil {
		t.Fatal(err)
	}
	if r2.Seq != 0 {
		t.Fatalf("fresh stream seq = %d, want 0", r2.Seq)
	}
}

// Handler return racing server Shutdown: an active connection must keep
// Shutdown blocked while its terminal response is in flight; once the
// frame drains and the connection goes idle, Shutdown closes it and
// returns nil.
func TestStateHandlerReturnRacesShutdown(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mBlockChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitEntered()

	h.remote.holdNextWrites(1)
	h.releaseBlock()
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evGateHold && e.from == "server" })

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- h.server.Shutdown(context.Background()) }()

	// Pure happens-before check: the frame is still parked in the gate.
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned while terminal frame was held: %v", err)
	default:
	}
	if got := h.server.countConnection(); got != 1 {
		t.Fatalf("connection count during drain = %d, want 1", got)
	}

	if !h.remote.releaseWrite() {
		t.Fatal("no held frame released")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after terminal frame drained")
	}
	if got := h.server.countConnection(); got != 0 {
		t.Fatalf("connection count after shutdown = %d, want 0", got)
	}

	var resp internal.EchoPayload
	if err := stream.RecvMsg(&resp); err != io.EOF {
		t.Fatalf("terminal RecvMsg = %v, want io.EOF", err)
	}
}

// Client disconnect: server receive loop terminates on EOF, run() exits
// and removes the connection; the parked handler's respond observes
// shutdown and fails harmlessly.
func TestStateClientDisconnectServerCleanup(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()
	h.expectServerStreams = true

	stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mBlockChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitEntered()

	if err := h.client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.wire.Close(); err != nil {
		t.Fatal(err)
	}

	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evRecvDone })
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evConnDone })
	if got := h.server.countConnection(); got != 0 {
		t.Fatalf("server retained disconnected connection: %d", got)
	}

	// Releasing the parked handler now lets it finish against a torn-down
	// connection run; respond returns ErrClosed via the `done` channel.
	h.releaseBlock()
}
