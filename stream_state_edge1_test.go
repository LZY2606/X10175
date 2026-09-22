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
)

// Two consecutive CloseSend calls: the first sends one half-close frame
// and flips localClosed; the second must be rejected locally as
// ErrStreamClosed without any second wire frame or error relaxation.
func TestStateCloseSendTwice(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	before := len(framesFrom(h.wire, 1, messageTypeData))
	err = stream.CloseSend()
	mustErrIs(t, err, ErrStreamClosed)
	after := len(framesFrom(h.wire, 1, messageTypeData))
	if before != after {
		t.Fatalf("second CloseSend emitted %d wire frame(s)", after-before)
	}

	// Sending after a successful half-close is also ErrStreamClosed.
	mustErrIs(t, stream.SendMsg(&internal.EchoPayload{Seq: 2}), ErrStreamClosed)

	// Drain terminal EOF and cleanup so teardown is quiescent.
	var resp internal.EchoPayload
	mustErrIs(t, stream.RecvMsg(&resp), io.EOF)
}

// When the half-close write itself fails, localClosed must already be
// observable as set only after the failure path: a following CloseSend
// may retry (the flag never flipped on the error path), and the exact
// transport error is preserved. This is mutation contract #1.
func TestStateCloseSendFlagOrderOnWriteError(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	csStream, err := h.client.NewStream(context.Background(), descBidi, svcState, mBlockChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := csStream.(*clientStream)
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	// Keep the handler alive so the stream stays registered server-side.
	select {
	case <-h.blockEntered:
	default:
	}

	h.wire.failNextWrite(errScriptedTransport)
	err = stream.CloseSend()
	if !errors.Is(err, errScriptedTransport) {
		t.Fatalf("first CloseSend error = %v, want injected error", err)
	}

	// localClosed must NOT have flipped on the failing path, otherwise a
	// retry would surface the uniform ErrStreamClosed and mask the cause.
	if stream.localClosed {
		t.Fatal("localClosed flipped before the failing write returned")
	}
	h.wire.failNextWrite(errScriptedTransport)
	err = stream.CloseSend()
	if !errors.Is(err, errScriptedTransport) {
		t.Fatalf("second CloseSend error = %v, want the same injected error (flag flipped too early)", err)
	}

	// Release and complete the stream cleanly.
	close(h.blockRelease)
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var resp internal.EchoPayload
	mustErrIs(t, stream.RecvMsg(&resp), io.EOF)
}

// Half-close without any preceding data frame. Both client-streaming and
// bidirectional handlers must observe the close as a single empty data
// frame with RemoteClosed|NoData and finish normally.
func TestStateHalfCloseNoData(t *testing.T) {
	t.Run("client streaming", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		stream, err := h.client.NewStream(context.Background(), descCS, svcState, mUpload, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("RecvMsg: %v", err)
		}
		if resp.Seq != 0 {
			t.Fatalf("aggregate = %d, want 0", resp.Seq)
		}

		// Exactly one data frame was sent: the empty half-close.
		dataFrames := framesFrom(h.wire, 1, messageTypeData)
		if len(dataFrames) != 1 {
			t.Fatalf("data frames = %d, want 1", len(dataFrames))
		}
		if dataFrames[0].header.Flags&flagRemoteClosed == 0 || dataFrames[0].header.Flags&flagNoData == 0 {
			t.Fatalf("half-close flags = %#x", dataFrames[0].header.Flags)
		}
		mustErrIs(t, stream.RecvMsg(&resp), io.EOF)
	})

	t.Run("bidirectional", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mChat, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		var resp internal.EchoPayload
		mustErrIs(t, stream.RecvMsg(&resp), io.EOF)

		// Server sends no data for a nil-returning bidi handler: just the
		// terminal empty data frame (RemoteClosed|NoData).
		srvFrames := framesFrom(h.remote, 1, messageTypeData)
		if len(srvFrames) != 1 {
			t.Fatalf("server data frames = %d, want 1", len(srvFrames))
		}
		if srvFrames[0].header.Flags&flagRemoteClosed == 0 || srvFrames[0].header.Flags&flagNoData == 0 {
			t.Fatalf("terminal frame flags = %#x", srvFrames[0].header.Flags)
		}
	})
}

// Frames arriving after the remote terminal status: until the client has
// deleted the stream they must still be delivered in order (the contract
// of the pending-message-first select); afterwards they are rejected as
// orphans and never crash the receive loop.
func TestStateExtraFramesAfterFinalStatus(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	// Unary call held by the server lets us inject a stray data frame
	// before the terminal response arrives, while the stream is live.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := h.client.NewStream(ctx, descBidi, svcState, mErrorChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}

	var resp internal.EchoPayload
	err = stream.RecvMsg(&resp)
	mustCode(t, err, codes.ResourceExhausted)

	// RecvMsg on a terminal response neither deleted the stream nor set
	// remoteClosed (that pair happens for normal streaming frames); the
	// stream must still be in the client set.
	if !h.clientHasStream(1) {
		t.Fatal("stream removed from client map immediately on status")
	}

	// An extra data frame delivered now must be buffered, not orphaned.
	injectData(h.remote, 1, 0, &internal.EchoPayload{Seq: 99, Msg: "late"})
	h.sink.waitFor(t, 0, func(e hEvent) bool {
		return e.kind == evFrame && e.from == "server" && e.id == 1 && e.mtype == messageTypeData
	})
	h.sink.assertAbsent(t, func(e hEvent) bool { return e.kind == evOrphan })
	var late internal.EchoPayload
	if err := stream.RecvMsg(&late); err != nil {
		t.Fatalf("extra frame before cleanup: %v", err)
	}
	if late.Seq != 99 {
		t.Fatalf("late frame payload = %d", late.Seq)
	}

	// Force client-side cleanup of the finished stream, then a further
	// frame must be rejected as an orphan and the loop stays alive.
	h.client.deleteStream(h.client.getStream(streamID(1)))
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evDeleted })
	injectData(h.remote, 1, 0, &internal.EchoPayload{Seq: 100})
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evOrphan && e.id == 1 })

	// Receive loop healthy: a second stream still works.
	stream2, err := h.client.NewStream(context.Background(), descBidi, svcState, mChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream2.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	var r2 internal.EchoPayload
	if err := stream2.RecvMsg(&r2); err != nil {
		t.Fatal(err)
	}
	if r2.Seq != 2 {
		t.Fatalf("post-orphan echo = %d", r2.Seq)
	}
	if err := stream2.CloseSend(); err != nil {
		t.Fatal(err)
	}
	mustErrIs(t, stream2.RecvMsg(&r2), io.EOF)
}
