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

func mustCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (%v)", want, st.Code(), err)
	}
}

func mustErrIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("expected error wrapping %v, got %T: %v", target, err, err)
	}
}

// --- unary ---------------------------------------------------------------

// TestStateUnaryContract pins down the unary lifecycle: exact return
// value, single stream create/delete pair on the client, no server-side
// stream registration, and protocol behavior for stray frames.
func TestStateUnaryContract(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		var req, resp internal.EchoPayload
		req.Seq = 7
		req.Msg = "ping"
		cur := 0
		if err := h.client.Call(context.Background(), svcState, mUnary, &req, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Seq != 8 || resp.Msg != "ping" {
			t.Fatalf("unexpected response seq=%d msg=%q", resp.Seq, resp.Msg)
		}

		// Client stream set must be empty after the call.
		if len(h.client.streams) != 0 {
			t.Fatalf("client stream map not empty: %v", h.client.streams)
		}

		// The lifecycle events must appear in order.
		var e hEvent
		e, cur = h.waitKind(cur, evCreated)
		if e.id != 1 {
			t.Fatalf("first stream id = %d, want 1", e.id)
		}
		e, cur = h.waitKind(cur, evDeleted)
		if e.id != 1 {
			t.Fatalf("deleted stream id = %d, want 1", e.id)
		}

		// The server must never register a streamHandler for a unary call.
		h.sink.assertAbsent(t, func(e hEvent) bool { return e.kind == evReg })
	})

	t.Run("server error returns status", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mErrorChat, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer stream.CloseSend()
		if err := stream.SendMsg(&internal.EchoPayload{Seq: 5}); err != nil {
			t.Fatal(err)
		}
		var resp internal.EchoPayload
		err = stream.RecvMsg(&resp)
		mustCode(t, err, codes.ResourceExhausted)
	})

	t.Run("transport error on the request write", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		// The request frame never reaches the server: its write fails with
		// a terminal transport error before delivery.
		h.wire.failNextWrite(errScriptedTransport)
		var req, resp internal.EchoPayload
		err := h.client.Call(context.Background(), svcState, mUnary, &req, &resp)
		if !errors.Is(err, errScriptedTransport) {
			t.Fatalf("expected injected transport error, got %v", err)
		}

		// The failed stream is created then cleaned up when the receive loop
		// exits after the connection close; create precedes deletion.
		cur := 0
		_, cur = h.waitKind(cur, evCreated)
		_, _ = h.waitKind(cur, evDeleted)

		// No request frame ever crossed the wire.
		if fs := framesFrom(h.wire, 1, messageTypeRequest); len(fs) != 0 {
			t.Fatalf("failed request was delivered: %d frames", len(fs))
		}
	})
}

// --- client streaming ----------------------------------------------------

func TestStateClientStreamingContract(t *testing.T) {
	t.Run("upload then half close returns aggregate", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		stream, err := h.client.NewStream(context.Background(), descCS, svcState, mUpload, nil)
		if err != nil {
			t.Fatal(err)
		}
		cur := 0
		_, cur = h.waitKind(cur, evCreated)
		e, cur := h.waitKind(cur, evReg)
		if e.id != 1 {
			t.Fatalf("server registered stream id %d, want 1", e.id)
		}

		for i := int64(1); i <= 3; i++ {
			if err := stream.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}

		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatal(err)
		}
		if resp.Seq != 6 {
			t.Fatalf("aggregate seq = %d, want 6", resp.Seq)
		}

		// Handler completion precedes map removal, which precedes client
		// stream removal, both tied to the same stream id.
		var ev hEvent
		ev, cur = h.waitKind(cur, evHandler)
		if ev.id != 1 {
			t.Fatalf("handler-done id = %d", ev.id)
		}
		ev, cur = h.waitKind(cur, evUnreg)
		if ev.id != 1 {
			t.Fatalf("unreg id = %d", ev.id)
		}
		ev, cur = h.waitKind(cur, evDeleted)
		if ev.id != 1 || ev.from != "client" {
			t.Fatalf("client deleted id = %d from %s", ev.id, ev.from)
		}

		// The server-side terminal response was a single response frame.
		resps := framesFrom(h.remote, 1, messageTypeResponse)
		if len(resps) != 1 {
			t.Fatalf("expected 1 response frame, got %d", len(resps))
		}

		// A second RecvMsg returns io.EOF deterministically.
		err = stream.RecvMsg(&resp)
		mustErrIs(t, err, io.EOF)
	})

	t.Run("cannot close or send on non streaming client", func(t *testing.T) {
		h := newStateHarness(t)
		defer h.cleanup()

		stream, err := h.client.NewStream(context.Background(), descSS, svcState, mDownload, &internal.EchoPayload{Seq: 1})
		if err != nil {
			t.Fatal(err)
		}
		mustErrIs(t, stream.CloseSend(), ErrProtocol)
		mustErrIs(t, stream.SendMsg(&internal.EchoPayload{Seq: 1}), ErrProtocol)

		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatal(err)
		}
		if resp.Seq != 1 {
			t.Fatalf("first streamed item seq = %d", resp.Seq)
		}
	})
}

// --- server streaming ----------------------------------------------------

func TestStateServerStreamingContract(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descSS, svcState, mDownload, &internal.EchoPayload{Seq: 3, Msg: "d"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.CloseSend()

	cur := 0
	_, cur = h.waitKind(cur, evCreated)
	_, cur = h.waitKind(cur, evReg)

	for i := int64(1); i <= 3; i++ {
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		if resp.Seq != i || resp.Msg != "d" {
			t.Fatalf("item %d = seq %d msg %q", i, resp.Seq, resp.Msg)
		}
	}

	var resp internal.EchoPayload
	mustErrIs(t, stream.RecvMsg(&resp), io.EOF)
	// Repeated terminal reads keep returning io.EOF (remoteClosed).
	mustErrIs(t, stream.RecvMsg(&resp), io.EOF)

	// Terminal data frame had RemoteClosed|NoData; cleanup happened once.
	var ev hEvent
	ev, cur = h.waitKind(cur, evHandler)
	ev, cur = h.waitKind(cur, evUnreg)
	if ev.id != 1 {
		t.Fatalf("unreg id = %d", ev.id)
	}
	ev, _ = h.waitKind(cur, evDeleted)
	if ev.from != "client" || ev.id != 1 {
		t.Fatalf("client cleanup event = %+v", ev)
	}
}

// --- bidirectional -------------------------------------------------------

func TestStateBidiContract(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mChat, nil)
	if err != nil {
		t.Fatal(err)
	}
	cur := 0
	_, cur = h.waitKind(cur, evCreated)
	_, cur = h.waitKind(cur, evReg)

	for i := int64(1); i <= 5; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
			t.Fatal(err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatal(err)
		}
		if resp.Seq != i+1 {
			t.Fatalf("echo %d = %d", i, resp.Seq)
		}
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var resp internal.EchoPayload
	mustErrIs(t, stream.RecvMsg(&resp), io.EOF)

	_, cur = h.waitKind(cur, evHandler)
	_, cur = h.waitKind(cur, evUnreg)
	_, _ = h.waitKind(cur, evDeleted)

	// No orphans on either side during clean shutdown.
	h.sink.assertAbsent(t, func(e hEvent) bool {
		return e.kind == evOrphan || e.kind == evOrphanSrv
	})
}
