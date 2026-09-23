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

// ---------------------------------------------------------------------------
// Unary
// ---------------------------------------------------------------------------

// TestContractUnary replays a unary RPC and asserts the exact two-frame
// wire sequence, response value, status code, and the precise point at
// which both stream sets are cleaned up.
func TestContractUnary(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	ctx := context.Background()
	var resp internal.EchoPayload
	err := h.client.Call(ctx, contractService, methodUnary, &internal.EchoPayload{Seq: 40, Msg: "u"}, &resp)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Seq != 41 || resp.Msg != "u" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// A successfully dispatched stream is registered before its handler
	// runs and unregistered exactly when the final response is written.
	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: 0},
		{dir: "s2c", typ: messageTypeResponse, sid: 1, hasPayload: true},
	})
	lc := h.waitLifecycle(2)
	if lc[0] != (streamLifecycle{"added", 1}) || lc[1] != (streamLifecycle{"removed", 1}) {
		t.Fatalf("unexpected server lifecycle: %+v", lc)
	}
	if h.serverStreamCount() != 0 || h.clientStreamCount() != 0 {
		t.Fatalf("streams not cleaned up: server=%d client=%d", h.serverStreamCount(), h.clientStreamCount())
	}
}

// TestContractUnaryErrorStatus verifies that a non-OK grpc status is the
// observable result and that the stream is still fully cleaned up.
func TestContractUnaryErrorStatus(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	ctx := context.Background()
	var resp internal.EchoPayload
	err := h.client.Call(ctx, contractService, "DoesNotExist", &internal.EchoPayload{}, &resp)
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: 0},
		{dir: "s2c", typ: messageTypeResponse, sid: 1, hasPayload: true},
	})
	lc := h.waitLifecycle(1)
	if lc[0] != (streamLifecycle{"removed", 1}) {
		t.Fatalf("unexpected server lifecycle: %+v", lc)
	}
	if h.serverStreamCount() != 0 || h.clientStreamCount() != 0 {
		t.Fatalf("streams not cleaned up: server=%d client=%d", h.serverStreamCount(), h.clientStreamCount())
	}
}

// ---------------------------------------------------------------------------
// Client streaming
// ---------------------------------------------------------------------------

// TestContractClientStreaming replays open -> data -> data -> half-close ->
// final response and asserts every frame, the counted result, and cleanup.
func TestContractClientStreaming(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, contractService, methodC2S, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteOpen},
	})
	lc := h.waitLifecycle(1)
	if lc[0] != (streamLifecycle{"added", 1}) {
		t.Fatalf("want added, got %+v", lc)
	}

	for i := range 2 {
		if err := cs.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "x"}); err != nil {
			t.Fatalf("SendMsg %d: %v", i, err)
		}
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	h.assertWiresPrefix([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteOpen},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: flagRemoteClosed | flagNoData, emptyPayload: true},
	})

	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if resp.Seq != 2 {
		t.Fatalf("count = %d, want 2", resp.Seq)
	}

	// C2S server is non-streaming: the final result arrives as a Response.
	final := h.waitWire(4)
	if final.hdr.Type != messageTypeResponse {
		t.Fatalf("final frame type = %s, want response", final.hdr.Type)
	}
	if got := statusOf(decodeResponse(t, final)); got.Code() != codes.OK {
		t.Fatalf("final status = %v, want OK", got)
	}
	// Subsequent RecvMsg observes EOF (remote closed latched client-side).
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("second RecvMsg = %v, want io.EOF", err)
	}
	lc = h.waitLifecycle(2)
	if lc[1] != (streamLifecycle{"removed", 1}) {
		t.Fatalf("want removed, got %+v", lc)
	}
	if h.serverStreamCount() != 0 || h.clientStreamCount() != 0 {
		t.Fatalf("streams not cleaned up: server=%d client=%d", h.serverStreamCount(), h.clientStreamCount())
	}
}

// TestContractDoubleCloseSend asserts the exact error of two consecutive
// CloseSend calls and that only one half-close frame hits the wire.
func TestContractDoubleCloseSend(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, contractService, methodC2S, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	h.waitWires(1)

	if err := cs.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	// The second CloseSend must report the specific stream-closed error and
	// must not send another frame.
	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend = %v, want ErrStreamClosed", err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendMsg after close = %v, want ErrStreamClosed", err)
	}

	wires := h.waitWires(2) // request + one half-close
	if n := h.log.wireCount(); n != 2 {
		t.Fatalf("wire count = %d, want 2: %s", n, formatWires(wires))
	}
	if wires[1].hdr.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("half-close flags = 0x%x", wires[1].hdr.Flags)
	}

	// The stream still completes normally server-side.
	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
}

// TestContractCloseSendOnNonStreamingClient verifies CloseSend is rejected
// for a non-streaming client with the wrapped protocol error.
func TestContractCloseSendOnNonStreamingClient(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{}, contractService, methodS2C, &internal.EchoPayload{Seq: 1})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() {
		var out internal.EchoPayload
		_ = cs.RecvMsg(&out)
		_ = cs.RecvMsg(&out)
	}()
	h.waitWires(1)

	if err := cs.CloseSend(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("CloseSend = %v, want ErrProtocol", err)
	}
}

// ---------------------------------------------------------------------------
// Server streaming
// ---------------------------------------------------------------------------

// TestContractServerStreaming asserts data frames precede the terminal
// empty data frame carrying the final status, and that RecvMsg returns EOF
// at exactly that point.
func TestContractServerStreaming(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingServer: true}, contractService, methodS2C,
		&internal.EchoPayload{Seq: 2})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// Request already carries the closed-from-client semantics.
	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteClosed, hasPayload: true},
	})

	for i := int64(1); i <= 2; i++ {
		var out internal.EchoPayload
		if err := cs.RecvMsg(&out); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if out.Seq != i || out.Msg != "data" {
			t.Fatalf("data %d = %+v", i, out)
		}
	}
	// The third message is the final payload inside an RC data frame.
	var final internal.EchoPayload
	if err := cs.RecvMsg(&final); err != nil {
		t.Fatalf("final RecvMsg: %v", err)
	}
	if final.Msg != "done" || final.Seq != 2 {
		t.Fatalf("final = %+v", final)
	}
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("post-final RecvMsg = %v, want io.EOF", err)
	}
	// EOF is sticky.
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("second post-final RecvMsg = %v, want io.EOF", err)
	}

	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteClosed, hasPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: flagRemoteClosed, hasPayload: true},
	})
	lc := h.waitLifecycle(2)
	if lc[0] != (streamLifecycle{"added", 1}) || lc[1] != (streamLifecycle{"removed", 1}) {
		t.Fatalf("lifecycle = %+v", lc)
	}
	if h.serverStreamCount() != 0 || h.clientStreamCount() != 0 {
		t.Fatalf("streams not cleaned up: server=%d client=%d", h.serverStreamCount(), h.clientStreamCount())
	}
}

// TestContractHalfCloseWithoutData opens a client-streaming RPC and sends
// the half-close without any preceding data; the handler must observe EOF
// and produce its response.
func TestContractHalfCloseWithoutData(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, contractService, methodC2S, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	h.waitWires(1)
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if resp.Seq != 0 {
		t.Fatalf("count = %d, want 0", resp.Seq)
	}
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("post-final RecvMsg = %v, want io.EOF", err)
	}

	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteOpen, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: flagRemoteClosed | flagNoData, emptyPayload: true},
		{dir: "s2c", typ: messageTypeResponse, sid: 1, hasPayload: true},
	})
}

// ---------------------------------------------------------------------------
// Bidirectional streaming
// ---------------------------------------------------------------------------

// TestContractBidiStreaming replays interleaved data in both directions
// followed by client half-close and server final response.
func TestContractBidiStreaming(t *testing.T) {
	h := newContractHarness(t)
	defer h.shutdown()

	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true},
		contractService, methodBidi, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteOpen, hasPayload: true},
	})

	for i := range 2 {
		if err := cs.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "hi"}); err != nil {
			t.Fatalf("SendMsg %d: %v", i, err)
		}
		var echo internal.EchoPayload
		if err := cs.RecvMsg(&echo); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if echo.Msg != "echo:hi" || echo.Seq != int64(i) {
			t.Fatalf("echo %d = %+v", i, echo)
		}
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	// Final server response (count of received messages).
	var final internal.EchoPayload
	if err := cs.RecvMsg(&final); err != nil {
		t.Fatalf("final RecvMsg: %v", err)
	}
	if final.Seq != 2 {
		t.Fatalf("final count = %d, want 2", final.Seq)
	}
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("post-final RecvMsg = %v, want io.EOF", err)
	}

	h.assertWiresExact([]wireExpect{
		{dir: "c2s", typ: messageTypeRequest, sid: 1, flags: flagRemoteOpen, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: 0, hasPayload: true},
		{dir: "c2s", typ: messageTypeData, sid: 1, flags: flagRemoteClosed | flagNoData, emptyPayload: true},
		{dir: "s2c", typ: messageTypeData, sid: 1, flags: flagRemoteClosed, hasPayload: true},
	})
	lc := h.waitLifecycle(2)
	if lc[0] != (streamLifecycle{"added", 1}) || lc[1] != (streamLifecycle{"removed", 1}) {
		t.Fatalf("lifecycle = %+v", lc)
	}
}
