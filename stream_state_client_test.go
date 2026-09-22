/*
   Copyright The containerd Authors.
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

// ---------------------------------------------------------------------------
// unary
// ---------------------------------------------------------------------------

// TestSMUnaryOK replays a complete unary request/response and asserts the
// exact outbound request, the decoded response payload and the removal of
// the ephemeral stream from the client stream set.
func TestSMUnaryOK(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()

	req := &internal.TestPayload{Foo: "ping"}
	var resp internal.TestPayload
	done := h.callAsync(ctx, "Unary", req, &resp)

	reqf := h.conn.nextWrite(t)
	if reqf.typ() != messageTypeRequest {
		t.Fatalf("got %s, want request", reqf.typ())
	}
	dreq := decodeRequest(t, reqf)
	if dreq.Method != "Unary" {
		t.Fatalf("method=%q", dreq.Method)
	}
	if dreq.Payload == nil {
		t.Fatal("request payload missing")
	}
	sid := reqf.streamID()
	if sid%2 != 1 {
		t.Fatalf("stream id must be odd, got %d", sid)
	}
	if got := h.streamCount(); got != 1 {
		t.Fatalf("stream set size=%d, want 1", got)
	}

	h.conn.enqueue(responseFrame(t, sid, codes.OK, "", &internal.TestPayload{Foo: "pong"}))

	if err := waitErr(t, done, "unary call"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Foo != "pong" {
		t.Fatalf("response payload=%q", resp.Foo)
	}
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set size after completion=%d, want 0", got)
	}
}

// TestSMUnaryNonOKStatus verifies that a terminal non-OK status is
// surfaced verbatim and cleans up the stream.
func TestSMUnaryNonOKStatus(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()

	done := h.callAsync(ctx, "Unary", &internal.TestPayload{}, &internal.TestPayload{})
	reqf := h.conn.nextWrite(t)
	sid := reqf.streamID()

	h.conn.enqueue(responseFrame(t, sid, codes.PermissionDenied, "nope", nil))

	err := waitErr(t, done, "unary call")
	assertStatusCode(t, err, codes.PermissionDenied)
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set size=%d, want 0", got)
	}
}

// TestSMUnaryTerminalTransportError verifies that a terminal non-status
// read error terminates the receive loop, closes the client, fails the
// outstanding call with ErrClosed and empties the stream set. Compare
// with TestSMUnaryServerStatusError below, where a grpc status only fails
// the affected stream and leaves the connection alive.
func TestSMUnaryTerminalTransportError(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()

	done := h.callAsync(ctx, "Unary", &internal.TestPayload{}, &internal.TestPayload{})
	h.conn.nextWrite(t)

	if got := h.streamCount(); got != 1 {
		t.Fatalf("stream set size=%d, want 1", got)
	}
	h.conn.failReads(errors.New("wire severed deterministically"))

	err := waitErr(t, done, "unary call")
	assertExactError(t, err, ErrClosed)
	waitClosed(t, h.runDone(), "client run loop")
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set size after transport error=%d, want 0", got)
	}
}

// TestSMUnaryEOFCloses verifies that io.EOF from the transport is mapped
// to ErrClosed (rather than surfaced as EOF).
func TestSMUnaryEOFCloses(t *testing.T) {
	h := newClientSMHarness(t)
	ctx := context.Background()

	done := h.callAsync(ctx, "Unary", &internal.TestPayload{}, &internal.TestPayload{})
	h.conn.nextWrite(t)
	h.conn.failReads(io.EOF)

	err := waitErr(t, done, "unary call")
	assertExactError(t, err, ErrClosed)
	waitClosed(t, h.runDone(), "client run loop")
}

// TestSMUnaryCancelBeforeResponse verifies that cancelling the call
// context while waiting for the response returns context.Canceled and that
// the unary stream is reaped immediately (dispatch owns the stream and
// deletes it on return). A late response for the removed stream must be
// dropped without disturbing the connection.
func TestSMUnaryCancelBeforeResponse(t *testing.T) {
	h := newClientSMHarness(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := h.callAsync(ctx, "Unary", &internal.TestPayload{}, &internal.TestPayload{})
	reqf := h.conn.nextWrite(t)
	sid := reqf.streamID()

	cancel()
	if err := waitErr(t, done, "canceled unary call"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set size=%d, want 0", got)
	}

	// Late response for the removed stream must be ignored.
	h.conn.enqueue(responseFrame(t, sid, codes.OK, "", &internal.TestPayload{}))

	// A fresh unary call must still work and must not reuse the old id.
	var resp2 internal.TestPayload
	done2 := h.callAsync(context.Background(), "Unary", &internal.TestPayload{}, &resp2)
	reqf2 := h.conn.nextWrite(t)
	if reqf2.streamID() == sid {
		t.Fatal("new unary call reused the canceled stream id")
	}
	h.conn.enqueue(responseFrame(t, reqf2.streamID(), codes.OK, "", &internal.TestPayload{Foo: "second"}))
	if err := waitErr(t, done2, "second call"); err != nil {
		t.Fatalf("second call after late frame: %v", err)
	}
	if resp2.Foo != "second" {
		t.Fatalf("resp2=%q", resp2.Foo)
	}
	if got := h.streamCount(); got != 0 {
		t.Fatalf("stream set size=%d, want 0", got)
	}
}
