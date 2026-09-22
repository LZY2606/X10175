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

// TestSMServerUnaryOK replays a unary request through the real server
// connection loop and asserts: handler registration happens before the
// response is written, the response carries OK and the echo payload, the
// unary stream never enters the stream set, and the active counter is 0
// once the response is sent.
func TestSMServerUnaryOK(t *testing.T) {
	h := newServerSMHarness(t)
	reg := h.expectRegistration(1)
	h.start(&ServiceDesc{
		Methods: map[string]Method{
			"Unary": unaryMethod(func(_ context.Context, req *internal.TestPayload) (*internal.TestPayload, error) {
				return &internal.TestPayload{Foo: req.Foo + "!"}, nil
			}),
		},
	})

	h.conn.enqueue(requestFrame(t, 1, 0, "Unary", &internal.TestPayload{Foo: "hi"}))
	// Unary methods never register a streamHandler: expect no signal.
	select {
	case <-reg:
		t.Fatal("unary request unexpectedly registered a streaming handler")
	default:
	}
	f, resp := h.expectResponse()
	if f.streamID() != 1 {
		t.Fatalf("response sid=%d want 1", f.streamID())
	}
	if resp.Status.GetCode() != int32(codes.OK) {
		t.Fatalf("status=%s", codes.Code(resp.Status.GetCode()))
	}
	var out internal.TestPayload
	unmarshalTest(t, resp.Payload, &out)
	if out.Foo != "hi!" {
		t.Fatalf("payload=%q", out.Foo)
	}
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d want 0 after unary response", h.streamCount())
	}
	if h.activeCount() != 0 {
		t.Fatalf("active=%d want 0 after unary response", h.activeCount())
	}
}

// TestSMServerUnaryStatus verifies a handler-returned grpc status is sent
// verbatim on the response frame and tears down the stream accounting.
func TestSMServerUnaryStatus(t *testing.T) {
	h := newServerSMHarness(t)
	h.start(&ServiceDesc{
		Methods: map[string]Method{
			"Unary": unaryMethod(func(context.Context, *internal.TestPayload) (*internal.TestPayload, error) {
				return nil, statusErr(codes.NotFound, "missing")
			}),
		},
	})
	h.conn.enqueue(requestFrame(t, 1, 0, "Unary", &internal.TestPayload{}))
	_, resp := h.expectResponse()
	if resp.Status.GetCode() != int32(codes.NotFound) || resp.Status.GetMessage() != "missing" {
		t.Fatalf("status=%+v", resp.Status)
	}
	if h.streamCount() != 0 || h.activeCount() != 0 {
		t.Fatalf("counts after status: streams=%d active=%d", h.streamCount(), h.activeCount())
	}
}

// TestSMServerStreamingSequence covers server-streaming: one request,
// multiple data frames, then the terminal RC|NoData data frame emitted by
// the handler's return. The stream stays registered until the terminal
// frame and is deleted exactly then. The handler is gated so the mid-stream
// state is observable without relying on scheduling.
func TestSMServerStreamingSequence(t *testing.T) {
	h := newServerSMHarness(t)
	reg := h.expectRegistration(3)
	proceedSecond := make(chan struct{})
	h.start(streamDesc("Watch", false, true, func(_ context.Context, ss StreamServer) (any, error) {
		if err := ss.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
			return nil, err
		}
		<-proceedSecond
		if err := ss.SendMsg(&internal.EchoPayload{Seq: 2}); err != nil {
			return nil, err
		}
		return nil, nil
	}))

	h.conn.enqueue(requestFrame(t, 3, flagRemoteClosed, "Watch", &internal.TestPayload{}))
	_ = waitHandler(t, reg)

	d1 := h.expectData()
	if d1.streamID() != 3 || d1.flags() != 0 {
		t.Fatalf("data1=%+v", d1.header)
	}
	// Parked between the two data frames: the stream is still registered.
	if h.streamCount() != 1 {
		t.Fatalf("stream set=%d want 1 mid-stream", h.streamCount())
	}
	if h.activeCount() != 1 {
		t.Fatalf("active=%d want 1 mid-stream", h.activeCount())
	}

	close(proceedSecond)
	d2 := h.expectData()
	if d2.flags() != 0 {
		t.Fatalf("data2 flags=%#x", d2.flags())
	}
	fin := h.expectData()
	if fin.flags() != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal data flags=%#x", fin.flags())
	}
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d want 0 after terminal frame", h.streamCount())
	}
	if h.activeCount() != 0 {
		t.Fatalf("active=%d want 0 after terminal frame", h.activeCount())
	}
}

// TestSMServerBidiHalfCloseAndLateFrame drives a bidi stream: request,
// inbound data, a RC close frame (handler drains to EOF), and then asserts
// a late data frame after the handler's terminal frame is rejected with
// the precise "StreamID is no longer active" status.
func TestSMServerBidiHalfCloseAndLateFrame(t *testing.T) {
	h := newServerSMHarness(t)
	reg := h.expectRegistration(5)
	sawEOF := make(chan struct{})
	h.start(streamDesc("Bidi", true, true, func(_ context.Context, ss StreamServer) (any, error) {
		for {
			var p internal.EchoPayload
			if err := ss.RecvMsg(&p); err != nil {
				if errors.Is(err, io.EOF) {
					close(sawEOF)
					return nil, nil
				}
				return nil, err
			}
		}
	}))

	h.conn.enqueue(requestFrame(t, 5, flagRemoteOpen, "Bidi", nil))
	sh := waitHandler(t, reg)
	if sh.remoteClosed {
		t.Fatal("handler remoteClosed set before RC frame")
	}

	h.conn.enqueue(dataFrame(t, 5, 0, &internal.EchoPayload{Seq: 1}))
	h.conn.enqueue(dataFrame(t, 5, flagRemoteClosed|flagNoData, nil))
	waitSignal(t, sawEOF, "handler to observe EOF after RC frame")
	if !sh.remoteClosed {
		t.Fatal("handler remoteClosed not set by close frame")
	}

	// Terminal frame (handler returned nil).
	fin := h.expectData()
	if fin.flags() != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal flags=%#x", fin.flags())
	}
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d want 0 after terminal frame", h.streamCount())
	}

	// A late data frame on the finished id is rejected with a specific
	// status and does not re-create the stream.
	h.conn.enqueue(dataFrame(t, 5, 0, &internal.EchoPayload{Seq: 2}))
	_, resp := h.expectResponse()
	if resp.Status.GetCode() != int32(codes.InvalidArgument) ||
		resp.Status.GetMessage() != "StreamID is no longer active" {
		t.Fatalf("late-frame status=%+v", resp.Status)
	}
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d must stay 0 after rejected late frame", h.streamCount())
	}
}

// TestSMServerMutationEarlyMapDelete is a mutation-oriented contract test.
//
// Contract: a bidi stream remains in the connection stream map until its
// terminal frame is sent. A data frame that merely carries the remote-closed
// half-close must NOT remove the stream - later data frames on that id must
// still be delivered to the same handler until the handler returns.
//
// A mutation that deletes the stream entry (or otherwise makes it
// unresolvable) when the RC half-close is observed causes the subsequent
// in-window data frame to be rejected with "StreamID is no longer active";
// this test fails because the handler must observe that data and finish
// normally.
func TestSMServerMutationEarlyMapDelete(t *testing.T) {
	h := newServerSMHarness(t)
	reg := h.expectRegistration(7)
	gotSecond := make(chan struct{})
	h.start(streamDesc("Bidi", true, true, func(_ context.Context, ss StreamServer) (any, error) {
		var first internal.EchoPayload
		if err := ss.RecvMsg(&first); err != nil {
			return nil, err
		}
		if first.Seq != 1 {
			t.Fatalf("first seq=%d want 1", first.Seq)
		}
		var second internal.EchoPayload
		if err := ss.RecvMsg(&second); err != nil {
			return nil, err
		}
		if second.Seq != 2 {
			t.Fatalf("second seq=%d want 2", second.Seq)
		}
		close(gotSecond)
		// RC arrives after; drain to EOF.
		var third internal.EchoPayload
		if err := ss.RecvMsg(&third); err != io.EOF {
			return nil, err
		}
		return nil, nil
	}))

	h.conn.enqueue(requestFrame(t, 7, flagRemoteOpen, "Bidi", nil))
	_ = waitHandler(t, reg)

	// Two data frames arrive BEFORE the RC half-close. Production keeps
	// the stream resolvable across both; an early delete breaks the second.
	h.conn.enqueue(dataFrame(t, 7, 0, &internal.EchoPayload{Seq: 1}))
	h.conn.enqueue(dataFrame(t, 7, 0, &internal.EchoPayload{Seq: 2}))
	waitSignal(t, gotSecond, "handler consuming the second data frame")

	// Still registered at the half-close point.
	if h.streamCount() != 1 {
		t.Fatalf("stream set=%d want 1 before terminal frame", h.streamCount())
	}

	h.conn.enqueue(dataFrame(t, 7, flagRemoteClosed|flagNoData, nil))
	fin := h.expectData()
	if fin.flags() != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal flags=%#x", fin.flags())
	}
	if h.streamCount() != 0 {
		t.Fatalf("stream set=%d want 0 after terminal frame", h.streamCount())
	}
}

// TestSMServerStreamIDReuse verifies that a request with an id not strictly
// greater than the previous one is rejected with the exact reuse status and
// does not register a handler or touch the active counter.
func TestSMServerStreamIDReuse(t *testing.T) {
	h := newServerSMHarness(t)
	reg := h.expectRegistration(9)
	h.start(&ServiceDesc{
		Methods: map[string]Method{
			"Unary": unaryMethod(func(_ context.Context, _ *internal.TestPayload) (*internal.TestPayload, error) {
				return &internal.TestPayload{}, nil
			}),
		},
	})

	h.conn.enqueue(requestFrame(t, 9, 0, "Unary", &internal.TestPayload{}))
	h.expectResponse()

	// Reuse a lower/equal id.
	h.conn.enqueue(requestFrame(t, 9, 0, "Unary", &internal.TestPayload{}))
	_, resp := h.expectResponse()
	if resp.Status.GetCode() != int32(codes.InvalidArgument) ||
		resp.Status.GetMessage() != "StreamID cannot be re-used and must increment" {
		t.Fatalf("reuse status=%+v", resp.Status)
	}

	// An even id is rejected for a different reason (id parity), and no
	// handler is registered.
	h.conn.enqueue(requestFrame(t, 8, 0, "Unary", &internal.TestPayload{}))
	_, resp2 := h.expectResponse()
	if resp2.Status.GetCode() != int32(codes.InvalidArgument) ||
		resp2.Status.GetMessage() != "StreamID must be odd for client initiated streams" {
		t.Fatalf("parity status=%+v", resp2.Status)
	}

	select {
	case <-reg:
		t.Fatal("no streaming handler should be registered for unary requests")
	default:
	}
	if h.streamCount() != 0 || h.activeCount() != 0 {
		t.Fatalf("counts after rejections: streams=%d active=%d", h.streamCount(), h.activeCount())
	}
}
