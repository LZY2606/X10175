package ttrpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestContractUnaryLifecycle pins the unary state machine: request and
// response frames, stream-id allocation, and synchronous stream cleanup
// before Call returns.
func TestContractUnaryLifecycle(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	var resp internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 41}, &resp); err != nil {
		t.Fatalf("unary call: %v", err)
	}
	if resp.Seq != 42 {
		t.Fatalf("response seq %d, want 42", resp.Seq)
	}

	frames := h.log.waitCount(t, 2)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, 0, 1)
	contractAssertFrame(t, frames[1], "S2C", messageTypeResponse, 0, 1)
	contractAssertStatus(t, frames[1], codes.OK)

	// The stream is removed synchronously before Call returns.
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty after unary call: %d", n)
	}

	// Stream ids advance by two per call.
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &resp); err != nil {
		t.Fatalf("second unary call: %v", err)
	}
	frames = h.log.waitCount(t, 4)
	contractAssertFrame(t, frames[2], "C2S", messageTypeRequest, 0, 3)
	contractAssertFrame(t, frames[3], "S2C", messageTypeResponse, 0, 3)
	contractAssertStatus(t, frames[3], codes.OK)
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty after second call: %d", n)
	}
}

// TestContractUnaryNonOKStatus pins propagation of a server status code
// for a unary call, including cleanup on the error path.
func TestContractUnaryNonOKStatus(t *testing.T) {
	h := newContractHarness(t)

	err := h.client.Call(context.Background(), contractService, "Fail", &internal.EchoPayload{}, &internal.EchoPayload{})
	if err == nil {
		t.Fatal("expected error from Fail method")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("status code %v, want PermissionDenied", status.Code(err))
	}

	frames := h.log.waitCount(t, 2)
	contractAssertFrame(t, frames[1], "S2C", messageTypeResponse, 0, 1)
	contractAssertStatus(t, frames[1], codes.PermissionDenied)

	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty after failed call: %d", n)
	}
}

// TestContractUnaryCancelInFlight pins the observable result when the
// caller's context is canceled while the response is still on the wire:
// the cancellation wins because nothing has been delivered yet, the
// stream is removed synchronously, and the late response is dropped
// without poisoning the connection.
func TestContractUnaryCancelInFlight(t *testing.T) {
	h := newContractHarness(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Park the server's first write: the unary response.
	h.serverConn.holdNextWrite()

	callErr := contractGo(func() error {
		var resp internal.EchoPayload
		return h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &resp)
	})

	frames := h.log.waitCount(t, 1)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, 0, 1)
	h.serverConn.waitParked(t)
	h.waitClientStreamCount(1)

	// Cancel while the response is parked: the only observable outcome is
	// the context error, because no frame has reached the client.
	cancel()
	if err := contractAwait(t, callErr, "canceled unary call"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call: got %v, want context.Canceled", err)
	}
	// dispatch removes the stream synchronously on the cancel path.
	h.waitClientStreamCount(0)

	// Release the late response. The stream is gone, so the frame is
	// dropped and the connection stays healthy.
	h.serverConn.releaseWrite()
	var resp internal.EchoPayload
	if err := h.client.Call(context.Background(), contractService, "Echo", &internal.EchoPayload{Seq: 2}, &resp); err != nil {
		t.Fatalf("connection unhealthy after cancel: %v", err)
	}
	if resp.Seq != 3 {
		t.Fatalf("response seq %d, want 3", resp.Seq)
	}
}

// TestContractServerSideTeardown pins the observable errors when the
// transport dies underneath a pending unary call and a pending stream
// receive: both report ErrClosed and every stream is cleaned up.
func TestContractServerSideTeardown(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	// Park the server's first write (the unary response) so the call is
	// genuinely in flight when the transport dies.
	h.serverConn.holdNextWrite()
	callErr := contractGo(func() error {
		var resp internal.EchoPayload
		return h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &resp)
	})
	h.log.waitCount(t, 1)
	h.serverConn.waitParked(t)
	h.waitClientStreamCount(1)

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	h.waitClientStreamCount(2)

	// The server side of the transport dies.
	if err := h.serverConn.Close(); err != nil {
		t.Fatalf("close server conn: %v", err)
	}

	if err := contractAwait(t, callErr, "pending unary call"); !errors.Is(err, ErrClosed) {
		t.Fatalf("pending unary call: got %v, want ErrClosed", err)
	}
	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); !errors.Is(err, ErrClosed) {
		t.Fatalf("pending stream recv: got %v, want ErrClosed", err)
	}
	h.waitClientStreamCount(0)

	// The connection is terminal: further calls fail with ErrClosed.
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{}, &internal.EchoPayload{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-teardown call: got %v, want ErrClosed", err)
	}
}

// TestContractNewStreamWriteError pins the observable error when the
// transport fails on the very first write of a new stream.
func TestContractNewStreamWriteError(t *testing.T) {
	h := newContractHarness(t)

	h.clientConn.injectWriteError(io.ErrClosedPipe)
	_, err := h.client.NewStream(context.Background(), &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "EchoStream", nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("NewStream with broken transport: got %v, want ErrClosed", err)
	}

	// The stream was registered before the failed write and stays in the
	// table until the connection is closed.
	h.waitClientStreamCount(1)
	if err := h.client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	h.waitClientStreamCount(0)
}

// TestContractClientStreamingDoubleCloseSend pins the client-streaming
// send-side state machine: the first CloseSend emits exactly one close
// frame, a second CloseSend and any later SendMsg are local errors that
// produce no frames, and the final status removes the stream.
func TestContractClientStreamingDoubleCloseSend(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "Collect", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}

	// A second CloseSend is rejected locally as ErrStreamClosed.
	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend: got %v, want ErrStreamClosed", err)
	}
	// SendMsg after CloseSend is rejected locally as well.
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 2}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendMsg after CloseSend: got %v, want ErrStreamClosed", err)
	}

	// Exactly three frames went out: request, one data frame, one close
	// frame. The rejected operations produced nothing.
	frames := h.log.waitCount(t, 3)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, flagRemoteOpen, 1)
	contractAssertFrame(t, frames[1], "C2S", messageTypeData, 0, 1)
	contractAssertFrame(t, frames[2], "C2S", messageTypeData, flagRemoteClosed|flagNoData, 1)
	if frames[2].hdr.Length != 0 {
		t.Fatalf("close frame must not carry data, got %d bytes", frames[2].hdr.Length)
	}

	// The server observed the half-close after exactly one message.
	if n := contractAwaitInt64(t, h.collectCount, "handler message count"); n != 1 {
		t.Fatalf("handler saw %d messages, want 1", n)
	}
	close(h.collectRelease)

	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg final status: %v", err)
	}
	if resp.Seq != 1 {
		t.Fatalf("final count %d, want 1", resp.Seq)
	}
	frames = h.log.waitCount(t, 4)
	contractAssertFrame(t, frames[3], "S2C", messageTypeResponse, 0, 1)
	contractAssertStatus(t, frames[3], codes.OK)

	// The final status removed the stream synchronously.
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final status: %d", n)
	}
	// Further receives report a local EOF.
	if err := cs.RecvMsg(&resp); err != io.EOF {
		t.Fatalf("RecvMsg after final status: got %v, want io.EOF", err)
	}
}

// TestContractClientStreamingHalfCloseWithoutData pins half-closing a
// client stream without ever sending data: the server observes an
// immediate EOF and the final status still completes the stream.
func TestContractClientStreamingHalfCloseWithoutData(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "Collect", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Only the request and the empty close frame may appear.
	frames := h.log.waitCount(t, 2)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, flagRemoteOpen, 1)
	contractAssertFrame(t, frames[1], "C2S", messageTypeData, flagRemoteClosed|flagNoData, 1)
	if frames[1].hdr.Length != 0 {
		t.Fatalf("close frame must not carry data, got %d bytes", frames[1].hdr.Length)
	}

	if n := contractAwaitInt64(t, h.collectCount, "handler message count"); n != 0 {
		t.Fatalf("handler saw %d messages, want 0", n)
	}
	close(h.collectRelease)

	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg final status: %v", err)
	}
	if resp.Seq != 0 {
		t.Fatalf("final count %d, want 0", resp.Seq)
	}
	if err := cs.RecvMsg(&resp); err != io.EOF {
		t.Fatalf("RecvMsg after final status: got %v, want io.EOF", err)
	}
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final status: %d", n)
	}
}

// TestContractServerStreamingGatedWrites pins the server-streaming
// receive-side state machine: writes can be paused mid-stream, a parked
// frame is not observable by RecvMsg, protocol misuse is rejected
// locally, and the final payload arrives as a closing data frame.
func TestContractServerStreamingGatedWrites(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", &internal.EchoPayload{Seq: 7})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// Sending on a receive-only stream is a local protocol error.
	if err := cs.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("SendMsg on receive-only stream: got %v, want ErrProtocol", err)
	}
	if err := cs.CloseSend(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("CloseSend on receive-only stream: got %v, want ErrProtocol", err)
	}

	frames := h.log.waitCount(t, 1)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, flagRemoteClosed, 1)

	// Park the server's first write (the first data frame).
	h.serverConn.holdNextWrite()
	close(h.s2cStart)
	h.serverConn.waitParked(t)

	// The frame was attempted but not delivered: RecvMsg must block.
	recvErr := contractGo(func() error {
		var msg internal.EchoPayload
		if err := cs.RecvMsg(&msg); err != nil {
			return err
		}
		if msg.Seq != 1 {
			return errors.New("unexpected first message")
		}
		return nil
	})
	contractNoResultYet(t, recvErr, "RecvMsg while server write parked")

	h.serverConn.releaseWrite()
	if err := contractAwait(t, recvErr, "first message"); err != nil {
		t.Fatal(err)
	}

	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("second message: %v", err)
	}
	if msg.Seq != 2 {
		t.Fatalf("second message seq %d, want 2", msg.Seq)
	}
	// The handler's return value arrives as a closing data frame.
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("final message: %v", err)
	}
	if msg.Seq != 99 {
		t.Fatalf("final message seq %d, want 99", msg.Seq)
	}
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after final frame: got %v, want io.EOF", err)
	}

	frames = h.log.waitCount(t, 4)
	contractAssertFrame(t, frames[1], "S2C", messageTypeData, 0, 1)
	contractAssertFrame(t, frames[2], "S2C", messageTypeData, 0, 1)
	contractAssertFrame(t, frames[3], "S2C", messageTypeData, flagRemoteClosed, 1)

	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final frame: %d", n)
	}
	h.waitConnState(connStateIdle)
}

// TestContractBidiStreamingLifecycle pins the bidirectional state
// machine: strict request/response interleaving on the wire, half-close
// propagation, and cleanup on both ends.
func TestContractBidiStreamingLifecycle(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	contractAwaitSignal(t, h.echoStreamStarted, "echo handler start")

	for i := int64(1); i <= 3; i++ {
		if err := cs.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
			t.Fatalf("SendMsg %d: %v", i, err)
		}
		var msg internal.EchoPayload
		if err := cs.RecvMsg(&msg); err != nil {
			t.Fatalf("RecvMsg %d: %v", i, err)
		}
		if msg.Seq != i+1 {
			t.Fatalf("echo %d: got seq %d, want %d", i, msg.Seq, i+1)
		}
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after half-close: got %v, want io.EOF", err)
	}

	// The wire shows strict ping-pong ordering followed by both closes.
	frames := h.log.waitCount(t, 9)
	contractAssertFrame(t, frames[0], "C2S", messageTypeRequest, flagRemoteOpen, 1)
	for i := 0; i < 3; i++ {
		contractAssertFrame(t, frames[1+2*i], "C2S", messageTypeData, 0, 1)
		contractAssertFrame(t, frames[2+2*i], "S2C", messageTypeData, 0, 1)
	}
	contractAssertFrame(t, frames[7], "C2S", messageTypeData, flagRemoteClosed|flagNoData, 1)
	contractAssertFrame(t, frames[8], "S2C", messageTypeData, flagRemoteClosed|flagNoData, 1)

	contractAwaitSignal(t, h.echoStreamDone, "echo handler return")
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final frame: %d", n)
	}
	h.waitConnState(connStateIdle)
}

// TestContractExtraFramesAfterFinalStatus pins the handling of frames
// that arrive for a stream id whose final status was already exchanged:
// the server rejects them with InvalidArgument, the client drops
// anything for unknown stream ids, and the connection stays healthy.
func TestContractExtraFramesAfterFinalStatus(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after half-close: got %v, want io.EOF", err)
	}
	h.log.waitCount(t, 5)
	// The server has finished the stream and removed it from its table.
	h.waitConnState(connStateIdle)

	// A late data frame for the finished stream id is rejected by the
	// server with an InvalidArgument status on the same stream id.
	payload, err := proto.Marshal(&internal.EchoPayload{Seq: 100})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	h.injectC2S(contractDataFrame(1, 0, payload))

	frames := h.log.waitCount(t, 6)
	contractAssertFrame(t, frames[5], "S2C", messageTypeResponse, 0, 1)
	contractAssertStatus(t, frames[5], codes.InvalidArgument)

	// The client no longer tracks the stream: the error status is
	// dropped and the connection stays healthy.
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty: %d", n)
	}
	var resp internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &resp); err != nil {
		t.Fatalf("connection unhealthy after late frame: %v", err)
	}
	if resp.Seq != 2 {
		t.Fatalf("response seq %d, want 2", resp.Seq)
	}

	// A late frame delivered to the client for a removed stream id is
	// dropped as well; a subsequent call proves the receive loop
	// processed it without harm (frames are handled in order).
	h.injectS2C(contractDataFrame(1, 0, payload))
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 5}, &resp); err != nil {
		t.Fatalf("connection unhealthy after client-side late frame: %v", err)
	}
	if resp.Seq != 6 {
		t.Fatalf("response seq %d, want 6", resp.Seq)
	}
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty: %d", n)
	}
}

// TestContractStreamIDReuseAndOrdering pins the server's stream-id
// rules: ids must be odd, strictly increasing, and never reused.
func TestContractStreamIDReuseAndOrdering(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	// A real unary call occupies stream id 1 and completes.
	var resp internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &resp); err != nil {
		t.Fatalf("unary call: %v", err)
	}
	h.log.waitCount(t, 2)

	payload, err := proto.Marshal(&internal.EchoPayload{Seq: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Reusing a completed stream id for a new request is rejected.
	h.injectC2S(contractRequestFrame(t, 1, 0, contractService, "Echo", payload))
	frames := h.log.waitCount(t, 3)
	contractAssertFrame(t, frames[2], "S2C", messageTypeResponse, 0, 1)
	contractAssertStatus(t, frames[2], codes.InvalidArgument)

	// An even stream id is rejected.
	h.injectC2S(contractRequestFrame(t, 4, 0, contractService, "Echo", payload))
	frames = h.log.waitCount(t, 4)
	contractAssertFrame(t, frames[3], "S2C", messageTypeResponse, 0, 4)
	contractAssertStatus(t, frames[3], codes.InvalidArgument)

	// A fresh, higher odd id is accepted, proving the connection is
	// still healthy after the rejections.
	h.injectC2S(contractRequestFrame(t, 5, 0, contractService, "Echo", payload))
	frames = h.log.waitCount(t, 5)
	contractAssertFrame(t, frames[4], "S2C", messageTypeResponse, 0, 5)
	contractAssertStatus(t, frames[4], codes.OK)

	// A lower id after a higher one is rejected: ids must increase.
	h.injectC2S(contractRequestFrame(t, 3, 0, contractService, "Echo", payload))
	frames = h.log.waitCount(t, 6)
	contractAssertFrame(t, frames[5], "S2C", messageTypeResponse, 0, 3)
	contractAssertStatus(t, frames[5], codes.InvalidArgument)

	// The client never created streams for these ids; nothing leaks.
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("client stream table not empty: %d", n)
	}
}

// TestContractStreamReceiveCancelWithFullQueue pins the client-side
// backpressure contract: when the stream receive queue is full and the
// context is canceled, receive reports the context error instead of
// blocking, and the stream itself is left open with its queue intact.
func TestContractStreamReceiveCancelWithFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newStream(1, contractSenderFunc(func(uint32, messageType, uint8, []byte) error { return nil }), 1)

	if err := s.receive(ctx, &streamMessage{}); err != nil {
		t.Fatalf("first receive: %v", err)
	}
	// The queue is now full. A canceled context must be reported
	// immediately instead of waiting for the drain timeout.
	cancel()
	if err := s.receive(ctx, &streamMessage{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("full queue + cancel: got %v, want context.Canceled", err)
	}
	// Cancellation did not close the stream and the queued message is
	// still intact.
	select {
	case <-s.recvClose:
		t.Fatal("stream closed by cancellation")
	default:
	}
	select {
	case <-s.recv:
	default:
		t.Fatal("queued message lost after cancellation")
	}
	// After closeWithError, receive reports the terminal error.
	if err := s.closeWithError(nil); err != nil {
		t.Fatalf("closeWithError: %v", err)
	}
	if err := s.receive(context.Background(), &streamMessage{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed stream receive: got %v, want ErrClosed", err)
	}
}

type contractSenderFunc func(uint32, messageType, uint8, []byte) error

func (f contractSenderFunc) send(id uint32, mt messageType, flags uint8, b []byte) error {
	return f(id, mt, flags, b)
}

// TestContractStreamHandlerDataCancelWithFullQueue pins the server-side
// backpressure contract for streamHandler.data, plus the drain-then-EOF
// behavior of closeSend and the rejection of data after closeSend.
func TestContractStreamHandlerDataCancelWithFullQueue(t *testing.T) {
	noop := func(any) error { return nil }

	// Full queue + canceled context: the context error is reported.
	ctx, cancel := context.WithCancel(context.Background())
	sh := &streamHandler{ctx: ctx, recv: make(chan Unmarshaler, 1)}
	if err := sh.data(noop); err != nil {
		t.Fatalf("first data: %v", err)
	}
	cancel()
	if err := sh.data(noop); !errors.Is(err, context.Canceled) {
		t.Fatalf("full queue + cancel: got %v, want context.Canceled", err)
	}

	// closeSend lets RecvMsg drain the queue before reporting EOF, and
	// further data is rejected as ErrStreamClosed.
	sh2 := &streamHandler{ctx: context.Background(), recv: make(chan Unmarshaler, 1)}
	if err := sh2.data(noop); err != nil {
		t.Fatalf("first data: %v", err)
	}
	sh2.closeSend()
	var msg internal.EchoPayload
	if err := sh2.RecvMsg(&msg); err != nil {
		t.Fatalf("drain after closeSend: %v", err)
	}
	if err := sh2.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after drain: got %v, want io.EOF", err)
	}
	if err := sh2.data(noop); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("data after closeSend: got %v, want ErrStreamClosed", err)
	}
}

// TestContractServerQueueFullClosesStream pins the full-stack overflow
// path: when the server handler stops consuming, the stream queue fills
// and the server rejects the stream with InvalidArgument. The failing
// send path uses the production 1-second drain timeout; the test itself
// never sleeps.
func TestContractServerQueueFullClosesStream(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "Hang", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	contractAwaitSignal(t, h.hangStarted, "handler start")

	// Overflow the server-side stream queue (capacity
	// streamRecvBufferSize): the handler never consumes.
	for i := 0; i < streamRecvBufferSize+2; i++ {
		if err := cs.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			t.Fatalf("SendMsg %d: %v", i, err)
		}
	}

	// The server rejects the overflowing stream with InvalidArgument.
	var msg internal.EchoPayload
	err = cs.RecvMsg(&msg)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("overflow status: got %v, want InvalidArgument", err)
	}
	// The error status leaves the stream registered client-side.
	if n := h.clientStreamCount(); n != 1 {
		t.Fatalf("client stream count %d, want 1", n)
	}

	// Tearing down the connection cleans the stream up on both ends.
	h.disconnectClient()
	h.waitClientStreamCount(0)
	h.waitConnCount(0)
}

// The following three tests pin the observable contract when context
// cancellation and a remote status race each other. RecvMsg has no
// deterministic outcome for a truly simultaneous arrival (its select
// chooses at random), so the contract is defined by observation order:
// whichever becomes ready first is the observable result. Each test
// makes exactly one of them ready at the decision point.

// TestContractRecvMsgCancelBeforeStatus: cancellation is observable when
// it happens while no frame has been delivered yet.
func TestContractRecvMsgCancelBeforeStatus(t *testing.T) {
	h := newContractHarness(t)
	ctx, cancel := context.WithCancel(context.Background())

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", &internal.EchoPayload{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// Park the server's first data frame so nothing is in flight.
	h.serverConn.holdNextWrite()
	close(h.s2cStart)
	h.serverConn.waitParked(t)

	cancel()
	recvErr := contractGo(func() error {
		var msg internal.EchoPayload
		return cs.RecvMsg(&msg)
	})
	if err := contractAwait(t, recvErr, "RecvMsg after cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecvMsg after cancel: got %v, want context.Canceled", err)
	}

	// Cancellation does not remove the stream from the client table;
	// frames released afterwards are still queued for it.
	h.serverConn.releaseWrite()
	contractWait(t, "queued frames after release", func() bool {
		return h.clientStreamQueued(1) >= 1
	})
	if n := h.clientStreamCount(); n != 1 {
		t.Fatalf("client stream count %d, want 1 (cancel must not unregister)", n)
	}
}

// TestContractRecvMsgFinalStatusBeatsLaterCancel: once the final status
// has been observed, a later cancellation cannot change the terminal
// result; RecvMsg reports io.EOF.
func TestContractRecvMsgFinalStatusBeatsLaterCancel(t *testing.T) {
	h := newContractHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", &internal.EchoPayload{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	close(h.s2cStart)

	var msg internal.EchoPayload
	for want := int64(1); want <= 2; want++ {
		if err := cs.RecvMsg(&msg); err != nil {
			t.Fatalf("RecvMsg %d: %v", want, err)
		}
		if msg.Seq != want {
			t.Fatalf("message seq %d, want %d", msg.Seq, want)
		}
	}
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg final: %v", err)
	}
	if msg.Seq != 99 {
		t.Fatalf("final message seq %d, want 99", msg.Seq)
	}

	// The final status was consumed; a cancel now cannot be observed.
	cancel()
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after final status and cancel: got %v, want io.EOF", err)
	}
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final status: %d", n)
	}
}

// TestContractQueuedFramesBeatTeardown: frames already queued in the
// client stream buffer are delivered even after the connection is torn
// down, and a queued final status keeps its EOF semantics.
func TestContractQueuedFramesBeatTeardown(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", &internal.EchoPayload{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	close(h.s2cStart)

	// Wait until the server has written all three frames; they are now
	// queued ahead of anything else on the client read path.
	h.log.waitCount(t, 4)

	// Inject a terminal read error behind the queued frames.
	h.clientConn.injectReadError(io.EOF)

	// The queued frames are delivered in order despite the teardown.
	var msg internal.EchoPayload
	for want := int64(1); want <= 2; want++ {
		if err := cs.RecvMsg(&msg); err != nil {
			t.Fatalf("RecvMsg %d after teardown: %v", want, err)
		}
		if msg.Seq != want {
			t.Fatalf("message seq %d, want %d", msg.Seq, want)
		}
	}
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg final after teardown: %v", err)
	}
	if msg.Seq != 99 {
		t.Fatalf("final message seq %d, want 99", msg.Seq)
	}
	// The queued final status was consumed, so the terminal result is
	// the local EOF, not the teardown error.
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after queued final status: got %v, want io.EOF", err)
	}
	h.waitClientStreamCount(0)
}

// TestContractHandlerReturnRacesServerShutdown pins the race between a
// handler returning (its final frame in flight) and Server.Shutdown:
// shutdown waits for the active connection, the client still receives
// the final frame, and everything drains cleanly afterwards.
func TestContractHandlerReturnRacesServerShutdown(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	var msg internal.EchoPayload
	if err := cs.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	h.waitConnState(connStateActive)

	// Half-close: the handler returns and its final frame is parked on
	// the wire.
	h.serverConn.holdNextWrite()
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	h.serverConn.waitParked(t)

	// Shutdown must wait: the connection is still active with a response
	// in flight.
	shutdownErr := contractGo(func() error {
		return h.server.Shutdown(context.Background())
	})
	contractNoResultYet(t, shutdownErr, "Shutdown with response in flight")
	if st := h.connState(); st != connStateActive {
		t.Fatalf("connection state %v, want active", st)
	}

	// Release the final frame: the client observes the end of the
	// stream, then shutdown completes.
	h.serverConn.releaseWrite()
	if err := cs.RecvMsg(&msg); err != io.EOF {
		t.Fatalf("RecvMsg after release: got %v, want io.EOF", err)
	}
	if err := contractAwait(t, shutdownErr, "Shutdown"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	h.waitConnCount(0)
	if n := h.clientStreamCount(); n != 0 {
		t.Fatalf("stream not removed after final frame: %d", n)
	}
}

// TestContractServerCleanupOnClientDisconnect pins server-side cleanup
// when the client vanishes mid-stream: the handler's context is
// canceled, the handler returns, the connection leaves the server's
// connection set, and the client drains its own stream table.
func TestContractServerCleanupOnClientDisconnect(t *testing.T) {
	h := newContractHarness(t)
	ctx := context.Background()

	cs, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "Hang", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	contractAwaitSignal(t, h.hangStarted, "handler start")
	h.waitClientStreamCount(1)
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	// A unary call forces the server run loop to observe the active
	// stream and mark the connection active.
	var ping internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{Seq: 1}, &ping); err != nil {
		t.Fatalf("ping: %v", err)
	}
	h.waitConnState(connStateActive)

	// Hard client disconnect without any protocol teardown.
	h.disconnectClient()

	// The handler's context is canceled and the handler returns.
	if err := contractAwaitErr(t, h.hangErr, "handler return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("handler error: got %v, want context.Canceled", err)
	}
	// The connection leaves the server's connection set.
	h.waitConnCount(0)
	// The client drains its own stream table.
	h.waitClientStreamCount(0)
	// The client connection is terminal.
	if err := h.client.Call(ctx, contractService, "Echo", &internal.EchoPayload{}, &internal.EchoPayload{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-disconnect call: got %v, want ErrClosed", err)
	}
}
