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
	"strings"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"time"
)

func echoUnaryDesc() *ServiceDesc {
	return &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
		},
	}
}

// readResponseFrame reads one frame and asserts it is a terminal response
// with the expected status code, returning the decoded Response.
func readResponseFrame(t *testing.T, h *serverHarness, sid uint32, code codes.Code) *Response {
	t.Helper()
	f := h.peer.recv()
	if f.header.Type != messageTypeResponse {
		t.Fatalf("frame type = %v, want response", f.header.Type)
	}
	if f.header.StreamID != sid {
		t.Fatalf("response stream id = %d, want %d", f.header.StreamID, sid)
	}
	resp := decodeResponse(t, f)
	if resp.Status == nil {
		t.Fatalf("response carries no status")
	}
	if codes.Code(resp.Status.Code) != code {
		t.Fatalf("status code = %v, want %v (msg %q)", codes.Code(resp.Status.Code), code, resp.Status.Message)
	}
	return resp
}

// TestStateServerUnaryThenShutdown: a unary response is fully written before
// a graceful Shutdown begins; Shutdown then drains the idle connection and
// returns nil.
func TestStateServerUnaryThenShutdown(t *testing.T) {
	h := newServerHarness(t, echoUnaryDesc())

	h.sendRequest(1, "Echo", &internal.EchoPayload{Seq: 10, Msg: "one"}, 0)
	resp := readResponseFrame(t, h, 1, codes.OK)
	var out internal.EchoPayload
	if err := proto.Unmarshal(resp.Payload, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if out.Seq != 11 {
		t.Fatalf("echo seq = %d, want 11", out.Seq)
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- h.server.Shutdown(context.Background()) }()
	if err := waitErr(t, shutdownDone); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	h.log.waitEvent(t, evServerExited)
	if n := h.server.countConnection(); n != 0 {
		t.Fatalf("connections after Shutdown = %d, want 0", n)
	}
}

// TestStateServerCloseDropsInFlightUnary: a forced Close while the unary
// handler is still parked tears the connection down before the handler
// responds; the in-flight response is dropped and the peer observes EOF.
func TestStateServerCloseDropsInFlightUnary(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	desc := &ServiceDesc{
		Methods: map[string]Method{
			"Slow": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				close(started)
				<-release
				return &req, nil
			},
		},
	}
	h := newServerHarness(t, desc)

	h.sendRequest(1, "Slow", &internal.EchoPayload{Seq: 1}, 0)
	mustSignal(t, started, "handler start")

	if err := h.server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.log.waitEvent(t, evServerExited)

	// The connection is gone before the handler was released: the peer
	// observes EOF and no response frame ever arrives.
	if _, err := h.peer.recvErr(); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("peer read err = %v, want EOF", err)
	}
	close(release)
	if n := h.server.countConnection(); n != 0 {
		t.Fatalf("connections after Close = %d, want 0", n)
	}
}

// TestStateServerStreamHandlerCompletesDuringShutdown races a graceful
// Shutdown against a parked stream handler. The connection stays alive while
// the stream is active; once the handler returns, its terminal frame is
// still written, and only then does Shutdown drain the connection.
func TestStateServerStreamHandlerCompletesDuringShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"Park": {
				Handler: func(_ context.Context, _ StreamServer) (any, error) {
					close(started)
					<-release
					return nil, nil
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	h := newServerHarness(t, desc)

	h.sendRequest(1, "Park", nil, flagRemoteOpen)
	mustSignal(t, started, "handler start")
	if !h.streamPresent(1) {
		t.Fatal("stream must be tracked while the handler is parked")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- h.server.Shutdown(context.Background()) }()

	// Let the handler finish: its terminal frame must still reach the peer
	// even though Shutdown is in progress.
	close(release)
	terminal := h.peer.recv()
	if terminal.header.Type != messageTypeData ||
		terminal.header.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal frame wrong: %+v", terminal.header)
	}
	h.log.waitEvent(t, evServerDeleted)
	if h.streamPresent(1) {
		t.Fatal("stream must be removed once the terminal frame is written")
	}

	if err := waitErr(t, shutdownDone); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	h.log.waitEvent(t, evServerExited)
}

// TestStateServerDisconnectCleanup: when the client vanishes mid-stream, the
// connection's run loop exits, the handler's context is canceled (its
// RecvMsg unblocks with context.Canceled), and the connection is removed
// from the server's connection set.
func TestStateServerDisconnectCleanup(t *testing.T) {
	started := make(chan struct{})
	handlerDone := make(chan error, 1)
	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"Echo": {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					close(started)
					var req internal.EchoPayload
					err := ss.RecvMsg(&req)
					handlerDone <- err
					return nil, err
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	h := newServerHarness(t, desc)

	h.sendRequest(1, "Echo", nil, flagRemoteOpen)
	mustSignal(t, started, "handler start")
	if !h.streamPresent(1) {
		t.Fatal("stream must be tracked while the client is connected")
	}

	// Client disconnects without any terminal frame.
	h.peer.close()

	h.log.waitEvent(t, evServerExited)
	if err := waitErr(t, handlerDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("handler RecvMsg err = %v, want context.Canceled", err)
	}

	// Shutdown rendezvous: it only returns once the connection is removed.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- h.server.Shutdown(context.Background()) }()
	if err := waitErr(t, shutdownDone); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := h.server.countConnection(); n != 0 {
		t.Fatalf("connections after disconnect = %d, want 0", n)
	}
}

// TestStateServerStreamIDRules pins the receive-goroutine validation:
// even ids, reused or non-incrementing ids, data for unknown ids, and late
// data for finished streams are each rejected with InvalidArgument on the
// offending id, without poisoning the connection.
func TestStateServerStreamIDRules(t *testing.T) {
	desc := echoUnaryDesc()
	desc.Streams = map[string]Stream{
		"Quick": {
			Handler: func(_ context.Context, _ StreamServer) (any, error) {
				return nil, nil
			},
			StreamingClient: true,
			StreamingServer: true,
		},
	}
	h := newServerHarness(t, desc)

	// Even client-initiated ids are invalid.
	h.sendRequest(2, "Echo", &internal.EchoPayload{}, 0)
	resp := readResponseFrame(t, h, 2, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "must be odd") {
		t.Fatalf("message = %q, want 'must be odd'", resp.Status.Message)
	}

	// A valid odd id works afterwards (connection not poisoned).
	h.sendRequest(1, "Echo", &internal.EchoPayload{Seq: 1}, 0)
	readResponseFrame(t, h, 1, codes.OK)

	// Reusing an id is rejected.
	h.sendRequest(1, "Echo", &internal.EchoPayload{}, 0)
	resp = readResponseFrame(t, h, 1, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "cannot be re-used") {
		t.Fatalf("message = %q, want 'cannot be re-used'", resp.Status.Message)
	}

	// A higher id works; a lower (non-incrementing) id is then rejected.
	h.sendRequest(5, "Echo", &internal.EchoPayload{Seq: 2}, 0)
	readResponseFrame(t, h, 5, codes.OK)
	h.sendRequest(3, "Echo", &internal.EchoPayload{}, 0)
	resp = readResponseFrame(t, h, 3, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "cannot be re-used") {
		t.Fatalf("message = %q, want 'cannot be re-used'", resp.Status.Message)
	}

	// Data for a stream that never existed is rejected.
	h.sendData(7, &internal.EchoPayload{Seq: 1}, 0)
	resp = readResponseFrame(t, h, 7, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "no longer active") {
		t.Fatalf("message = %q, want 'no longer active'", resp.Status.Message)
	}

	// Late data after a stream's terminal frame is rejected the same way.
	h.sendRequest(9, "Quick", nil, flagRemoteOpen)
	terminal := h.peer.recv()
	if terminal.header.Type != messageTypeData ||
		terminal.header.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal frame wrong: %+v", terminal.header)
	}
	h.log.waitEvent(t, evServerDeleted)
	if h.streamPresent(9) {
		t.Fatal("stream 9 must be removed after its terminal frame")
	}
	h.sendData(9, &internal.EchoPayload{Seq: 2}, 0)
	resp = readResponseFrame(t, h, 9, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "no longer active") {
		t.Fatalf("message = %q, want 'no longer active'", resp.Status.Message)
	}

	// The connection is still usable for a fresh stream.
	h.sendRequest(11, "Echo", &internal.EchoPayload{Seq: 3}, 0)
	readResponseFrame(t, h, 11, codes.OK)
}

// TestStateServerBackpressureCancel fills a stream handler's receive buffer
// so the receive goroutine parks in the backpressure slow path, then cancels
// the connection context. The parked delivery must fail with the context
// error and surface as an InvalidArgument status on that stream id, while
// the connection stays alive enough to deliver the handler's own terminal
// response afterwards.
func TestStateServerBackpressureCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"Sink": {
				Handler: func(_ context.Context, _ StreamServer) (any, error) {
					close(started)
					<-release // park without consuming
					return nil, nil
				},
				StreamingClient: true,
				StreamingServer: false,
			},
		},
	}
	h := newServerHarness(t, desc)

	h.sendRequest(1, "Sink", nil, flagRemoteOpen)
	mustSignal(t, started, "handler start")

	// Fill the handler receive buffer; the (N+1)-th frame parks the
	// receive goroutine in the slow path.
	for i := int64(1); i <= streamRecvBufferSize+1; i++ {
		h.sendData(1, &internal.EchoPayload{Seq: i}, 0)
	}
	h.log.waitEvent(t, evServerBlocked)

	// Cancelling the connection context unblocks the parked delivery with
	// the context error, surfaced as InvalidArgument on stream 1.
	h.cancelCtx()
	resp := readResponseFrame(t, h, 1, codes.InvalidArgument)
	if !strings.Contains(resp.Status.Message, "data handling error") {
		t.Fatalf("message = %q, want 'data handling error'", resp.Status.Message)
	}

	// The handler still terminates the stream normally afterwards.
	close(release)
	readResponseFrame(t, h, 1, codes.OK)
	h.log.waitEvent(t, evServerDeleted)
}

// TestStateServerStreamTableLifecycle is mutation contract M2: a stream must
// remain in the connection's stream table for its whole lifetime. Deleting
// it early (for example when the first data frame is answered) would make
// the next data frame bounce with 'no longer active' instead of reaching the
// handler.
func TestStateServerStreamTableLifecycle(t *testing.T) {
	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"Echo": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if errors.Is(err, io.EOF) {
								return nil, nil
							}
							return nil, err
						}
						req.Seq++
						if err := ss.SendMsg(&req); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	h := newServerHarness(t, desc)

	h.sendRequest(1, "Echo", nil, flagRemoteOpen)
	if !h.streamPresent(1) {
		t.Fatal("stream must be tracked right after the request")
	}

	// Two rounds of echo: the stream must survive the first terminal-less
	// data exchange.
	for i := int64(1); i <= 2; i++ {
		h.sendData(1, &internal.EchoPayload{Seq: i}, 0)
		echo := h.peer.recv()
		if echo.header.Type != messageTypeData || echo.header.Flags != 0 {
			t.Fatalf("echo %d: unexpected frame %+v (stream dropped early?)", i, echo.header)
		}
		if got := decodeEcho(t, echo); got.Seq != i+1 {
			t.Fatalf("echo %d seq = %d, want %d", i, got.Seq, i+1)
		}
		if !h.streamPresent(1) {
			t.Fatalf("stream deleted after echo %d; must live until terminal frame", i)
		}
	}
	h.log.assertNotRecorded(t, evServerDeleted)

	// Half-close ends the stream: exactly one terminal frame, then removal.
	h.sendData(1, nil, flagRemoteClosed|flagNoData)
	terminal := h.peer.recv()
	if terminal.header.Flags != flagRemoteClosed|flagNoData {
		t.Fatalf("terminal flags = %#x, want half-close", terminal.header.Flags)
	}
	h.log.waitEvent(t, evServerDeleted)
	if h.streamPresent(1) {
		t.Fatal("stream must be removed after the terminal frame")
	}
	if h.log.count(evServerDeleted) != 1 {
		t.Fatalf("stream deleted %d times, want exactly 1", h.log.count(evServerDeleted))
	}
}

// mustSignal blocks until ch is closed, failing the test after the wait
// timeout instead of hanging forever.
func mustSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
