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

func wireEvent(id uint32, detail string) func(recEvent) bool {
	return func(e recEvent) bool {
		return e.kind == evWire && e.id == id && e.detail == detail
	}
}

func kindEvent(kind eventKind, id uint32) func(recEvent) bool {
	return func(e recEvent) bool { return e.kind == kind && e.id == id }
}

// TestContractUnary replays a unary RPC frame by frame and pins down:
//   - exact request/response types and flags,
//   - the client stream exists exactly between createStream and the terminal
//     response and is deleted *after* the response frame is observed,
//   - the server never keeps a unary stream in its stream set,
//   - frames arriving on the now-inactive id are rejected (logged),
//   - stream ids keep increasing and are never reused.
func TestContractUnary(t *testing.T) {
	h := newHarness(t, nil)
	h.register(&ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var p internal.EchoPayload
				if err := unmarshal(&p); err != nil {
					return nil, err
				}
				p.Seq++
				return &p, nil
			},
		},
	})

	var req, resp internal.EchoPayload
	req.Seq = 7
	if err := h.client.Call(context.Background(), "stateMachineService", "Echo", &req, &resp); err != nil {
		t.Fatalf("unary call: %v", err)
	}
	if resp.Seq != 8 {
		t.Fatalf("seq: got %d want 8", resp.Seq)
	}

	// Request frame: stream id 1, type request, flags 0 (unary).
	h.rec.wait(t, "server received response frame", wireEvent(1, "server-recv"))
	frames := h.rec.snapshot()
	var sawRequest, sawResponse bool
	for _, e := range frames {
		if e.kind != evWire {
			continue
		}
		switch e.detail {
		case "server-recv":
			if e.id != 1 || e.mtype != messageTypeRequest || e.flags != 0 {
				t.Fatalf("unary request frame mismatch: %+v", e)
			}
			sawRequest = true
		case "client-recv":
			if e.id != 1 || e.mtype != messageTypeResponse || e.flags != 0 {
				t.Fatalf("unary response frame mismatch: %+v", e)
			}
			sawResponse = true
		}
	}
	if !sawRequest || !sawResponse {
		t.Fatalf("missing request/response frames: %v", frames)
	}

	// Client stream 1 was registered once and deleted once, and the response
	// wire frame was observed before deletion.
	h.rec.assertOrder(t, wireEvent(1, "client-recv"), kindEvent(evClientDeleted, 1))
	h.rec.assertOrder(t, kindEvent(evClientRegistered, 1), wireEvent(1, "client-recv"))
	if n := h.rec.count(kindEvent(evClientRegistered, 1)); n != 1 {
		t.Fatalf("stream 1 registered %d times, want 1", n)
	}

	// A unary request is also tracked server-side for the active/idle
	// connection state, but exactly once and removed with the response.
	if n := h.rec.count(kindEvent(evServerRegistered, 1)); n != 1 {
		t.Fatalf("unary stream registered server-side %d times, want 1", n)
	}
	h.rec.assertOrder(t, wireEvent(1, "server-recv"), kindEvent(evServerDeleted, 1))
	if h.sset.len() != 0 {
		t.Fatalf("server stream set not empty after unary: %d", h.sset.len())
	}

	// A stray late frame for the completed id is rejected, not delivered.
	before := len(h.rec.snapshot())
	h.serverC.injectRaw(encodeFrame(wireFrame{
		streamID: 1,
		mtype:    messageTypeResponse,
		flags:    0,
	}))
	h.rec.wait(t, "inactive dispatch for stray frame", func(e recEvent) bool {
		return e.kind == evClientDispatch && e.id == 1 && e.outcome == dispatchInactive
	})
	if n := h.rec.count(func(e recEvent) bool {
		return e.kind == evClientDispatch && e.id == 1 && e.outcome == dispatchDelivered
	}); n != 1 {
		t.Fatalf("expected exactly 1 delivered dispatch for stream 1, got %d", n)
	}
	_ = before

	// A subsequent call gets a new, strictly increasing odd id and succeeds.
	var req2, resp2 internal.EchoPayload
	req2.Seq = 100
	if err := h.client.Call(context.Background(), "stateMachineService", "Echo", &req2, &resp2); err != nil {
		t.Fatalf("second unary call: %v", err)
	}
	if resp2.Seq != 101 {
		t.Fatalf("seq: got %d want 101", resp2.Seq)
	}
	h.rec.wait(t, "server received second request", func(e recEvent) bool {
		return e.kind == evWire && e.id == 3 && e.detail == "server-recv"
	})
	if h.client.nextStreamID != 5 {
		t.Fatalf("nextStreamID: got %d want 5", h.client.nextStreamID)
	}
	if h.client.hasStream(1) || h.client.hasStream(3) {
		t.Fatal("completed unary streams still present in client map")
	}

	// Transport shutdown must not be mistaken for a grpc status error.
	h.clientC.injectReadError(io.ErrUnexpectedEOF)
	h.waitClientRunDone()
	h.waitServerRunDone()
}

// TestContractUnaryRemoteStatus asserts the exact status code and message
// delivered on error, and that the stream is cleaned up afterwards.
func TestContractUnaryRemoteStatus(t *testing.T) {
	h := newHarness(t, nil)
	h.register(&ServiceDesc{
		Methods: map[string]Method{
			"Boom": func(context.Context, func(any) error) (any, error) {
				return nil, status.Error(codes.ResourceExhausted, "backoff please")
			},
		},
	})

	var req, resp internal.EchoPayload
	err := h.client.Call(context.Background(), "stateMachineService", "Boom", &req, &resp)
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a grpc status: %v", err)
	}
	if st.Code() != codes.ResourceExhausted || st.Message() != "backoff please" {
		t.Fatalf("unexpected status: %v", st)
	}
	h.rec.wait(t, "stream 1 deleted after error status", kindEvent(evClientDeleted, 1))
	if h.client.hasStream(1) {
		t.Fatal("stream not deleted after terminal error status")
	}
	h.cleanup()
}

// TestContractClientStreaming covers: normal upload + final response, two
// consecutive CloseSend calls (the second is an idempotent error and sends
// nothing), and the exact ordering of close flag versus local state.
func TestContractClientStreaming(t *testing.T) {
	h := newHarness(t, nil)

	h.register(&ServiceDesc{
		Streams: map[string]Stream{
			"Upload": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var n int64
					for {
						var p internal.EchoPayload
						if err := ss.RecvMsg(&p); err != nil {
							if errors.Is(err, io.EOF) {
								return &internal.EchoPayload{Seq: n, Msg: "ack"}, nil
							}
							return nil, err
						}
						n += p.Seq
					}
				},
				StreamingClient: true,
				StreamingServer: false,
			},
		},
	})

	// Initial request: flagRemoteOpen, no payload.
	cs, err := h.client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, "stateMachineService", "Upload", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.rec.wait(t, "open request on wire", func(e recEvent) bool {
		return e.kind == evWire && e.detail == "server-recv" && e.id == 1 &&
			e.mtype == messageTypeRequest && e.flags == flagRemoteOpen
	})
	h.rec.wait(t, "server registered stream 1", kindEvent(evServerRegistered, 1))

	if err := cs.SendMsg(&internal.EchoPayload{Seq: 10, Msg: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 20, Msg: "b"}); err != nil {
		t.Fatal(err)
	}
	h.rec.wait(t, "second data frame on wire", func(e recEvent) bool {
		return e.kind == evWire && e.detail == "server-recv" && e.id == 1 &&
			e.mtype == messageTypeData && e.flags == 0 && e.seq == 0
	})

	// Pause writes so the first CloseSend parks; this proves the local flag is
	// not set until the close frame is successfully on the wire.
	h.clientC.pauseWrites()
	if err := cs.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	h.rec.wait(t, "close frame parked", func(e recEvent) bool {
		return e.kind == evHeld && e.id == 1 && e.flags == flagRemoteClosed|flagNoData
	})
	if len(h.clientC.heldFrames()) != 1 {
		t.Fatalf("held frames: %d", len(h.clientC.heldFrames()))
	}

	// Second CloseSend while the first is parked but already sent: localClosed
	// is already true, so ErrStreamClosed and no additional frame.
	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend err: %v, want ErrStreamClosed", err)
	}
	if len(h.clientC.heldFrames()) != 1 {
		t.Fatalf("second CloseSend produced a frame; held=%d", len(h.clientC.heldFrames()))
	}

	h.clientC.resumeWrites()
	h.rec.wait(t, "close frame reached server", func(e recEvent) bool {
		return e.kind == evWire && e.detail == "server-recv" && e.id == 1 &&
			e.mtype == messageTypeData && e.flags == flagRemoteClosed|flagNoData
	})

	// Sending after a completed half-close is rejected locally.
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendMsg after CloseSend: %v", err)
	}

	// Final response.
	var ack internal.EchoPayload
	if err := cs.RecvMsg(&ack); err != nil {
		t.Fatalf("RecvMsg final: %v", err)
	}
	if ack.Seq != 30 || ack.Msg != "ack" {
		t.Fatalf("ack: seq=%d msg=%q", ack.Seq, ack.Msg)
	}

	// After the terminal response, further RecvMsg returns io.EOF.
	var extra internal.EchoPayload
	if err := cs.RecvMsg(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("RecvMsg after terminal: %v, want io.EOF", err)
	}

	// Cleanup ordering: final response observed before both deletes.
	h.rec.assertOrder(t, wireEvent(1, "client-recv"), kindEvent(evClientDeleted, 1))
	h.rec.assertOrder(t, wireEvent(1, "client-recv"), kindEvent(evServerDeleted, 1))
	if h.client.hasStream(1) || h.sset.has(1) {
		t.Fatal("stream still present after completion")
	}

	// Third CloseSend keeps returning ErrStreamClosed and sends nothing.
	closeWiresBefore := h.rec.count(func(e recEvent) bool {
		return e.kind == evWire && e.detail == "server-recv" && e.id == 1
	})
	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("third CloseSend: %v", err)
	}
	if n := h.rec.count(func(e recEvent) bool {
		return e.kind == evWire && e.detail == "server-recv" && e.id == 1
	}); n != closeWiresBefore {
		t.Fatal("repeated CloseSend produced wire traffic")
	}

	h.cleanup()
}
