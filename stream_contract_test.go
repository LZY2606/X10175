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

// State-machine contract tests for the four RPC shapes. Each test replays an
// exact frame sequence through the relay and asserts concrete return values,
// status codes, event ordering, and the point at which streams leave the
// client's stream table. The final expectServerEOF in each test proves the
// server-side connection state machine returned to idle (active == 0) before
// honoring shutdown, i.e. no stream leaked.

import (
	"context"
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const smService = "sm"

// expectFrame is a small assertion helper for header fields.
func expectFrame(t *testing.T, f frame, mt messageType, sid uint32, flags uint8) {
	t.Helper()
	if f.header.Type != mt || f.header.StreamID != sid || f.header.Flags != flags {
		t.Fatalf("unexpected frame: got %s sid=%d flags=%02x, want %s sid=%d flags=%02x",
			f.header.Type, f.header.StreamID, f.header.Flags, mt, sid, flags)
	}
}

func TestContractUnaryStateMachine(t *testing.T) {
	rig := newSMRig(t)
	rig.server.RegisterService(smService, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &internal.TestPayload{Foo: req.Foo + req.Foo}, nil
			},
		},
	})

	var resp internal.TestPayload
	callDone := make(chan error, 1)
	go func() {
		callDone <- rig.client.Call(context.Background(), smService, "Echo", &internal.TestPayload{Foo: "ab"}, &resp)
	}()

	// The client's request write stays paused inside channel.send until the
	// relay reads it off the pipe.
	f := rig.relay.readClient(t)
	expectFrame(t, f, messageTypeRequest, 1, 0)

	// The stream is registered before the request hits the wire.
	if ids := streamIDs(rig.client); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected stream table {1} while request in flight, got %v", ids)
	}

	var reqMsg Request
	if err := proto.Unmarshal(f.payload, &reqMsg); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if reqMsg.Service != smService || reqMsg.Method != "Echo" {
		t.Fatalf("unexpected request target %q/%q", reqMsg.Service, reqMsg.Method)
	}
	rig.relay.forwardToServer(t, f)

	f = rig.relay.readServer(t)
	expectFrame(t, f, messageTypeResponse, 1, 0)
	respMsg := unmarshalResponse(t, f)
	if respMsg.Status.GetCode() != int32(codes.OK) {
		t.Fatalf("expected OK status, got %v", respMsg.Status.GetCode())
	}
	rig.relay.forwardToClient(t, f)

	if err := <-callDone; err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Foo != "abab" {
		t.Fatalf("unexpected response payload %q", resp.Foo)
	}

	// Cleanup timing: once Call returns, the stream is gone from the table.
	if ids := streamIDs(rig.client); len(ids) != 0 {
		t.Fatalf("expected empty stream table after Call, got %v", ids)
	}

	rig.relay.expectEvents(t,
		"c->s request sid=1 flags=00",
		"s->c response sid=1 flags=00",
	)

	// Server drained the unary call (active back to 0) before shutdown.
	rig.sc.close()
	rig.relay.expectServerEOF(t)
}

func TestContractClientStreamingStateMachine(t *testing.T) {
	rig := newSMRig(t)
	rig.server.RegisterService(smService, &ServiceDesc{
		Streams: map[string]Stream{
			"Upload": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var sum int64
					for {
						var p internal.EchoPayload
						err := ss.RecvMsg(&p)
						if err == io.EOF {
							return &internal.EchoPayload{Seq: sum}, nil
						}
						if err != nil {
							return nil, err
						}
						sum += p.Seq
					}
				},
				StreamingClient: true,
			},
		},
	})

	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := rig.client.NewStream(context.Background(), &StreamDesc{StreamingClient: true}, smService, "Upload", nil)
		streamCh <- streamResult{cs, err}
	}()

	// StreamingClient streams open with flagRemoteOpen.
	f := rig.relay.passClientFrame(t)
	expectFrame(t, f, messageTypeRequest, 1, flagRemoteOpen)

	got := <-streamCh
	if got.err != nil {
		t.Fatalf("new stream: %v", got.err)
	}
	cs := got.cs

	if ids := streamIDs(rig.client); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected stream table {1}, got %v", ids)
	}

	// Each SendMsg write is paused until the relay releases exactly one frame.
	for _, seq := range []int64{1, 2} {
		sendDone := make(chan error, 1)
		go func() { sendDone <- cs.SendMsg(&internal.EchoPayload{Seq: seq}) }()
		f = rig.relay.passClientFrame(t)
		expectFrame(t, f, messageTypeData, 1, 0)
		if err := <-sendDone; err != nil {
			t.Fatalf("send %d: %v", seq, err)
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	f = rig.relay.passClientFrame(t)
	expectFrame(t, f, messageTypeData, 1, flagRemoteClosed|flagNoData)
	if f.header.Length != 0 {
		t.Fatalf("half-close frame must not carry data, got length %d", f.header.Length)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close send: %v", err)
	}

	// The server observed EOF, produced its aggregate, and answered with a
	// final Response message.
	f = rig.relay.passServerFrame(t)
	expectFrame(t, f, messageTypeResponse, 1, 0)
	respMsg := unmarshalResponse(t, f)
	if respMsg.Status.GetCode() != int32(codes.OK) {
		t.Fatalf("expected OK status, got %v", respMsg.Status.GetCode())
	}

	var out internal.EchoPayload
	if err := cs.RecvMsg(&out); err != nil {
		t.Fatalf("recv final: %v", err)
	}
	if out.Seq != 3 {
		t.Fatalf("expected aggregated seq 3, got %d", out.Seq)
	}

	// After the final status the stream is closed and removed; further
	// receives report io.EOF.
	if err := cs.RecvMsg(&out); err != io.EOF {
		t.Fatalf("expected io.EOF after final status, got %v", err)
	}
	if ids := streamIDs(rig.client); len(ids) != 0 {
		t.Fatalf("expected empty stream table after final status, got %v", ids)
	}

	rig.relay.expectEvents(t,
		"c->s request sid=1 flags=02",
		"c->s data sid=1 flags=00",
		"c->s data sid=1 flags=00",
		"c->s data sid=1 flags=05",
		"s->c response sid=1 flags=00",
	)

	rig.sc.close()
	rig.relay.expectServerEOF(t)
}

func TestContractServerStreamingStateMachine(t *testing.T) {
	rig := newSMRig(t)
	rig.server.RegisterService(smService, &ServiceDesc{
		Streams: map[string]Stream{
			"Download": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var req internal.TestPayload
					if err := ss.RecvMsg(&req); err != nil {
						return nil, err
					}
					if req.Foo != "go" {
						return nil, io.ErrUnexpectedEOF
					}
					for i := int64(0); i < 3; i++ {
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
							return nil, err
						}
					}
					return nil, nil
				},
				StreamingServer: true,
			},
		},
	})

	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := rig.client.NewStream(context.Background(), &StreamDesc{StreamingServer: true}, smService, "Download", &internal.TestPayload{Foo: "go"})
		streamCh <- streamResult{cs, err}
	}()

	// Non-streaming client half-closes immediately: flagRemoteClosed.
	f := rig.relay.passClientFrame(t)
	expectFrame(t, f, messageTypeRequest, 1, flagRemoteClosed)

	got := <-streamCh
	if got.err != nil {
		t.Fatalf("new stream: %v", got.err)
	}
	cs := got.cs

	for i := int64(0); i < 3; i++ {
		f = rig.relay.passServerFrame(t)
		expectFrame(t, f, messageTypeData, 1, 0)
		var out internal.EchoPayload
		if err := cs.RecvMsg(&out); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if out.Seq != i {
			t.Fatalf("expected seq %d, got %d", i, out.Seq)
		}
		// The stream stays registered until the final status is consumed.
		if ids := streamIDs(rig.client); len(ids) != 1 {
			t.Fatalf("expected stream to remain registered, got %v", ids)
		}
	}

	// Server finished: final frame is data with remoteClosed|noData.
	f = rig.relay.passServerFrame(t)
	expectFrame(t, f, messageTypeData, 1, flagRemoteClosed|flagNoData)
	if f.header.Length != 0 {
		t.Fatalf("final frame must not carry data, got length %d", f.header.Length)
	}

	var out internal.EchoPayload
	if err := cs.RecvMsg(&out); err != io.EOF {
		t.Fatalf("expected io.EOF from final frame, got %v", err)
	}
	if ids := streamIDs(rig.client); len(ids) != 0 {
		t.Fatalf("expected empty stream table after final frame, got %v", ids)
	}

	rig.relay.expectEvents(t,
		"c->s request sid=1 flags=04",
		"s->c data sid=1 flags=00",
		"s->c data sid=1 flags=00",
		"s->c data sid=1 flags=00",
		"s->c data sid=1 flags=05",
	)

	rig.sc.close()
	rig.relay.expectServerEOF(t)
}

func TestContractBidiStreamingStateMachine(t *testing.T) {
	rig := newSMRig(t)
	rig.server.RegisterService(smService, &ServiceDesc{
		Streams: map[string]Stream{
			"Echo": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for {
						var p internal.EchoPayload
						err := ss.RecvMsg(&p)
						if err == io.EOF {
							return nil, nil
						}
						if err != nil {
							return nil, err
						}
						p.Seq++
						if err := ss.SendMsg(&p); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})

	type streamResult struct {
		cs  ClientStream
		err error
	}
	streamCh := make(chan streamResult, 1)
	go func() {
		cs, err := rig.client.NewStream(context.Background(), &StreamDesc{StreamingClient: true, StreamingServer: true}, smService, "Echo", nil)
		streamCh <- streamResult{cs, err}
	}()

	f := rig.relay.passClientFrame(t)
	expectFrame(t, f, messageTypeRequest, 1, flagRemoteOpen)

	got := <-streamCh
	if got.err != nil {
		t.Fatalf("new stream: %v", got.err)
	}
	cs := got.cs

	// Two full echo round-trips, frame by frame.
	for _, seq := range []int64{1, 2} {
		sendDone := make(chan error, 1)
		go func() { sendDone <- cs.SendMsg(&internal.EchoPayload{Seq: seq}) }()
		f = rig.relay.passClientFrame(t)
		expectFrame(t, f, messageTypeData, 1, 0)
		if err := <-sendDone; err != nil {
			t.Fatalf("send %d: %v", seq, err)
		}

		f = rig.relay.passServerFrame(t)
		expectFrame(t, f, messageTypeData, 1, 0)
		var out internal.EchoPayload
		if err := cs.RecvMsg(&out); err != nil {
			t.Fatalf("recv %d: %v", seq, err)
		}
		if out.Seq != seq+1 {
			t.Fatalf("expected echoed seq %d, got %d", seq+1, out.Seq)
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.CloseSend() }()
	f = rig.relay.passClientFrame(t)
	expectFrame(t, f, messageTypeData, 1, flagRemoteClosed|flagNoData)
	if err := <-closeDone; err != nil {
		t.Fatalf("close send: %v", err)
	}

	// Server observed the half-close, returned, and emitted its final frame.
	f = rig.relay.passServerFrame(t)
	expectFrame(t, f, messageTypeData, 1, flagRemoteClosed|flagNoData)

	var out internal.EchoPayload
	if err := cs.RecvMsg(&out); err != io.EOF {
		t.Fatalf("expected io.EOF from final frame, got %v", err)
	}
	if ids := streamIDs(rig.client); len(ids) != 0 {
		t.Fatalf("expected empty stream table after final frame, got %v", ids)
	}

	rig.relay.expectEvents(t,
		"c->s request sid=1 flags=02",
		"c->s data sid=1 flags=00",
		"s->c data sid=1 flags=00",
		"c->s data sid=1 flags=00",
		"s->c data sid=1 flags=00",
		"c->s data sid=1 flags=05",
		"s->c data sid=1 flags=05",
	)

	rig.sc.close()
	rig.relay.expectServerEOF(t)
}
