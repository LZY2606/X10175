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
// Unary contracts
// ---------------------------------------------------------------------------

// TestContractUnarySuccess verifies the exact request/response frame pair,
// the response payload, and that no stream survives the call.
func TestContractUnarySuccess(t *testing.T) {
	h := newManagedHarness(t, stateServiceDesc{
		methods: map[string]Method{
			"Echo": func(_ context.Context, u func(any) error) (any, error) {
				var p internal.EchoPayload
				if err := u(&p); err != nil {
					return nil, err
				}
				p.Seq++
				return &p, nil
			},
		},
	})

	var req, resp internal.EchoPayload
	req.Seq = 10
	if err := h.client.Call(context.Background(), stateTestService, "Echo", &req, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 11 {
		t.Fatalf("seq=%d want 11", resp.Seq)
	}

	// Exactly one request and one response, both on stream 1.
	ch := h.clientGate.headers()
	sh := h.serverGate.headers()
	if len(ch) != 1 || ch[0].Type != messageTypeRequest || ch[0].StreamID != 1 {
		t.Fatalf("client frames: %+v", ch)
	}
	if len(sh) != 1 || sh[0].Type != messageTypeResponse || sh[0].StreamID != 1 || sh[0].Flags != 0 {
		t.Fatalf("server frames: %+v", sh)
	}

	// Unary calls never register a server stream and leave no client stream.
	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("client streams leaked: %v", ids)
	}
}

// TestContractUnaryStatusError checks that a non-OK status is delivered
// with the right code/message and the client stream is removed.
func TestContractUnaryStatusError(t *testing.T) {
	h := newManagedHarness(t, stateServiceDesc{
		methods: map[string]Method{
			"Boom": func(context.Context, func(any) error) (any, error) {
				return nil, status.Errorf(codes.ResourceExhausted, "out of tokens")
			},
		},
	})
	var req, resp internal.EchoPayload
	err := h.client.Call(context.Background(), stateTestService, "Boom", &req, &resp)
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a grpc status: %v", err)
	}
	if st.Code() != codes.ResourceExhausted || st.Message() != "out of tokens" {
		t.Fatalf("status=%v", st)
	}
	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("client streams leaked after error: %v", ids)
	}
}

// TestContractUnaryUnimplemented verifies unknown services/methods get
// Unimplemented and still clean up.
func TestContractUnaryUnimplemented(t *testing.T) {
	h := newManagedHarness(t, stateServiceDesc{})
	var req, resp internal.EchoPayload
	err := h.client.Call(context.Background(), "nope", "Nope", &req, &resp)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("leaked streams: %v", ids)
	}
}

// ---------------------------------------------------------------------------
// Context cancellation vs remote final status race
// ---------------------------------------------------------------------------

func blockOnReleaseEcho() (map[string]Method, chan struct{}, chan struct{}) {
	release := make(chan struct{})
	entered := make(chan struct{})
	return map[string]Method{
		"Wait": func(_ context.Context, _ func(any) error) (any, error) {
			close(entered)
			<-release
			return &internal.EchoPayload{Seq: 7}, nil
		},
	}, release, entered
}

// TestContractUnaryCancelBeforeStatus: cancel arrives strictly before the
// response is readable. Result must be context.Canceled deterministically.
func TestContractUnaryCancelBeforeStatus(t *testing.T) {
	methods, release, entered := blockOnReleaseEcho()
	h := newManagedHarness(t, stateServiceDesc{methods: methods})
	h.pauseServer() // hold the response off the wire

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		errCh <- h.client.Call(ctx, stateTestService, "Wait", &req, &resp)
	}()
	<-entered

	// Release the handler and wait until its response write is parked.
	close(release)
	h.serverGate.waitPending(t)

	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}

	// Now let the withheld response leave. It must not bring the stream
	// back or be observable; the call already completed and cleanup ran.
	h.serverGate.permitNext(t)
	h.serverGate.setAuto(true)

	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("leaked client streams: %v", ids)
	}
}

// TestContractUnaryStatusBeforeCancel: the final response is delivered and
// buffered; cancellation afterwards must not overwrite the success.
func TestContractUnaryStatusBeforeCancel(t *testing.T) {
	methods, release, entered := blockOnReleaseEcho()
	h := newManagedHarness(t, stateServiceDesc{methods: methods})
	h.pauseServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	var resp internal.EchoPayload
	go func() {
		var req internal.EchoPayload
		errCh <- h.client.Call(ctx, stateTestService, "Wait", &req, &resp)
	}()
	<-entered
	close(release)
	h.serverGate.waitPending(t)
	// Release the response onto the wire so the client buffers it.
	h.serverGate.permitNext(t)
	h.serverGate.setAuto(true)
	// Wait until the response is definitely buffered in the client stream
	// (recv cap is 1), then cancel the call context.
	waitFor(t, "response buffered", func() bool {
		ids := h.client.clientStreamIDs()
		return len(ids) == 1
	})
	// The message is in the stream's recv channel. Even if ctx is
	// already cancelled when dispatch selects, the observed value must be
	// the success (or a single permitted race outcome), never a stale
	// status from a previous call.
	cancel()
	err := <-errCh
	if err != nil {
		t.Fatalf("expected success after buffered status, got %v", err)
	}
	if resp.Seq != 7 {
		t.Fatalf("seq=%d", resp.Seq)
	}
	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("leaked client streams: %v", ids)
	}
}

// TestContractUnaryCancelAndStatusConcurrent states the observable contract
// when both are selectable at the same time: the result must be exactly one
// of {success, context.Canceled}; no transport-level or unknown error is
// permitted, and the stream is removed regardless.
func TestContractUnaryCancelAndStatusConcurrent(t *testing.T) {
	methods, release, entered := blockOnReleaseEcho()
	h := newManagedHarness(t, stateServiceDesc{methods: methods})
	h.pauseServer()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		errCh <- h.client.Call(ctx, stateTestService, "Wait", &req, &resp)
	}()
	<-entered
	close(release)
	h.serverGate.waitPending(t)

	// Make both outcomes simultaneously available: response becomes
	// deliverable and ctx fires, with no ordering between them.
	h.serverGate.permitNext(t)
	h.serverGate.setAuto(true)
	cancel()

	err := <-errCh
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled):
	default:
		t.Fatalf("err=%v, want nil or context.Canceled", err)
	}
	if ids := h.client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("leaked client streams: %v", ids)
	}
}

// TestContractUnaryClientClosedMidsend injects a deterministic transport
// error into the unary request write. The Call must surface ErrClosed
// (filtered transport failure) rather than hanging or a generic error.
func TestContractUnaryClientClosedMidsend(t *testing.T) {
	events := make(chan serverTestEvent, 64)
	server, err := NewServer(withServerTestHooks(&serverTestHooks{events: events}))
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterService(stateTestService, &ServiceDesc{Methods: map[string]Method{
		"Echo": func(context.Context, func(any) error) (any, error) {
			return &internal.EchoPayload{}, nil
		},
	}})
	serverRaw, clientRaw := memPipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runTestConn(ctx, server, &scriptedConn{Conn: serverRaw, gate: newWriteGate(true)})

	client := NewClient(&scriptedConn{Conn: clientRaw, gate: newWriteGate(true)},
		withClientTestHooks(&clientTestHooks{
			sendHook: func(uint32, messageType, uint8) error { return io.ErrClosedPipe },
		}))
	t.Cleanup(func() { client.Close() })

	var req, resp internal.EchoPayload
	err = client.Call(context.Background(), stateTestService, "Echo", &req, &resp)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v want ErrClosed", err)
	}
	if ids := client.clientStreamIDs(); len(ids) != 0 {
		t.Fatalf("client streams leaked: %v", ids)
	}
}
