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
	"strings"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

// waitResponseFrameFrom waits for a recorded response frame from side c
// matching stream id and asserts the status message contains wantMsg.
func (h *stateHarness) waitResponseFrom(c *scriptedConn, sid uint32, want codes.Code, wantMsg string) *Response {
	h.t.Helper()
	name := c.name
	h.sink.waitFor(h.t, 0, func(e hEvent) bool {
		return e.kind == evFrame && e.from == name && e.id == sid && e.msg == messageTypeResponse.String()
	})
	fs := framesFrom(c, sid, messageTypeResponse)
	f := fs[len(fs)-1]
	resp, err := decodeResponse(f)
	if err != nil {
		h.t.Fatal(err)
	}
	st := gstatus.FromProto(resp.Status)
	if st.Code() != want {
		h.t.Fatalf("sid %d code = %s, want %s (msg=%q)", sid, st.Code(), want, st.Message())
	}
	if wantMsg != "" && !strings.Contains(st.Message(), wantMsg) {
		h.t.Fatalf("sid %d message = %q, want substring %q", sid, st.Message(), wantMsg)
	}
	return resp
}

// Stream id rules: odd-only client ids, strictly increasing per
// connection, and data frames on unknown/retired ids are rejected with
// InvalidArgument statuses sent back on the offending id.
func TestStateStreamIDRules(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	// sid 1: legal, opens an Upload stream and finishes normally, so the
	// id becomes retired.
	injectRequest(h.wire, 1, flagRemoteOpen, svcState, mUpload, nil)
	h.waitResponseFrom(h.wire, 1, codes.OK, "")
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evReg && e.id == 1 })
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evUnreg && e.id == 1 })

	// Reused (non-increasing) id: rejected with the reuse message.
	injectRequest(h.wire, 1, flagRemoteClosed, svcState, mUnary, &internal.EchoPayload{Seq: 1})
	h.waitResponseFrom(h.wire, 1, codes.InvalidArgument, "re-used")

	// Even id: rejected with the odd-only message (checked before reuse).
	injectRequest(h.wire, 4, flagRemoteClosed, svcState, mUnary, &internal.EchoPayload{Seq: 1})
	h.waitResponseFrom(h.wire, 4, codes.InvalidArgument, "odd")

	// Another legal, higher, odd id works.
	injectRequest(h.wire, 3, flagRemoteOpen, svcState, mUpload, nil)
	h.waitResponseFrom(h.wire, 3, codes.OK, "")

	// Data frame on a never-used id is rejected.
	injectData(h.wire, 5, flagRemoteClosed|flagNoData, nil)
	h.waitResponseFrom(h.wire, 5, codes.InvalidArgument, "no longer active")

	// Data frame on the retired sid 1 is rejected too.
	injectData(h.wire, 1, flagRemoteClosed|flagNoData, nil)
	h.waitResponseFrom(h.wire, 1, codes.InvalidArgument, "no longer active")

	// The connection remains healthy: a real client call still works.
	var req, resp internal.EchoPayload
	req.Seq = 10
	if err := h.client.Call(context.Background(), svcState, mUnary, &req, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 11 {
		t.Fatalf("post-rule call seq = %d", resp.Seq)
	}
}

// Context cancellation arriving before the remote terminal status: the
// call observes context.Canceled and the later status is dropped as an
// orphan; connection and stream set end up clean.
func TestStateCancelBeforeStatus(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		errc <- h.client.Call(ctx, svcState, mHoldUnary, &req, &resp)
	}()
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evCreated && e.id == 1 })

	cancel()
	select {
	case err := <-errc:
		mustErrIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("call did not return after cancellation")
	}

	// Release the server handler; the late response hits a deleted stream.
	h.releaseBlock()
	h.sink.waitFor(t, 0, func(e hEvent) bool {
		return e.kind == evOrphan && e.from == "client" && e.id == 1
	})
}

// Remote terminal status arriving before cancellation is observed: the
// call returns the status value and the later cancel is inert.
func TestStateStatusBeforeCancel(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		req.Seq = 21
		errc <- h.client.Call(ctx, svcState, mHoldUnary, &req, &resp)
	}()
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evCreated && e.id == 1 })

	h.releaseBlock()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("call returned %v, want nil status-first", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not return after status released")
	}
	cancel()
}

// When context cancellation and a remote frame are simultaneously ready,
// the select is free to choose, but the outcome must be exactly one of
// the two defined results and must never corrupt the stream set. The
// invariant is checked over many replays.
func TestStateCancelStatusRaceInvariant(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		h := newStateHarness(t)

		stream, err := h.client.NewStream(context.Background(), descBidi, svcState, mChat, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
			t.Fatal(err)
		}
		// Wait until the echoed frame is already buffered in the stream.
		h.sink.waitFor(t, 0, func(e hEvent) bool {
			return e.kind == evFrame && e.from == "server" && e.id == 1 && e.msg == messageTypeData.String()
		})

		_, cancel := context.WithCancel(context.Background())
		cancel()
		var p internal.EchoPayload
		err = stream.RecvMsg(&p)
		switch {
		case err == nil:
			// Frame won: it must be the exact echoed payload.
			if p.Seq != 2 {
				t.Fatalf("iter %d frame result seq = %d, want 2", iter, p.Seq)
			}
		case err == context.Canceled:
			// Cancellation won: stream still registered and the buffered
			// frame remains readable on a fresh attempt.
			if !h.clientHasStream(1) {
				t.Fatalf("iter %d stream vanished on cancellation", iter)
			}
			var p2 internal.EchoPayload
			if err := stream.RecvMsg(&p2); err != nil {
				t.Fatalf("iter %d post-cancel read: %v", iter, err)
			}
		default:
			t.Fatalf("iter %d undefined race outcome: %v", iter, err)
		}

		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		var end internal.EchoPayload
		_ = stream.RecvMsg(&end)
		h.cleanup()
	}
}

// Mutation contract #2: the server must keep a stream in its map until
// its terminal closeStream response has actually been written. A late
// data frame arriving in that window is a per-stream data handling error;
// deleting the map entry early collapses it into the generic
// "no longer active" rejection. Run with the production code — swap the
// order in serverConn.run to confirm this test fails.
func TestMutationContractMapDeleteOrder(t *testing.T) {
	h := newStateHarness(t)
	defer h.cleanup()

	// Hold the terminal Upload response on the server wire.
	h.remote.holdNextWrites(1)

	cs, err := h.client.NewStream(context.Background(), descCS, svcState, mUpload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 7}); err != nil {
		t.Fatal(err)
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatal(err)
	}

	// Handler has finished (terminal response is enqueued), but the frame
	// is parked in the write gate.
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evHandler && e.id == 1 })
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evGateHold && e.from == "server" })

	// Inject a late data frame from the raw client during this window.
	injectData(h.wire, 1, flagRemoteClosed|flagNoData, nil)

	// Healthy: data-on-live-stream path -> "data handling error".
	// Early-delete mutation: unknown id path -> "no longer active".
	h.waitResponseFrom(h.wire, 1, codes.InvalidArgument, "data handling error")

	if !h.remote.releaseWrite() {
		t.Fatal("terminal frame missing")
	}
	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("terminal RecvMsg: %v", err)
	}
}
