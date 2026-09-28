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

// Replayable client-side streaming state machine contract tests. Every
// scenario drives a fully scripted peer: writes pause for explicit release,
// reads replay queued frames/errors, and teardown is signalled through
// channels, so ordering assertions never rely on scheduling or sleeps.

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

const fsmService = "fsmService"

var (
	bidiDesc   = &StreamDesc{StreamingClient: true, StreamingServer: true}
	clientDesc = &StreamDesc{StreamingClient: true}
	serverDesc = &StreamDesc{StreamingServer: true}
	noneDesc   = &StreamDesc{}
)

var fsmTransportErr = errors.New("fsm: injected transport write failure")

func okResponse(sid uint32) (messageHeader, []byte) {
	return encodeResponseFrame(sid, &Response{
		Status:  status.New(codes.OK, "").Proto(),
		Payload: mustMarshalEcho(&internal.EchoPayload{Seq: 7}),
	})
}

func streamCloseFrame(sid uint32) (messageHeader, []byte) {
	return encodeDataFrame(sid, flagRemoteClosed|flagNoData, nil)
}

func recvInGoroutine(cs ClientStream, m proto.Message) <-chan error {
	done := make(chan error, 1)
	go func() { done <- cs.RecvMsg(m) }()
	return done
}

func sendInGoroutine(cs ClientStream, m proto.Message) <-chan error {
	done := make(chan error, 1)
	go func() { done <- cs.SendMsg(m) }()
	return done
}

func closeSendInGoroutine(cs ClientStream) <-chan error {
	done := make(chan error, 1)
	go func() { done <- cs.CloseSend() }()
	return done
}

// --- unary -----------------------------------------------------------------

func TestFSMUnarySuccess(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	var req, resp internal.EchoPayload
	req.Seq = 1
	frame, done := sc.callSync(t, fsmService, "Unary", &req, &resp)
	if frame.h.StreamID != 1 {
		t.Fatalf("first stream id = %d, want 1", frame.h.StreamID)
	}
	if sc.streamCount() != 1 {
		t.Fatalf("stream count before response = %d, want 1", sc.streamCount())
	}

	sc.conn.enqueueFrame(okResponse(1))
	waitCh(t, done, "unary Call to succeed")
	if resp.Seq != 7 {
		t.Fatalf("resp seq = %d, want 7", resp.Seq)
	}
	if sc.streamCount() != 0 {
		t.Fatalf("stream count after response = %d, want 0", sc.streamCount())
	}
}

// TestFSMUnaryPreCanceledContext: a context canceled before the response is
// queued yields context.Canceled; the deferred stream removal makes the late
// response frame get rejected while the connection stays usable.
func TestFSMUnaryPreCanceledContext(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var req, resp internal.EchoPayload
	go func() { done <- sc.client.Call(ctx, fsmService, "Unary", &req, &resp) }()
	w := sc.waitWrite(t)
	w.allow()
	if sc.streamCount() != 1 {
		t.Fatalf("want one live stream, got %d", sc.streamCount())
	}

	cancel()
	if err := waitCh(t, done, "canceled Call"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Call err = %v, want context.Canceled", err)
	}
	sc.conn.enqueueFrame(okResponse(1)) // late frame rejected; stream gone

	frame2, done2 := sc.callSync(t, fsmService, "Unary", &internal.EchoPayload{}, &internal.EchoPayload{})
	if frame2.h.StreamID != 3 {
		t.Fatalf("second unary stream id = %d, want 3", frame2.h.StreamID)
	}
	sc.conn.enqueueFrame(okResponse(3))
	if err := waitCh(t, done2, "second unary Call"); err != nil {
		t.Fatalf("second Call: %v", err)
	}
	if sc.streamCount() != 0 {
		t.Fatalf("streams leaked: %d", sc.streamCount())
	}
}

// TestFSMUnaryRequestWriteFailure pins the exact error and table state when
// the opening write fails: the injected error is returned verbatim (not
// collapsed to ErrClosed) and the registered stream is cleaned up once the
// receive loop exits after the peer is torn down.
func TestFSMUnaryRequestWriteFailure(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	done := make(chan error, 1)
	go func() {
		_, err := sc.client.NewStream(context.Background(), noneDesc, fsmService, "Unary", &internal.EchoPayload{})
		done <- err
	}()
	w := sc.waitWrite(t)
	w.fail(fsmTransportErr)
	if err := waitCh(t, done, "failed NewStream"); !errors.Is(err, fsmTransportErr) {
		t.Fatalf("NewStream err = %v, want injected %v", err, fsmTransportErr)
	}

	sc.conn.Close()
	waitCh(t, sc.runExited(), "client teardown after write failure")
	if sc.streamCount() != 0 {
		t.Fatalf("streams left after teardown: %d", sc.streamCount())
	}
}

// TestFSMUnaryExtraFrameAfterResponse: stray frames after the terminal
// response cannot resurrect the stream or break the connection.
func TestFSMUnaryExtraFrameAfterResponse(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	var req, resp internal.EchoPayload
	_, done := sc.callSync(t, fsmService, "Unary", &req, &resp)
	sc.conn.enqueueFrame(okResponse(1))
	if err := waitCh(t, done, "unary Call"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	sc.conn.enqueueFrame(encodeDataFrame(1, 0, mustMarshalEcho(&internal.EchoPayload{Seq: 99})))
	sc.conn.enqueueFrame(okResponse(1))

	_, done2 := sc.callSync(t, fsmService, "Unary", &internal.EchoPayload{}, &internal.EchoPayload{})
	sc.conn.enqueueFrame(okResponse(3))
	if err := waitCh(t, done2, "fresh unary Call after stray frames"); err != nil {
		t.Fatalf("second Call: %v", err)
	}
	if sc.streamCount() != 0 {
		t.Fatalf("streams leaked: %d", sc.streamCount())
	}
}

// TestFSMUnaryConcurrentCancelAndResponse pins the first-event-wins contract
// between user cancellation and the terminal status. Each subtest releases a
// single event while Recv is parked, so the observable result is exact.
func TestFSMUnaryConcurrentCancelAndResponse(t *testing.T) {
	t.Run("CancelFirstReturnsCanceledThenLateResponseRejected", func(t *testing.T) {
		sc := newScriptedClient(t)
		defer sc.close(t)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			var req, resp internal.EchoPayload
			done <- sc.client.Call(ctx, fsmService, "Unary", &req, &resp)
		}()
		w := sc.waitWrite(t)
		w.allow()

		cancel()
		if err := waitCh(t, done, "canceled parked Call"); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		sc.conn.enqueueFrame(okResponse(1))
		if sc.streamCount() != 0 {
			t.Fatalf("stream count = %d, want 0", sc.streamCount())
		}
	})

	t.Run("ResponseFirstReturnsResponseThenCancelIrrelevant", func(t *testing.T) {
		sc := newScriptedClient(t)
		defer sc.close(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		var resp internal.EchoPayload
		go func() {
			var req internal.EchoPayload
			done <- sc.client.Call(ctx, fsmService, "Unary", &req, &resp)
		}()
		w := sc.waitWrite(t)
		w.allow()

		sc.conn.enqueueFrame(okResponse(1))
		if err := waitCh(t, done, "successful parked Call"); err != nil {
			t.Fatalf("Call: %v", err)
		}
		if resp.Seq != 7 {
			t.Fatalf("resp seq = %d, want 7", resp.Seq)
		}
		cancel()
		if sc.streamCount() != 0 {
			t.Fatalf("stream count = %d, want 0", sc.streamCount())
		}
	})
}

// --- client-side CloseSend --------------------------------------------------

// TestFSMClientStreamingDoubleCloseSend verifies two consecutive CloseSend
// calls: the first emits the no-data close frame, the second is rejected
// locally with ErrStreamClosed and emits nothing.
func TestFSMClientStreamingDoubleCloseSend(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	cs, _ := sc.openStreamSync(t, bidiDesc, fsmService, "Echo", nil)

	first := closeSendInGoroutine(cs)
	w := sc.waitWrite(t)
	if w.frame.h.Type != messageTypeData || w.frame.h.Flags&flagRemoteClosed == 0 {
		t.Fatalf("first CloseSend frame = %+v, want data with remote-closed", w.frame.h)
	}
	if w.frame.h.Flags&flagNoData == 0 || w.frame.h.Length != 0 {
		t.Fatalf("CloseSend frame must carry no-data and zero length")
	}
	w.allow()
	if err := waitCh(t, first, "first CloseSend"); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}

	if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend err = %v, want ErrStreamClosed", err)
	}
	assertNotReady(t, sc.conn.pendingW, "a write from the second CloseSend")
}

// TestFSMMutationCloseSendFlagPublishedAfterWrite targets mutation M1: the
// local-closed flag must be published only after the close frame write
// completes. Observing the state while the successful write is parked must
// show the flag false; after release it must be true. Moving the flag update
// ahead of the send fails the mid-write assertion. A failed write keeps the
// flag false so the half-close remains retryable.
func TestFSMMutationCloseSendFlagPublishedAfterWrite(t *testing.T) {
	t.Run("FlagFalseWhileWriteParkedTrueAfter", func(t *testing.T) {
		sc := newScriptedClient(t)
		defer sc.close(t)

		cs, _ := sc.openStreamSync(t, bidiDesc, fsmService, "Echo", nil)

		done := closeSendInGoroutine(cs)
		w := sc.waitWrite(t)
		if cs.localClosed {
			t.Fatalf("localClosed observed true while close write still parked; flag must be published only after the send completes")
		}
		w.allow()
		if err := waitCh(t, done, "CloseSend"); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		if !cs.localClosed {
			t.Fatalf("localClosed observed false after successful close write")
		}
		if err := cs.CloseSend(); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("second CloseSend err = %v, want ErrStreamClosed", err)
		}
	})

	t.Run("FailedWriteKeepsFlagUnset", func(t *testing.T) {
		sc := newScriptedClient(t)
		defer sc.close(t)

		cs, _ := sc.openStreamSync(t, bidiDesc, fsmService, "Echo", nil)
		done := closeSendInGoroutine(cs)
		w := sc.waitWrite(t)
		w.fail(fsmTransportErr)
		if err := waitCh(t, done, "failed CloseSend"); !errors.Is(err, fsmTransportErr) {
			t.Fatalf("CloseSend err = %v, want injected transport error verbatim", err)
		}
		if cs.localClosed {
			t.Fatalf("localClosed must remain false after a failed close write so the half-close is retryable")
		}
	})
}

// TestFSMClientStreamingCloseSendWithoutData verifies half-close with no
// preceding data message emits exactly one no-data, remote-closed data frame.
func TestFSMClientStreamingCloseSendWithoutData(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	cs, openFrame := sc.openStreamSync(t, clientDesc, fsmService, "Upload", nil)
	if openFrame.h.Flags != flagRemoteOpen {
		t.Fatalf("opening flags = %d, want flagRemoteOpen(%d)", openFrame.h.Flags, flagRemoteOpen)
	}

	done := closeSendInGoroutine(cs)
	w := sc.waitWrite(t)
	if w.frame.h.Type != messageTypeData || w.frame.h.Flags != flagRemoteClosed|flagNoData || w.frame.h.Length != 0 {
		t.Fatalf("half-close frame = %+v, want zero-length data flags remoteClosed|noData", w.frame.h)
	}
	w.allow()
	if err := waitCh(t, done, "half close"); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	sc.conn.enqueueFrame(okResponse(1))
	var resp internal.EchoPayload
	if err := cs.RecvMsg(&resp); err != nil {
		t.Fatalf("RecvMsg final status: %v", err)
	}
	if resp.Seq != 7 {
		t.Fatalf("resp seq = %d, want 7", resp.Seq)
	}
	if sc.streamCount() != 0 {
		t.Fatalf("stream count after final status = %d, want 0", sc.streamCount())
	}
}

// TestFSMClientStreamingSendAfterCloseRejected: no data frame is emitted
// after CloseSend succeeded.
func TestFSMClientStreamingSendAfterCloseRejected(t *testing.T) {
	sc := newScriptedClient(t)
	defer sc.close(t)

	cs, _ := sc.openStreamSync(t, bidiDesc, fsmService, "Echo", nil)
	done := closeSendInGoroutine(cs)
	w := sc.waitWrite(t)
	w.allow()
	if err := waitCh(t, done, "CloseSend"); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendMsg after CloseSend err = %v, want ErrStreamClosed", err)
	}
	assertNotReady(t, sc.conn.pendingW, "a data frame after CloseSend")
}
