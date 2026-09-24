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
	"google.golang.org/protobuf/proto"
)

type nopSender struct{ calls int }

func (n *nopSender) send(uint32, messageType, uint8, []byte) error {
	n.calls++
	return nil
}

func dataMsg(id streamID) *streamMessage {
	return &streamMessage{
		header:  messageHeader{StreamID: uint32(id), Type: messageTypeData},
		payload: []byte("payload"),
	}
}

// TestStreamReceiveStateMachine exercises the client-side stream receive
// contract directly, without a connection: cancellation while full, the
// queued-message-beats-close precedence, and sticky close errors.
func TestStreamReceiveStateMachine(t *testing.T) {
	t.Run("CancelWhileFullWinsBeforeTimeout", func(t *testing.T) {
		s := newStream(1, &nopSender{}, 1)
		if err := s.receive(context.Background(), dataMsg(1)); err != nil {
			t.Fatalf("first receive: %v", err)
		}

		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { done <- s.receive(ctx, dataMsg(1)) }()
		goscheds(5)

		// The only way out of the full queue (before the production
		// fallback) is ctx cancellation, which must report the exact error.
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

		// The stream is still usable: drain then receive again.
		<-s.recv
		if err := s.receive(context.Background(), dataMsg(1)); err != nil {
			t.Fatalf("receive after cancel: %v", err)
		}
		select {
		case <-s.recvClose:
			t.Fatal("a canceled receive must not close the stream")
		default:
		}
	})

	t.Run("QueuedMessageBeatsClose", func(t *testing.T) {
		s := newStream(1, &nopSender{}, 2)
		first := dataMsg(1)
		if err := s.receive(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		s.closeWithError(ErrStreamFull)

		// A message already queued stays readable even though the stream
		// is closed; recvErr only applies to subsequent deliveries.
		select {
		case <-s.recvClose:
		default:
			t.Fatal("stream should be closed")
		}
		select {
		case got := <-s.recv:
			if got != first {
				t.Fatal("wrong message delivered")
			}
		default:
			t.Fatal("queued message must remain readable after close")
		}
		if err := s.receive(context.Background(), dataMsg(1)); !errors.Is(err, ErrStreamFull) {
			t.Fatalf("expected sticky ErrStreamFull, got %v", err)
		}
	})

	t.Run("CloseWithErrorIsSticky", func(t *testing.T) {
		s := newStream(1, &nopSender{}, 1)
		terminal := errors.New("exact terminal error")
		s.closeWithError(terminal)
		// A later close must not overwrite the first recorded error.
		s.closeWithError(ErrClosed)
		if err := s.receive(context.Background(), dataMsg(1)); !errors.Is(err, terminal) {
			t.Fatalf("expected first sticky error, got %v", err)
		}
		if !errors.Is(s.recvErr, terminal) {
			t.Fatalf("recvErr overwritten: %v", s.recvErr)
		}
	})
}

// TestStreamHandlerStateMachine covers the server-side per-stream receive
// state: EOF after remote half-close, rejection of data once closed, ctx
// cancellation while the queue is full, and queued-message delivery.
func TestStreamHandlerStateMachine(t *testing.T) {
	t.Run("DataRejectedAfterRemoteClose", func(t *testing.T) {
		sh := &streamHandler{recv: make(chan Unmarshaler, 2)}
		sh.closeSend()
		sh.closeSend() // idempotent: must not panic on close(ch)

		var got internal.EchoPayload
		if err := sh.RecvMsg(&got); err != io.EOF {
			t.Fatalf("expected EOF after half-close, got %v", err)
		}
		if err := sh.data(func(any) error { return nil }); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("expected ErrStreamClosed for data after close, got %v", err)
		}
	})

	t.Run("CancelWhileQueueFull", func(t *testing.T) {
		sh := &streamHandler{recv: make(chan Unmarshaler, 1)}
		if err := sh.data(func(any) error { return nil }); err != nil {
			t.Fatal("first delivery failed")
		}
		ctx, cancel := context.WithCancel(context.Background())
		sh.ctx = ctx
		done := make(chan error, 1)
		go func() { done <- sh.data(func(any) error { return nil }) }()
		goscheds(5)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled for full queue, got %v", err)
		}

		// Drain and prove the handler still accepts deliveries.
		<-sh.recv
		if err := sh.data(func(any) error { return nil }); err != nil {
			t.Fatalf("delivery after drain: %v", err)
		}
	})

	t.Run("QueuedMessageDelivered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sh := &streamHandler{recv: make(chan Unmarshaler, 1), ctx: ctx}
		want := &internal.EchoPayload{Seq: 7, Msg: "queued"}
		payload := marshalProto(t, want)
		if err := sh.data(func(m any) error {
			return proto.Unmarshal(payload, m.(proto.Message))
		}); err != nil {
			t.Fatal(err)
		}

		var got internal.EchoPayload
		if err := sh.RecvMsg(&got); err != nil {
			t.Fatalf("RecvMsg: %v", err)
		}
		if got.Seq != 7 || got.Msg != "queued" {
			t.Fatalf("unexpected payload: %+v", got)
		}

		// With the queue drained and ctx canceled, the error is exact.
		cancel()
		if err := sh.RecvMsg(&got); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled after drain, got %v", err)
		}
	})
}
