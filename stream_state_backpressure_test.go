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
	"testing"

	"github.com/containerd/ttrpc/internal"
)

type contractSender struct{}

func (contractSender) send(uint32, messageType, uint8, []byte) error { return nil }

func TestClientQueueFullCancellationDoesNotEnqueueOrCloseStream(t *testing.T) {
	s := newStream(1, contractSender{}, streamRecvBufferSize)
	for range streamRecvBufferSize {
		s.recv <- &streamMessage{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := &streamMessage{header: messageHeader{StreamID: 1, Type: messageTypeData}}
	if err := s.receive(ctx, msg); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive on full queue = %v, want context.Canceled", err)
	}

	select {
	case <-s.recvClose:
		t.Fatal("full queue cancellation unexpectedly closed the stream")
	default:
	}
	if len(s.recv) != streamRecvBufferSize {
		t.Fatalf("queue length after canceled delivery = %d, want %d", len(s.recv), streamRecvBufferSize)
	}
}

func TestQueuedFinalStatusWinsOverAlreadyCanceledContext(t *testing.T) {
	c := &Client{codec: codec{}, streams: make(map[streamID]*stream)}
	s := newStream(1, contractSender{}, 1)
	c.streams[1] = s

	payload := framePayload(t, &internal.EchoPayload{Seq: 23, Msg: "queued"})
	respPayload := framePayload(t, &Response{Payload: payload})
	s.recv <- &streamMessage{
		header:  messageHeader{StreamID: 1, Type: messageTypeResponse, Length: uint32(len(respPayload))},
		payload: respPayload,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cs := &clientStream{ctx: ctx, s: s, c: c, desc: &StreamDesc{}}

	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("queued final status must win over canceled context: %v", err)
	}
	if got.Seq != 23 || got.Msg != "queued" {
		t.Fatalf("unexpected queued response: %+v", got)
	}
	assertClientStreamMissing(t, c, 1)
}

func TestQueuedStreamDataWinsOverAlreadyCanceledContext(t *testing.T) {
	c := &Client{codec: codec{}, streams: make(map[streamID]*stream)}
	s := newStream(1, contractSender{}, 1)
	c.streams[1] = s
	payload := framePayload(t, &internal.EchoPayload{Seq: 7, Msg: "queued-data"})
	s.recv <- &streamMessage{
		header:  messageHeader{StreamID: 1, Type: messageTypeData, Length: uint32(len(payload))},
		payload: payload,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cs := &clientStream{ctx: ctx, s: s, c: c, desc: &StreamDesc{StreamingServer: true}}

	var got internal.EchoPayload
	if err := cs.RecvMsg(&got); err != nil {
		t.Fatalf("queued data must win over canceled context: %v", err)
	}
	if got.Seq != 7 || got.Msg != "queued-data" {
		t.Fatalf("unexpected queued data: %+v", got)
	}
	assertClientStreamPresent(t, c, 1)
}

func TestServerQueueFullCancellationReturnsToReceiver(t *testing.T) {
	sh := &streamHandler{recv: make(chan Unmarshaler, streamRecvBufferSize)}
	for range streamRecvBufferSize {
		sh.recv <- func(any) error { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sh.ctx = ctx

	err := sh.data(func(any) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("server data on full queue = %v, want context.Canceled", err)
	}
	if len(sh.recv) != streamRecvBufferSize {
		t.Fatalf("server queue length after canceled delivery = %d, want %d", len(sh.recv), streamRecvBufferSize)
	}
}
