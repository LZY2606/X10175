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
	"errors"
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
)

func TestMutationContractSuccessfulCloseSendIsIdempotent(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "ClientStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("first CloseSend: %v", err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeData)
	if err := stream.CloseSend(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("second CloseSend = %v, want ErrStreamClosed", err)
	}
}

func TestMutationContractFailedCloseSendDoesNotSetLocalClosed(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "ClientStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	h.clientNet.failNextWrite(errScriptedTransport)

	if err := stream.CloseSend(); !errors.Is(err, errScriptedTransport) {
		t.Fatalf("first CloseSend = %v, want transport error", err)
	}
	if err := stream.CloseSend(); !errors.Is(err, errScriptedTransport) {
		t.Fatalf("second CloseSend = %v, want original transport error", err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); !errors.Is(err, errScriptedTransport) {
		t.Fatalf("SendMsg after failed CloseSend = %v, want original transport error", err)
	}
}

func TestMutationContractServerStreamNotDeletedBeforeFinalFrame(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "ServerStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	h.serverNet.pauseWrites()
	h.serverNet.waitForPausedWrite(ctx)
	first := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	if first.flags != 0 || first.typeName != messageTypeData || first.streamID != 1 {
		t.Fatalf("unexpected first frame: %+v", first)
	}
	assertMapContains(t, h.conn.testStreamIDs(), 1)
	if h.conn.testActiveStreams() != 1 {
		t.Fatalf("active streams = %d, want 1", h.conn.testActiveStreams())
	}
	h.serverNet.allowOneWrite()

	var msg internal.EchoPayload
	if err := stream.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg first: %v", err)
	}
	h.serverNet.allowOneWrite()
	second := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	if second.flags != 0 {
		t.Fatalf("second frame flags = %#x, want 0", second.flags)
	}
	var secondMsg internal.EchoPayload
	if err := stream.RecvMsg(&secondMsg); err != nil {
		t.Fatalf("RecvMsg second: %v", err)
	}
	h.serverNet.waitForPausedWrite(ctx)
	final := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	h.waitEvent(t, ctx, "server-stream-closing", 1)
	assertMapContains(t, h.conn.testStreamIDs(), 1)
	if final.flags != flagRemoteClosed|flagNoData {
		t.Fatalf("final frame flags = %#x", final.flags)
	}
	h.serverNet.allowOneWrite()
	if err := stream.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("terminal RecvMsg = %v, want io.EOF", err)
	}
	h.waitEvent(t, ctx, "server-stream-closed", 1)
	assertMapMissing(t, h.conn.testStreamIDs(), 1)
}
