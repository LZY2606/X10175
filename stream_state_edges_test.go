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
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
)

func TestBidiStateContract(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, contractService, "Bidi", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)

	if err := stream.SendMsg(&internal.EchoPayload{Seq: 40, Msg: "ping"}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	dataFrame := h.waitFrame(t, ctx, h.clientNet, 1, messageTypeData)
	if dataFrame.flags != 0 {
		t.Fatalf("data flags = %#x, want 0", dataFrame.flags)
	}
	echoFrame := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	if echoFrame.flags != 0 {
		t.Fatalf("echo flags = %#x, want 0", echoFrame.flags)
	}
	if h.conn.testActiveStreams() != 1 {
		t.Fatalf("active streams = %d, want 1", h.conn.testActiveStreams())
	}

	var echo internal.EchoPayload
	if err := stream.RecvMsg(&echo); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if echo.Seq != 41 || echo.Msg != "ping" {
		t.Fatalf("unexpected echo: %+v", echo)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	halfClose := h.waitFrame(t, ctx, h.clientNet, 1, messageTypeData)
	if halfClose.flags != flagRemoteClosed|flagNoData {
		t.Fatalf("half-close flags = %#x", halfClose.flags)
	}
	h.waitEvent(t, ctx, "server-half-closed", 1)

	finalFrame := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	if finalFrame.flags != flagRemoteClosed|flagNoData {
		t.Fatalf("final frame flags = %#x", finalFrame.flags)
	}
	h.waitEvent(t, ctx, "server-stream-closed", 1)
	if err := stream.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("final RecvMsg = %v, want io.EOF", err)
	}

	lateData := h.dataFrame(t, 1, &internal.EchoPayload{Seq: 99, Msg: "late"}, 0)
	h.serverNet.queueFrame(lateData)
	h.waitEvent(t, ctx, "server-data-rejected", 1)
	rejection := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
	resp := h.decodeResponse(t, rejection)
	if resp.GetStatus().GetCode() != int32(codes.InvalidArgument) {
		t.Fatalf("late frame status = %v, want InvalidArgument", resp.GetStatus())
	}
	h.waitEvent(t, ctx, "client-inactive", 1)
	assertMapMissing(t, h.conn.testStreamIDs(), 1)
	assertClientStreamMissing(t, h.client, 1)
}

func TestStreamIDReuseRequestIsRejected(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	var firstResp internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 1}, &firstResp); err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
	reused := h.requestFrame(t, 1, &StreamDesc{}, "Unary", &internal.EchoPayload{Seq: 2})
	h.serverNet.queueFrame(reused)

	rejection := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
	resp := h.decodeResponse(t, rejection)
	if resp.GetStatus().GetCode() != int32(codes.InvalidArgument) {
		t.Fatalf("reused stream id status = %v, want InvalidArgument", resp.GetStatus())
	}
}
