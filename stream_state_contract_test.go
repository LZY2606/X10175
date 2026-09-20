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
)

func TestStreamingStateContracts(t *testing.T) {
	t.Run("UnarySuccessCleansStreamAfterFinalResponse", func(t *testing.T) {
		h := newContractHarness(t)
		h.registerStandardService()
		ctx, cancel := h.testContext()
		defer cancel()

		id := streamID(1)
		assertClientStreamMissing(t, h.client, id)
		h.serverNet.pauseWrites()
		errCh := make(chan error, 1)
		go func() {
			var resp internal.EchoPayload
			errCh <- h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 7}, &resp)
		}()

		request := h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
		if request.flags != 0 {
			t.Fatalf("unary request flags = %#x, want 0", request.flags)
		}
		assertClientStreamPresent(t, h.client, id)
		h.serverNet.waitForPausedWrite(ctx)
		h.serverNet.allowOneWrite()
		response := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
		if err := <-errCh; err != nil {
			t.Fatalf("unary call: %v", err)
		}
		h.waitEvent(t, ctx, "client-delivered", 1)
		assertClientStreamMissing(t, h.client, id)
		if h.conn.testActiveStreams() != 0 {
			t.Fatalf("active streams = %d, want 0", h.conn.testActiveStreams())
		}
		_ = response

		resp := h.decodeResponse(t, response)
		if resp.GetStatus().GetCode() != int32(codes.OK) {
			t.Fatalf("unexpected response status: %v", resp.GetStatus())
		}
	})

	t.Run("UnaryFinalStatusWinsWhenDeliveredBeforeCancellation", func(t *testing.T) {
		h := newContractHarness(t)
		h.registerStandardService()
		ctx, cancel := h.testContext()
		defer cancel()

		h.serverNet.pauseWrites()
		errCh := make(chan error, 1)
		go func() {
			var resp internal.EchoPayload
			errCh <- h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 1}, &resp)
		}()
		h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
		h.serverNet.waitForPausedWrite(ctx)
		h.serverNet.allowOneWrite()
		h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
		h.waitEvent(t, ctx, "client-delivered", 1)
		cancel()

		if err := <-errCh; err != nil {
			t.Fatalf("delivered final status must remain observable: %v", err)
		}
		assertClientStreamMissing(t, h.client, 1)
	})

	t.Run("UnaryCancellationWinsBeforeFinalStatusDelivery", func(t *testing.T) {
		h := newContractHarness(t)
		h.registerStandardService()
		ctx, cancel := h.testContext()
		releaseCtx, releaseCancel := h.testContext()
		defer cancel()
		defer releaseCancel()

		h.serverNet.pauseWrites()
		errCh := make(chan error, 1)
		go func() {
			var resp internal.EchoPayload
			errCh <- h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 1}, &resp)
		}()
		h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
		h.serverNet.waitForPausedWrite(releaseCtx)
		cancel()
		if err := <-errCh; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

		h.serverNet.allowOneWrite()
		h.waitFrame(t, releaseCtx, h.serverNet, 1, messageTypeResponse)
		h.waitEvent(t, releaseCtx, "client-inactive", 1)
		assertClientStreamMissing(t, h.client, 1)
	})

	t.Run("ClientStreamImmediateHalfCloseAndDuplicateCloseSend", func(t *testing.T) {
		h := newContractHarness(t)
		h.registerStandardService()
		ctx, cancel := h.testContext()
		defer cancel()

		stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "ClientStream", nil)
		if err != nil {
			t.Fatal(err)
		}
		request := h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
		if request.flags != flagRemoteOpen {
			t.Fatalf("open flags = %#x, want %#x", request.flags, flagRemoteOpen)
		}
		h.serverNet.pauseWrites()

		if err := stream.CloseSend(); err != nil {
			t.Fatalf("first CloseSend: %v", err)
		}
		if err := stream.CloseSend(); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("duplicate CloseSend: %v", err)
		}

		halfClose := h.waitFrame(t, ctx, h.clientNet, 1, messageTypeData)
		if halfClose.flags != flagRemoteClosed|flagNoData || len(halfClose.payload) != 0 {
			t.Fatalf("half-close frame flags=%#x payloadLen=%d", halfClose.flags, len(halfClose.payload))
		}
		h.waitEvent(t, ctx, "server-half-closed", 1)
		h.serverNet.waitForPausedWrite(ctx)
		finalResponse := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
		assertMapContains(t, h.conn.testStreamIDs(), 1)
		if h.conn.testActiveStreams() != 1 {
			t.Fatalf("active streams = %d, want 1", h.conn.testActiveStreams())
		}
		h.serverNet.allowOneWrite()
		_ = finalResponse

		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("final client-streaming response: %v", err)
		}
		if resp.Seq != 0 || resp.Msg != "client-stream-final" {
			t.Fatalf("unexpected final payload: %+v", resp)
		}
		assertMapMissing(t, h.conn.testStreamIDs(), 1)
		assertClientStreamMissing(t, h.client, 1)
		if err := stream.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
			t.Fatalf("Recv after final response = %v, want io.EOF", err)
		}
	})

	t.Run("ServerStreamingDataThenFinalEOFAndCleanup", func(t *testing.T) {
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
		h.serverNet.allowOneWrite()
		if first.flags != 0 {
			t.Fatalf("first data flags = %#x, want 0", first.flags)
		}
		h.waitEvent(t, ctx, "client-delivered", 1)
		assertMapContains(t, h.conn.testStreamIDs(), 1)
		assertClientStreamPresent(t, h.client, 1)

		var firstMsg internal.EchoPayload
		if err := stream.RecvMsg(&firstMsg); err != nil {
			t.Fatalf("first RecvMsg: %v", err)
		}
		if firstMsg.Seq != 1 || firstMsg.Msg != "server-stream" {
			t.Fatalf("unexpected first message: %+v", firstMsg)
		}

		h.serverNet.waitForPausedWrite(ctx)
		h.serverNet.allowOneWrite()
		secondFrame := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
		if secondFrame.flags != 0 {
			t.Fatalf("second frame flags = %#x, want 0", secondFrame.flags)
		}

		var secondMsg internal.EchoPayload
		if err := stream.RecvMsg(&secondMsg); err != nil {
			t.Fatalf("second RecvMsg: %v", err)
		}
		if secondMsg.Seq != 2 || secondMsg.Msg != "server-stream" {
			t.Fatalf("unexpected second message: %+v", secondMsg)
		}

		h.serverNet.waitForPausedWrite(ctx)
		finalFrame := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
		if finalFrame.flags != flagRemoteClosed|flagNoData {
			t.Fatalf("final frame flags = %#x, want %#x", finalFrame.flags, flagRemoteClosed|flagNoData)
		}
		h.serverNet.allowOneWrite()
		if err := stream.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
			t.Fatalf("terminal RecvMsg = %v, want io.EOF", err)
		}
		h.waitEvent(t, ctx, "server-stream-closed", 1)
		assertMapMissing(t, h.conn.testStreamIDs(), 1)
		assertClientStreamMissing(t, h.client, 1)
		if h.conn.testActiveStreams() != 0 {
			t.Fatalf("active streams = %d, want 0", h.conn.testActiveStreams())
		}
	})
}
