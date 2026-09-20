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
)

func TestHandlerReturnRacesWithServerShutdown(t *testing.T) {
	h := newContractHarness(t)
	release := make(chan struct{})
	h.server.RegisterService(contractService, &ServiceDesc{
		Streams: map[string]Stream{
			"Blocked": {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					if err := ss.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "before-shutdown"}); err != nil {
						return nil, err
					}
					<-release
					return nil, nil
				},
				StreamingServer: true,
			},
		},
	})
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingServer: true}, contractService, "Blocked", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	h.serverNet.pauseWrites()
	h.serverNet.waitForPausedWrite(ctx)
	h.serverNet.allowOneWrite()
	h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)

	var first internal.EchoPayload
	if err := stream.RecvMsg(&first); err != nil {
		t.Fatalf("first stream message: %v", err)
	}
	if first.Seq != 1 || first.Msg != "before-shutdown" {
		t.Fatalf("unexpected first stream message: %+v", first)
	}
	if h.conn.testActiveStreams() != 1 {
		t.Fatalf("active streams = %d, want 1", h.conn.testActiveStreams())
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- h.server.Shutdown(ctx) }()
	h.waitEvent(t, ctx, "shutdown-loop", 0)
	if h.server.countConnection() != 1 {
		t.Fatalf("shutdown retained connections = %d, want 1", h.server.countConnection())
	}
	if state, _ := h.conn.getState(); state != connStateActive {
		t.Fatalf("state during shutdown = %s, want active", state)
	}

	close(release)
	h.serverNet.waitForPausedWrite(ctx)
	h.serverNet.allowOneWrite()
	finalFrame := h.waitFrame(t, ctx, h.serverNet, 1, messageTypeData)
	if finalFrame.flags != flagRemoteClosed|flagNoData {
		t.Fatalf("final frame flags = %#x", finalFrame.flags)
	}
	h.waitEvent(t, ctx, "server-stream-closed", 1)
	if err := stream.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("final RecvMsg = %v, want io.EOF", err)
	}

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitDoneContext(t, ctx, h.conn.runDone, "connection after active handler completion")
	if h.server.countConnection() != 0 {
		t.Fatalf("connections after shutdown = %d, want 0", h.server.countConnection())
	}
}

func TestClientDisconnectCleansUpServerConnection(t *testing.T) {
	h := newContractHarness(t)
	started := make(chan struct{})
	release := make(chan struct{})
	h.server.RegisterService(contractService, &ServiceDesc{
		Streams: map[string]Stream{
			"Disconnect": {
				Handler: func(ctx context.Context, _ StreamServer) (any, error) {
					close(started)
					<-release
					return nil, ctx.Err()
				},
				StreamingClient: true,
			},
		},
	})
	ctx, cancel := h.testContext()
	defer cancel()

	stream, err := h.client.NewStream(ctx, &StreamDesc{StreamingClient: true}, contractService, "Disconnect", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitEvent(ctx, started, "handler")
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeData)
	h.waitEvent(t, ctx, "server-half-closed", 1)

	h.client.Close()
	waitDoneContext(t, ctx, h.conn.runDone, "server connection after client disconnect")
	if h.server.countConnection() != 0 {
		t.Fatalf("connections after disconnect = %d, want 0", h.server.countConnection())
	}
	close(release)
}

func TestDeterministicTransportReadError(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	h.clientNet.failRead(io.ErrUnexpectedEOF)
	errCh := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errCh <- h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{}, &resp)
	}()
	if err := waitError(ctx, errCh, "client transport read error"); !errors.Is(err, ErrClosed) {
		t.Fatalf("client read error = %v, want %v", err, ErrClosed)
	}
	waitDoneContext(t, ctx, h.client.userCloseWaitCh, "client run completion")
}

func TestDeterministicTransportWriteError(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	h.serverNet.failNextWrite(errScriptedTransport)
	errCh := make(chan error, 1)
	go func() {
		var resp internal.EchoPayload
		errCh <- h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 1}, &resp)
	}()
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	waitDoneContext(t, ctx, h.conn.runDone, "server after write failure")
}

func TestExtraFrameAfterFinalStatusIsRejected(t *testing.T) {
	h := newContractHarness(t)
	h.registerStandardService()
	ctx, cancel := h.testContext()
	defer cancel()

	var resp internal.EchoPayload
	if err := h.client.Call(ctx, contractService, "Unary", &internal.EchoPayload{Seq: 1}, &resp); err != nil {
		t.Fatal(err)
	}
	h.waitFrame(t, ctx, h.clientNet, 1, messageTypeRequest)
	h.waitFrame(t, ctx, h.serverNet, 1, messageTypeResponse)
	h.clientNet.queueFrame(h.dataFrame(t, 1, &internal.EchoPayload{Seq: 2, Msg: "extra"}, 0))
	h.waitTypedEvent(t, ctx, "client-inactive", 1, messageTypeData)
}

func waitError(ctx context.Context, ch <-chan error, name string) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		panic("timed out waiting for " + name)
	}
}
