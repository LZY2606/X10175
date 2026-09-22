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
	"net"
	"sync"
	"testing"
	"time"
)

const stateTestService = "stateService"

type stateServiceDesc struct {
	methods map[string]Method
	streams map[string]Stream
}

// managedHarness wires a real Server and Client over net.Pipe with gated
// writes on both sides and exposes deterministic server lifecycle events.
type managedHarness struct {
	t       *testing.T
	server  *Server
	client  *Client
	serverC *scriptedConn // server-side half of the pipe
	clientC *scriptedConn // client-side half of the pipe

	serverConn *serverConn // handle obtained from the conn-started event

	serverGate *writeGate // controls the server's outbound writes
	clientGate *writeGate // controls the client's outbound writes

	events    chan serverTestEvent
	recvBlock chan streamID

	connDone   chan struct{}
	closedOnce sync.Once
}

func newManagedHarness(t *testing.T, desc stateServiceDesc) *managedHarness {
	t.Helper()

	events := make(chan serverTestEvent, 4096)
	recvBlock := make(chan streamID, 64)

	server, err := NewServer(withServerTestHooks(&serverTestHooks{events: events}))
	if err != nil {
		t.Fatal(err)
	}
	if desc.methods != nil || desc.streams != nil {
		server.RegisterService(stateTestService, &ServiceDesc{
			Methods: desc.methods,
			Streams: desc.streams,
		})
	}

	serverRaw, clientRaw := memPipe()
	serverGate := newWriteGate(true)
	clientGate := newWriteGate(true)
	serverScripted := &scriptedConn{Conn: serverRaw, gate: serverGate}
	clientScripted := &scriptedConn{Conn: clientRaw, gate: clientGate}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h := &managedHarness{
		t:          t,
		server:     server,
		serverC:    serverScripted,
		clientC:    clientScripted,
		serverGate: serverGate,
		clientGate: clientGate,
		events:     events,
		recvBlock:  recvBlock,
		connDone:   make(chan struct{}),
	}

	runTestConn(ctx, server, serverScripted)

	client := NewClient(clientScripted, withClientTestHooks(&clientTestHooks{
		recvBlocked: recvBlock,
	}))
	h.client = client

	started := h.nextEvent()
	if started.kind != testEventConnStarted {
		t.Fatalf("expected conn-started, got %v", started.kind)
	}
	h.serverConn = started.conn

	t.Cleanup(h.cleanup)
	return h
}

// pauseServer/pauseClient switch a gate to manual mode; every write
// performed afterwards is parked until explicitly released.
func (h *managedHarness) pauseServer() { h.serverGate.setAuto(false) }
func (h *managedHarness) pauseClient() { h.clientGate.setAuto(false) }

func (h *managedHarness) nextEvent() serverTestEvent {
	return awaitEvent(h.t, h.events)
}

// waitConnDone blocks until the server connection goroutine exits.
func (h *managedHarness) waitConnDone() {
	h.t.Helper()
	select {
	case <-h.connDone:
	case <-time.After(testWaitTimeout):
		h.t.Fatal("server connection goroutine did not exit")
	}
}

// observeConnDone drains events until conn-done and closes connDone. It is
// started explicitly by tests that intend to tear the connection down.
func (h *managedHarness) observeConnDone() {
	go func() {
		for {
			ev := h.nextEvent()
			if ev.kind == testEventConnDone {
				close(h.connDone)
				return
			}
		}
	}()
}

func (h *managedHarness) cleanup() {
	h.closedOnce.Do(func() {
		h.client.Close()
	})
}

// wireHarness drives a real server connection with hand-built frames and
// reads frames back directly from the pipe. It provides exact control over
// frame order and flag combinations.
type wireHarness struct {
	t       *testing.T
	server  *Server
	serverC *scriptedConn // server-side half (writes gated)
	clientC net.Conn      // raw client-side half (tests write/read directly)

	serverGate *writeGate
	events     chan serverTestEvent
	serverConn *serverConn

	cancel context.CancelFunc
}

func newWireHarness(t *testing.T, desc stateServiceDesc) *wireHarness {
	t.Helper()

	events := make(chan serverTestEvent, 4096)
	server, err := NewServer(withServerTestHooks(&serverTestHooks{events: events}))
	if err != nil {
		t.Fatal(err)
	}
	if desc.methods != nil || desc.streams != nil {
		server.RegisterService(stateTestService, &ServiceDesc{
			Methods: desc.methods,
			Streams: desc.streams,
		})
	}

	serverRaw, clientRaw := memPipe()
	gate := newWriteGate(true)
	serverScripted := &scriptedConn{Conn: serverRaw, gate: gate}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runTestConn(ctx, server, serverScripted)

	h := &wireHarness{
		t:          t,
		server:     server,
		serverC:    serverScripted,
		clientC:    clientRaw,
		serverGate: gate,
		events:     events,
		cancel:     cancel,
	}
	started := h.nextEvent()
	if started.kind != testEventConnStarted {
		t.Fatalf("expected conn-started, got %v", started.kind)
	}
	h.serverConn = started.conn
	t.Cleanup(func() { clientRaw.Close() })
	return h
}

func (h *wireHarness) pause() { h.serverGate.setAuto(false) }

func (h *wireHarness) nextEvent() serverTestEvent { return awaitEvent(h.t, h.events) }

// readFrame reads a single raw frame from the client-side half.
func (h *wireHarness) readFrame() wireFrame { return readFrame(h.t, h.clientC) }

// sendFrame writes a raw frame from the "client" side.
func (h *wireHarness) sendFrame(hdr messageHeader, payload []byte) {
	writeRawFrame(h.t, h.clientC, hdr, payload)
}
