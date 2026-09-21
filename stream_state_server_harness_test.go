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

	"google.golang.org/protobuf/proto"
)

const stateTestService = "stateService"

type protoMessage = proto.Message

type serverHarness struct {
	t       *testing.T
	log     *orderedEventLog
	server  *Server
	sc      *serverConn
	peer    *fakePeer
	streams *sync.Map
	cancel  context.CancelFunc
}

func newServerHarness(t *testing.T, desc *ServiceDesc) *serverHarness {
	t.Helper()
	silenceStandardLogger(t)

	srv, err := NewServer()
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.RegisterService(stateTestService, desc)

	sc, peerConn := newScriptConn(t)
	events := newOrderedEventLog()
	h := &serverHarness{
		t:      t,
		log:    events,
		server: srv,
		peer:   newFakePeer(t, peerConn),
	}
	withTestHooks(t, streamTestHooks{
		serverStreamDeleted: func(sid uint32, streams *sync.Map) {
			h.streams = streams
			events.record(evServerDeleted)
		},
		serverStreamTable: func(streams *sync.Map) {
			h.streams = streams
		},
		serverDataBlocked: func(sid uint32) { events.record(evServerBlocked) },
		serverConnExited:  func() { events.record(evServerExited) },
	})

	sconn, err := srv.newConn(sc, nil)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	h.sc = sconn
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go sconn.run(ctx)

	t.Cleanup(func() {
		cancel()
		_ = peerConn.Close()
		sc.Close()
		_ = srv.Close()
	})
	return h
}

func (h *serverHarness) sendRequest(sid uint32, method string, msg proto.Message, flags uint8) {
	f := mustMarshalRequest(h.t, stateTestService, method, msg, flags)
	f.header.StreamID = sid
	h.peer.send(f)
}

func (h *serverHarness) sendData(sid uint32, msg proto.Message, flags uint8) {
	f := mustMarshalData(h.t, msg, flags)
	f.header.StreamID = sid
	h.peer.send(f)
}

func (h *serverHarness) streamPresent(sid uint32) bool {
	if h.streams == nil {
		return false
	}
	_, ok := h.streams.Load(sid)
	return ok
}

func (h *serverHarness) streamCount() int {
	if h.streams == nil {
		return 0
	}
	n := 0
	h.streams.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (h *serverHarness) connState() connState {
	st, _ := h.sc.getState()
	return st
}

// cancelCtx cancels the connection context handed to serverConn.run,
// simulating a server-wide shutdown of in-flight work without closing the
// transport.
func (h *serverHarness) cancelCtx() {
	h.cancel()
}

var _ = net.Pipe
