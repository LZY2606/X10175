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
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

// clientHarness wires a real Client to a scripted in-memory connection.
type clientHarness struct {
	conn *scriptedConn
	cl   *Client

	delivered chan string
	loopDone  chan error
}

func newClientHarness(t testing.TB) *clientHarness {
	t.Helper()
	conn := newScriptedConn("client")
	cl := NewClient(conn)
	h := &clientHarness{
		conn:      conn,
		cl:        cl,
		delivered: make(chan string, 1024),
		loopDone:  make(chan error, 1),
	}
	cl.onStreamDelivered = func(id streamID, depth int) {
		h.delivered <- "delivered"
	}
	cl.onReceiveLoopDone = func(err error) {
		h.loopDone <- err
	}
	t.Cleanup(func() {
		conn.Close()
		cl.Close()
	})
	return h
}

func (h *clientHarness) streamCount() int {
	h.cl.streamLock.RLock()
	n := len(h.cl.streams)
	h.cl.streamLock.RUnlock()
	return n
}

func (h *clientHarness) hasStream(id streamID) bool {
	h.cl.streamLock.RLock()
	_, ok := h.cl.streams[id]
	h.cl.streamLock.RUnlock()
	return ok
}

func (h *clientHarness) streamByID(id streamID) *stream {
	h.cl.streamLock.RLock()
	s := h.cl.streams[id]
	h.cl.streamLock.RUnlock()
	return s
}

// waitDeliveries blocks until n delivery observations have arrived.
func (h *clientHarness) waitDeliveries(t testing.TB, n int) {
	t.Helper()
	for range n {
		waitSignal(t, h.delivered, "client message delivery")
	}
}

// closeClient closes both halves and returns the receive loop terminal error.
func (h *clientHarness) closeClient(t testing.TB) error {
	h.conn.Close()
	h.cl.Close()
	return waitErrSignal(t, h.loopDone, "client receive loop exit")
}

// ---------------------------------------------------------------------------
// Server-side scripted harness
// ---------------------------------------------------------------------------

const (
	ssvc = "stateService"
)

type serverHarness struct {
	srv    *Server
	conn   *scriptedConn
	sc     *serverConn
	rec    *eventRecorder
	wg     sync.WaitGroup
	runErr chan error

	streamAdd      chan uint32
	streamDel      chan uint32
	finalFrameSent chan uint32
	dataQueued     chan string
	recvErr        chan error
	handlerDone    chan uint32
	connDone       chan struct{}
}

func newServerHarness(t testing.TB, remote *scriptedConn, streams map[string]Stream, methods map[string]Method) *serverHarness {
	t.Helper()
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	if streams != nil {
		srv.RegisterService(ssvc, &ServiceDesc{Streams: streams})
	}
	if methods != nil {
		srv.Register(ssvc, methods)
	}
	conn := newScriptedConn("server")
	if remote != nil {
		linkScriptedConns(conn, remote)
	}
	sc, err := srv.newConn(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &serverHarness{
		srv:            srv,
		conn:           conn,
		sc:             sc,
		rec:            newEventRecorder(),
		runErr:         make(chan error, 1),
		streamAdd:      make(chan uint32, 256),
		streamDel:      make(chan uint32, 256),
		finalFrameSent: make(chan uint32, 256),
		dataQueued:     make(chan string, 1024),
		recvErr:        make(chan error, 8),
		handlerDone:    make(chan uint32, 256),
		connDone:       make(chan struct{}, 1),
	}
	sc.onStreamAdd = func(id uint32) {
		h.rec.emit("add")
		h.streamAdd <- id
	}
	sc.onStreamDel = func(id uint32) {
		h.rec.emit("del")
		h.streamDel <- id
	}
	sc.onFinalFrameSent = func(id uint32) {
		h.rec.emit("finalsent")
		h.finalFrameSent <- id
	}
	sc.onDataQueued = func(id uint32, depth int) {
		h.dataQueued <- "queued"
	}
	sc.onRecvErr = func(err error) {
		h.recvErr <- err
	}
	sc.onHandlerDone = func(id uint32) {
		h.rec.emit("handlerdone")
		h.handlerDone <- id
	}
	sc.onConnDone = func() {
		h.rec.emit("conndone")
		close(h.connDone)
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.runErr <- sc.run(context.Background())
	}()

	t.Cleanup(func() {
		conn.Close()
		sc.close()
		h.wg.Wait()
		if err := srv.Close(); err != nil {
			t.Logf("server close: %v", err)
		}
	})
	return h
}

func (h *serverHarness) waitRun(t testing.TB) error {
	t.Helper()
	select {
	case err := <-h.runErr:
		return err
	case <-h.connDone:
		select {
		case err := <-h.runErr:
			return err
		default:
			return nil
		}
	}
}

// enqueueRequest is a helper for the common odd stream id request path.
func enqueueRequest(t testing.TB, h *serverHarness, sid uint32, method string, m proto.Message, flags uint8) {
	t.Helper()
	var payload []byte
	if m != nil {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		payload = b
	}
	req := &Request{Service: ssvc, Method: method, Payload: payload}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	h.conn.enqueueRaw(encodeFrame(sid, messageTypeRequest, flags, b))
}
