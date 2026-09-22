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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

func statusFromProto(p *rpcstatus.Status) *gstatus.Status {
	return gstatus.FromProto(p)
}

// This file contains replayable state machine tests for the four RPC
// shapes (unary, client-streaming, server-streaming, bidirectional) and
// for the interacting pieces of stream state: local send-close, remote
// terminal status, removal from the connection's stream set, backpressure
// blocking, and server shutdown.
//
// All progress is driven explicitly through scriptedConn gates or
// happens-before hook events; no test goroutine relies on scheduling
// order or time.Sleep. Durations only appear as failure backstops.

const (
	svcState   = "stateService"
	mUnary     = "Unary"
	mHoldUnary = "HoldUnary"
	mUpload    = "Upload"
	mDownload  = "Download"
	mChat      = "Chat"
	mErrorChat = "ErrorChat"
	mBlockChat = "BlockChat"
	mSlowUp    = "SlowUpload"
)

// --- event sink ---------------------------------------------------------

type evType string

const (
	evCreated   evType = "stream-created"
	evDeleted   evType = "stream-deleted"
	evOrphan    evType = "orphan-message"
	evBlocked   evType = "recv-blocked"
	evLoopDone  evType = "client-loop-done"
	evConnStart evType = "conn-start"
	evConnDone  evType = "conn-done"
	evState     evType = "conn-state"
	evReg       evType = "stream-registered"
	evUnreg     evType = "stream-unregistered"
	evHandler   evType = "handler-done"
	evOrphanSrv evType = "server-orphan-frame"
	evDropped   evType = "server-data-dropped"
	evRecvDone  evType = "server-recvloop-done"
	evFrame     evType = "wire-frame"
	evGateHold  evType = "write-gated"
)

type hEvent struct {
	kind evType
	from string // "client" | "server"
	id   uint32
	msg  string
	mtype messageType
	code codes.Code
}

type eventSink struct {
	mu     sync.Mutex
	events []hEvent
	notify chan struct{}
}

func newEventSink() *eventSink {
	return &eventSink{notify: make(chan struct{})}
}

func (s *eventSink) push(e hEvent) {
	s.mu.Lock()
	s.events = append(s.events, e)
	ch := s.notify
	s.mu.Unlock()
	// Non-blocking edge: waiters coalesce, and waitFor always rechecks
	// the full event prefix after being woken.
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *eventSink) snapshot() []hEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]hEvent, len(s.events))
	copy(out, s.events)
	return out
}

func (s *eventSink) notified() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// waitFor waits for a predicate to hold over the event prefix starting at
// cursor; on success it returns the matched event and the next cursor.
func (s *eventSink) waitFor(t testing.TB, cursor int, pred func(hEvent) bool) (hEvent, int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		for i := cursor; i < len(s.events); i++ {
			if pred(s.events[i]) {
				e := s.events[i]
				s.mu.Unlock()
				return e, i + 1
			}
		}
		s.mu.Unlock()
		if !time.Now().Before(deadline) {
			s.mu.Lock()
			t.Fatalf("timed out waiting for event after %d events; tail=%+v", cursor, tail(s.events))
		}
		select {
		case <-s.notified():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func tail(ev []hEvent) []hEvent {
	if len(ev) > 10 {
		return ev[len(ev)-10:]
	}
	return ev
}

// assertSoon checks that no event satisfying pred exists within the
// current snapshot. Because every state transition in the tests is
// explicit, callers invoke this only after the ordering that makes the
// event impossible has already been observed.
func (s *eventSink) assertAbsent(t testing.TB, pred func(hEvent) bool) {
	t.Helper()
	for _, e := range s.snapshot() {
		if pred(e) {
			t.Fatalf("unexpected event present: %+v", e)
		}
	}
}

// --- harness ------------------------------------------------------------

type stateHarness struct {
	t      *testing.T
	wire   *scriptedConn
	remote *scriptedConn
	sink   *eventSink
	server *Server
	client *Client
	conn   *serverConn

	blockStarted       atomic.Bool
	blockRelease       chan struct{}
	blockReleaseOnce   sync.Once
	blockEntered       chan struct{}
	expectServerStreams bool
}

func newStateHarness(t *testing.T) *stateHarness {
	t.Helper()
	sink := newEventSink()
	pair := newScriptedPair(
		func(from string, f wireFrame) {
			sink.push(hEvent{kind: evFrame, from: from, id: f.header.StreamID, mtype: f.header.Type, msg: frameKindString(f.header)})
		},
		func(from string) {
			sink.push(hEvent{kind: evGateHold, from: from})
		},
	)

	h := &stateHarness{
		t:             t,
		wire:          pair.client,
		remote:        pair.server,
		sink:          sink,
		blockRelease:  make(chan struct{}),
		blockEntered:  make(chan struct{}, 1),
	}

	ch := &clientHooks{
		streamCreated: func(id streamID) { sink.push(hEvent{kind: evCreated, from: "client", id: uint32(id)}) },
		streamDeleted: func(id streamID) { sink.push(hEvent{kind: evDeleted, from: "client", id: uint32(id)}) },
		orphanMessage: func(id streamID, mt messageType) {
			sink.push(hEvent{kind: evOrphan, from: "client", id: uint32(id), mtype: mt})
		},
		receiveBlocked: func(id streamID) { sink.push(hEvent{kind: evBlocked, from: "client", id: uint32(id)}) },
		receiveLoopDone: func() { sink.push(hEvent{kind: evLoopDone, from: "client"}) },
		messageDelivered: func(id streamID, mt messageType, flags uint8, ok bool) {
			if !ok {
				sink.push(hEvent{kind: evDropped, from: "client", id: uint32(id), mtype: mt})
			}
		},
	}

	sh := &serverHooks{
		connStart:        func(c *serverConn) { h.conn = c; sink.push(hEvent{kind: evConnStart, from: "server"}) },
		connDone:         func(*serverConn) { sink.push(hEvent{kind: evConnDone, from: "server"}) },
		connState:        func(_ *serverConn, st connState) { sink.push(hEvent{kind: evState, from: "server", msg: st.String()}) },
		streamRegistered: func(id uint32) { sink.push(hEvent{kind: evReg, from: "server", id: id}) },
		streamDeleted:    func(id uint32) { sink.push(hEvent{kind: evUnreg, from: "server", id: id}) },
		handlerDone:      func(id uint32) { sink.push(hEvent{kind: evHandler, from: "server", id: id}) },
		orphanFrame:      func(id uint32, mt messageType) { sink.push(hEvent{kind: evOrphanSrv, from: "server", id: id, mtype: mt}) },
		dataDropped:      func(id uint32, err error) { sink.push(hEvent{kind: evDropped, from: "server", id: id, msg: err.Error()}) },
		recvLoopDone:     func() { sink.push(hEvent{kind: evRecvDone, from: "server"}) },
	}

	server, err := NewServer(withTestServerHooks(sh))
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterService(svcState, h.serviceDesc())

	h.server = server
	h.conn = server.serveConnTest(pair.server)

	// Wait for the server connection goroutine to register itself.
	if _, c := sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evConnStart }); c <= 0 {
		t.Fatal("server connection never started")
	}

	h.client = NewClient(pair.client, withTestClientHooks(ch))
	return h
}

func (h *stateHarness) releaseBlock() {
	h.blockReleaseOnce.Do(func() { close(h.blockRelease) })
}

func (h *stateHarness) waitEntered() {
	h.t.Helper()
	select {
	case <-h.blockEntered:
	case <-time.After(5 * time.Second):
		h.t.Fatal("handler never parked")
	}
}

func (h *stateHarness) cleanup() {
	t := h.t
	// Release parked test handlers and held writes so teardown never
	// races a gate.
	h.releaseBlock()
	h.wire.releaseWrites()
	h.remote.releaseWrites()

	if h.conn != nil {
		h.conn.close()
		h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evConnDone })
	}
	_ = h.client.Close()

	// No events may remain in flight after teardown: every hook goroutine
	// must be quiescent before the next test starts.
	h.assertQuiescent(t)
}

func (h *stateHarness) assertQuiescent(t *testing.T) {
	// The client receive loop must have terminated.
	h.sink.waitFor(t, 0, func(e hEvent) bool { return e.kind == evLoopDone })

	// The stream sets visible to tests must be empty.
	h.client.streamLock.RLock()
	n := len(h.client.streams)
	h.client.streamLock.RUnlock()
	if n != 0 {
		t.Fatalf("client stream set not empty after teardown: %d entries", n)
	}
	if h.conn != nil && !h.expectServerStreams {
		count := 0
		h.conn.streams.Range(func(_, _ any) bool { count++; return true })
		if count != 0 {
			t.Fatalf("server stream set not empty after teardown: %d entries", count)
		}
	}
}

func frameKindString(h messageHeader) string {
	return h.Type.String()
}

// waitKind waits for the next event of a kind from cursor.
func (h *stateHarness) waitKind(cursor int, kind evType) (hEvent, int) {
	return h.sink.waitFor(h.t, cursor, func(e hEvent) bool { return e.kind == kind })
}

func (h *stateHarness) waitKindID(cursor int, kind evType, id uint32) (hEvent, int) {
	return h.sink.waitFor(h.t, cursor, func(e hEvent) bool { return e.kind == kind && e.id == id })
}

// streamIDs returns the current server-side stream id set.
func (h *stateHarness) serverStreamIDs() []uint32 {
	var ids []uint32
	h.conn.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	return ids
}

// clientHasStream reports the current client-side stream set membership.
func (h *stateHarness) clientHasStream(id streamID) bool {
	return h.client.getStream(id) != nil
}

// --- raw frame helpers --------------------------------------------------

func marshalRequest(service, method string, payload proto.Message) []byte {
	var p []byte
	if payload != nil {
		p, _ = proto.Marshal(payload)
	}
	req := &Request{Service: service, Method: method, Payload: p}
	b, _ := proto.Marshal(req)
	return b
}

func injectRequest(c *scriptedConn, sid uint32, flags uint8, service, method string, payload proto.Message) {
	b := marshalRequest(service, method, payload)
	c.injectFrame(encodeFrame(sid, messageTypeRequest, flags, b))
}

func injectData(c *scriptedConn, sid uint32, flags uint8, payload proto.Message) {
	var b []byte
	if payload != nil {
		b, _ = proto.Marshal(payload)
	}
	c.injectFrame(encodeFrame(sid, messageTypeData, flags, b))
}

func decodeResponse(f wireFrame) (*Response, error) {
	resp := &Response{}
	if err := proto.Unmarshal(f.payload, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// framesFrom returns recorded frames with the given stream id and type.
func framesFrom(c *scriptedConn, sid uint32, mt messageType) []wireFrame {
	var out []wireFrame
	for _, f := range c.recordedFrames() {
		if f.header.StreamID == sid && f.header.Type == mt {
			out = append(out, f)
		}
	}
	return out
}

// waitFrame waits for a recorded frame satisfying pred from a cursor.
func (h *stateHarness) waitFrame(cursor int, pred func(hEvent) bool) (hEvent, int) {
	return h.sink.waitFor(h.t, cursor, func(e hEvent) bool {
		return e.kind == evFrame && pred(e)
	})
}

// nextResponseFrom waits for a response frame recorded from side c and
// returns the decoded Response and the frame event.
func (h *stateHarness) nextResponseFrom(c *scriptedConn, sid uint32, cursor int) (*Response, hEvent, int) {
	name := c.name
	e, next := h.waitFrame(cursor, func(ev hEvent) bool {
		return ev.from == name && ev.id == sid && ev.msg == messageTypeResponse.String()
	})
	fs := framesFrom(c, sid, messageTypeResponse)
	if len(fs) == 0 {
		h.t.Fatalf("frame event without recording for stream %d", sid)
	}
	resp, err := decodeResponse(fs[len(fs)-1])
	if err != nil {
		h.t.Fatal(err)
	}
	return resp, e, next
}
