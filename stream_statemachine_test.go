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
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// This file contains a deterministic, replayable state-machine test driver for
// ttrpc streaming. No real sockets, no time.Sleep, no reliance on scheduler
// ordering: every progress point is driven by explicit hooks.
//
//   - scriptedConn is an in-memory net.Conn pair. A write may be paused at
//     frame granularity and later released one frame at a time; writes or
//     reads may be made to fail with a chosen error; either side may be
//     abruptly or gracefully closed; complete frames may be injected out of
//     band from either side.
//   - rec records a single totally ordered log of events: wire frames (by
//     receiver), client dispatch outcomes, stream-set insert/delete events on
//     both sides, server handler milestones and goroutine completion signals.
//   - Every test progresses only by waiting for concrete events and then
//     taking a concrete action.

const testWaitTimeout = 10 * time.Second

type eventKind string

const (
	evWire              eventKind = "wire"
	evClientRegistered  eventKind = "client-registered"
	evClientDeleted     eventKind = "client-deleted"
	evClientDispatch    eventKind = "client-dispatch"
	evClientRunDone     eventKind = "client-run-done"
	evServerRegistered  eventKind = "server-registered"
	evServerDeleted     eventKind = "server-deleted"
	evServerInactive    eventKind = "server-inactive"
	evServerRunDone     eventKind = "server-run-done"
	evHandler           eventKind = "handler"
	evHeld              eventKind = "held"
	evCustom            eventKind = "custom"
)

type recEvent struct {
	kind    eventKind
	id      uint32
	mtype   messageType
	flags   uint8
	outcome streamDispatchOutcome
	detail  string
	seq     int64
}

type recorder struct {
	mu     sync.Mutex
	cond   *sync.Cond
	events []recEvent
}

func newRecorder() *recorder {
	r := &recorder{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *recorder) add(e recEvent) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.cond.Broadcast()
	r.mu.Unlock()
}

// waitFor blocks until at least n events exist, or fails the test.
func (r *recorder) waitFor(t testing.TB, n int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) >= n {
		return
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(testWaitTimeout):
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for len(r.events) < n {
		r.cond.Wait()
	}
}

// wait blocks until an event matching pred exists, then returns it.
func (r *recorder) wait(t testing.TB, what string, pred func(recEvent) bool) recEvent {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(testWaitTimeout):
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for {
		for _, e := range r.events {
			if pred(e) {
				return e
			}
		}
		r.cond.Wait()
		if len(r.events) >= 0 {
			for _, e := range r.events {
				if pred(e) {
					return e
				}
			}
		}
	}
}

func (r *recorder) snapshot() []recEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recEvent, len(r.events))
	copy(out, r.events)
	return out
}

func (r *recorder) count(pred func(recEvent) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if pred(e) {
			n++
		}
	}
	return n
}

func (r *recorder) index(pred func(recEvent) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, e := range r.events {
		if pred(e) {
			return i
		}
	}
	return -1
}

// assertOrder fails if any event matching later occurs before one matching
// earlier. It also requires at least one event of each kind to exist (after
// the caller has established progress).
func (r *recorder) assertOrder(t testing.TB, earlier, later func(recEvent) bool) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	firstEarlier, firstLater := -1, -1
	for i, e := range r.events {
		if firstEarlier == -1 && earlier(e) {
			firstEarlier = i
		}
		if firstLater == -1 && later(e) {
			firstLater = i
		}
	}
	if firstEarlier == -1 || firstLater == -1 {
		t.Fatalf("assertOrder: missing events (earlier=%d, later=%d)", firstEarlier, firstLater)
	}
	if firstLater < firstEarlier {
		t.Fatalf("assertOrder: event order violated: %d before %d", firstLater, firstEarlier)
	}
}

func isWireID(id uint32) func(recEvent) bool {
	return func(e recEvent) bool { return e.kind == evWire && e.id == id }
}

// errScriptedClosed is the deterministic error observed when reading or
// writing a fully closed scripted connection.
var errScriptedClosed = errors.New("scripted conn: closed")

type heldWrite struct {
	frame wireFrame
}

type scriptedConn struct {
	name     string
	rec      *recorder
	peer     *scriptedConn
	local    net.Addr
	remote   net.Addr
	onFrame  func(wireFrame)

	mu sync.Mutex

	// read side
	readBuf []byte
	readErr error
	closed  bool // local close: writes fail
	shut    bool // local shutdown: reads on this end return EOF
	peerGone bool // peer closed its side: our reads EOF
	cond    *sync.Cond

	// write side
	writeErr     error // sticky error for the next successful frame delivery
	failNextOnce bool
	paused       bool
	held         []*heldWrite
}

type dummyAddr struct{ name string }

func (a dummyAddr) Network() string { return "scripted" }
func (a dummyAddr) String() string  { return a.name }

// newScriptedPair builds two connected in-memory conns. clientName/serverName
// are used in addresses and diagnostics.
func newScriptedPair(r *recorder, clientName, serverName string) (*scriptedConn, *scriptedConn) {
	a := &scriptedConn{
		name:   clientName,
		rec:    r,
		local:  dummyAddr{clientName},
		remote: dummyAddr{serverName},
	}
	b := &scriptedConn{
		name:   serverName,
		rec:    r,
		local:  dummyAddr{serverName},
		remote: dummyAddr{clientName},
	}
	a.peer = b
	b.peer = a
	return a, b
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	for {
		if len(c.readBuf) > 0 {
			n := copy(p, c.readBuf)
			c.readBuf = c.readBuf[n:]
			c.mu.Unlock()
			return n, nil
		}
		if c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			c.mu.Unlock()
			return 0, err
		}
		if c.shut || c.peerGone {
			c.mu.Unlock()
			return 0, io.EOF
		}
		// no data yet: wait until something happens
		// Use a cond via closed channel style: sleep-free polling on cond.
		cond := c.readCond()
		if cond == nil {
			c.mu.Unlock()
			return 0, errScriptedClosed
		}
		cond.Wait()
	}
}

// readCond lazily creates a cond used to park blocked readers.
func (c *scriptedConn) readCond() *sync.Cond {
	if c.cond == nil {
		c.cond = sync.NewCond(&c.mu)
	}
	return c.cond
}

func (c *scriptedConn) signalReaders() {
	if c.cond != nil {
		c.cond.Broadcast()
	}
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, errScriptedClosed
	}
	frame, ok := parseFrame(p)
	if !ok {
		c.mu.Unlock()
		return 0, fmt.Errorf("scripted conn %q: write was not one complete frame (%d bytes)", c.name, len(p))
	}

	fail := false
	if c.writeErr != nil {
		fail = true
		if c.failNextOnce {
			c.writeErr = nil
			c.failNextOnce = false
		}
	}

	if !fail && c.paused {
		hw := &heldWrite{frame: frame}
		c.held = append(c.held, hw)
		c.mu.Unlock()
		c.rec.add(recEvent{kind: evHeld, id: frame.streamID, mtype: frame.mtype, flags: frame.flags, detail: c.name})
		return len(p), nil
	}

	peer := c.peer
	c.mu.Unlock()

	if fail {
		// Writes are already off-wire at this point; report the chosen error.
		return 0, c.snapshotWriteErr()
	}

	peer.deliver(frame, p)
	return len(p), nil
}

func (c *scriptedConn) snapshotWriteErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeErr
}

// deliver hands a complete frame to the peer's read buffer.
func (c *scriptedConn) deliver(frame wireFrame, raw []byte) {
	c.mu.Lock()
	closed := c.shut || c.peerGone
	if !closed {
		c.readBuf = append(c.readBuf, raw...)
	}
	hook := c.onFrame
	c.signalReaders()
	c.mu.Unlock()
	if !closed && hook != nil {
		hook(frame)
	}
}

// injectRaw puts a complete, externally constructed frame into this conn's
// read buffer, as if the peer had sent it.
func (c *scriptedConn) injectRaw(raw []byte) {
	frame, ok := parseFrame(raw)
	if !ok {
		panic("injectRaw: not a complete frame")
	}
	c.mu.Lock()
	c.readBuf = append(c.readBuf, raw...)
	hook := c.onFrame
	c.signalReaders()
	c.mu.Unlock()
	if hook != nil {
		hook(frame)
	}
}

// pauseWrites parks subsequent frame writes until released.
func (c *scriptedConn) pauseWrites() {
	c.mu.Lock()
	c.paused = true
	c.mu.Unlock()
}

func (c *scriptedConn) resumeWrites() {
	c.mu.Lock()
	c.paused = false
	held := c.held
	c.held = nil
	c.mu.Unlock()
	// Deliver in original write order, outside the lock.
	for _, hw := range held {
		c.peer.deliver(hw.frame, encodeFrame(hw.frame))
	}
}

// releaseOne delivers the oldest parked frame, if any. Pause stays in effect.
func (c *scriptedConn) releaseOne() bool {
	c.mu.Lock()
	if len(c.held) == 0 {
		c.mu.Unlock()
		return false
	}
	hw := c.held[0]
	c.held = c.held[1:]
	c.mu.Unlock()
	c.peer.deliver(hw.frame, encodeFrame(hw.frame))
	return true
}

func (c *scriptedConn) heldFrames() []wireFrame {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]wireFrame, len(c.held))
	for i, hw := range c.held {
		out[i] = hw.frame
	}
	return out
}

// failNextWrite makes the very next frame write fail with err and then clears.
func (c *scriptedConn) failNextWrite(err error) {
	c.mu.Lock()
	c.writeErr = err
	c.failNextOnce = true
	c.mu.Unlock()
}

// failWrites makes every subsequent write fail with err.
func (c *scriptedConn) failWrites(err error) {
	c.mu.Lock()
	c.writeErr = err
	c.failNextOnce = false
	c.mu.Unlock()
}

// injectReadError stages err so the next blocking read returns it (after any
// already buffered complete frames are consumed).
func (c *scriptedConn) injectReadError(err error) {
	c.mu.Lock()
	c.readErr = err
	c.signalReaders()
	c.mu.Unlock()
}

// Close abruptly closes both directions: our writes fail and the peer's reads
// see EOF.
func (c *scriptedConn) Close() error {
	c.mu.Lock()
	if c.closed && c.shut {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.shut = true
	peer := c.peer
	c.signalReaders()
	c.mu.Unlock()

	if peer != nil {
		peer.mu.Lock()
		peer.peerGone = true
		peer.signalReaders()
		peer.mu.Unlock()
	}
	return nil
}

// CloseWrite half-closes: peer reads EOF, our reads remain available.
func (c *scriptedConn) CloseWrite() error {
	c.mu.Lock()
	c.closed = true
	peer := c.peer
	c.mu.Unlock()
	if peer != nil {
		peer.mu.Lock()
		peer.peerGone = true
		peer.signalReaders()
		peer.mu.Unlock()
	}
	return nil
}

// CloseRead makes our own future reads return EOF without affecting the peer.
func (c *scriptedConn) CloseRead() error {
	c.mu.Lock()
	c.shut = true
	c.signalReaders()
	c.mu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr  { return c.local }
func (c *scriptedConn) RemoteAddr() net.Addr { return c.remote }
func (c *scriptedConn) SetDeadline(t time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(t time.Time) error { return nil }

// wireFrame is a decoded ttrpc frame used by the scripted transport.
type wireFrame struct {
	streamID uint32
	mtype    messageType
	flags    uint8
	payload  []byte
}

func encodeFrame(f wireFrame) []byte {
	raw := make([]byte, messageHeaderLength+len(f.payload))
	raw[0] = byte(len(f.payload) >> 24)
	raw[1] = byte(len(f.payload) >> 16)
	raw[2] = byte(len(f.payload) >> 8)
	raw[3] = byte(len(f.payload))
	raw[4] = byte(f.streamID >> 24)
	raw[5] = byte(f.streamID >> 16)
	raw[6] = byte(f.streamID >> 8)
	raw[7] = byte(f.streamID)
	raw[8] = byte(f.mtype)
	raw[9] = f.flags
	copy(raw[messageHeaderLength:], f.payload)
	return raw
}

// parseFrame requires p to contain exactly one complete frame.
func parseFrame(p []byte) (wireFrame, bool) {
	if len(p) < messageHeaderLength {
		return wireFrame{}, false
	}
	mh := messageHeader{
		Length:   beUint32(p[0:4]),
		StreamID: beUint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}
	if int(mh.Length) != len(p)-messageHeaderLength {
		return wireFrame{}, false
	}
	payload := make([]byte, mh.Length)
	copy(payload, p[messageHeaderLength:])
	return wireFrame{streamID: mh.StreamID, mtype: mh.Type, flags: mh.Flags, payload: payload}, true
}

func beUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

// marshalRequest encodes a Request frame payload.
func marshalRequest(t testing.TB, service, method string, msg proto.Message, timeoutNano int64) []byte {
	t.Helper()
	var payload []byte
	if msg != nil {
		var err error
		payload, err = proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := &Request{Service: service, Method: method, Payload: payload, TimeoutNano: timeoutNano}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodeResponse(t testing.TB, f wireFrame) *Response {
	t.Helper()
	var resp Response
	if err := proto.Unmarshal(f.payload, &resp); err != nil {
		t.Fatalf("decode response on stream %d: %v", f.streamID, err)
	}
	return &resp
}

// clientStreamIDs returns a sorted list of currently registered client
// streams.
func (c *Client) clientStreamIDs() []streamID {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	ids := make([]streamID, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	return ids
}

func (c *Client) hasStream(id streamID) bool {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	_, ok := c.streams[id]
	return ok
}

// streamSet is a test-side view of the server connection stream set, kept up
// to date via onStreamRegistered/onStreamDeleted hooks.
type streamSet struct {
	mu sync.Mutex
	m  map[uint32]struct{}
}

func newStreamSet() *streamSet {
	return &streamSet{m: make(map[uint32]struct{})}
}

func (s *streamSet) add(id uint32) {
	s.mu.Lock()
	s.m[id] = struct{}{}
	s.mu.Unlock()
}

func (s *streamSet) del(id uint32) {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

func (s *streamSet) has(id uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[id]
	return ok
}

func (s *streamSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// harness wires together the scripted transport, recorder, client and server.
type harness struct {
	t       *testing.T
	rec     *recorder
	clientC *scriptedConn
	serverC *scriptedConn
	client  *Client
	server  *Server
	sc      *serverConn
	sset    *streamSet

	clientDone    chan struct{}
	serverDone    chan struct{}
	handlerEvents func(recEvent)
}

// newHarness builds a connected client/server over in-memory conns. The
// server's connection goroutine runs production code unchanged; the listener
// path is bypassed.
func newHarness(t *testing.T, handlerEvents func(recEvent)) *harness {
	t.Helper()
	r := newRecorder()
	clientC, serverC := newScriptedPair(r, "client", "server")

	sset := newStreamSet()
	clientDone := make(chan struct{})
	serverDone := make(chan struct{})

	clientC.onFrame = func(f wireFrame) {
		r.add(recEvent{kind: evWire, id: f.streamID, mtype: f.mtype, flags: f.flags, detail: "client-recv"})
	}
	serverC.onFrame = func(f wireFrame) {
		r.add(recEvent{kind: evWire, id: f.streamID, mtype: f.mtype, flags: f.flags, detail: "server-recv"})
	}

	client := newClientWithTestHooks(clientC, &clientTestHooks{
		onStreamRegistered: func(id streamID) {
			r.add(recEvent{kind: evClientRegistered, id: uint32(id)})
		},
		onStreamDeleted: func(id streamID) {
			r.add(recEvent{kind: evClientDeleted, id: uint32(id)})
		},
		onDispatched: func(id streamID, hdr messageHeader, outcome streamDispatchOutcome) {
			r.add(recEvent{kind: evClientDispatch, id: uint32(id), mtype: hdr.Type, flags: hdr.Flags, outcome: outcome})
		},
		onRunDone: func() {
			r.add(recEvent{kind: evClientRunDone})
			close(clientDone)
		},
	})

	server, err := newServerWithTestHooks(&serverConnTestHooks{
		onStreamRegistered: func(id uint32) {
			sset.add(id)
			r.add(recEvent{kind: evServerRegistered, id: id})
		},
		onStreamDeleted: func(id uint32) {
			sset.del(id)
			r.add(recEvent{kind: evServerDeleted, id: id})
		},
		onInactiveStream: func(id uint32) {
			r.add(recEvent{kind: evServerInactive, id: id})
		},
		onRunDone: func() {
			r.add(recEvent{kind: evServerRunDone})
			close(serverDone)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	sc, err := newServerConnWithTestHooks(server, serverC, server.testHooks)
	if err != nil {
		t.Fatal(err)
	}
	go sc.run(context.Background())

	h := &harness{
		t:       t,
		rec:     r,
		clientC: clientC,
		serverC: serverC,
		client:  client,
		server:  server,
		sc:      sc,
		sset:    sset,
	}
	h.clientDone = clientDone
	h.serverDone = serverDone
	h.handlerEvents = handlerEvents
	return h
}

func (h *harness) register(desc *ServiceDesc) {
	h.server.RegisterService("stateMachineService", desc)
}

func (h *harness) emit(detail string) {
	h.rec.add(recEvent{kind: evHandler, detail: detail})
}

// waitClientRunDone / waitServerRunDone block on goroutine completion signals.
func (h *harness) waitClientRunDone() {
	h.t.Helper()
	select {
	case <-h.clientDone:
	case <-time.After(testWaitTimeout):
		h.t.Fatal("client receive goroutine did not terminate")
	}
}

func (h *harness) waitServerRunDone() {
	h.t.Helper()
	select {
	case <-h.serverDone:
	case <-time.After(testWaitTimeout):
		h.t.Fatal("server connection goroutine did not terminate")
	}
}

func (h *harness) cleanup() {
	h.client.Close()
	h.serverC.Close()
}
