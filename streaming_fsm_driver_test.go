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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file provides a deterministic, replayable test driver for the
// streaming state machines. It uses an in-memory net.Conn implementation so
// no real port or unix socket is involved. Every wire write can be paused,
// released one frame at a time, failed with a deterministic transport
// error, or closed from either side. Synchronization is strictly
// event-driven: tests wait on goroutine completion channels or observed
// state transitions (guarded by a fail-safe watchdog), never on
// time.Sleep.

// errScriptWrite is the deterministic, non-nil transport error injected by
// the scripted connection on a write. It deliberately does not match any
// case in filterCloseErr so contracts can assert the exact error.
var errScriptWrite = errors.New("scripted transport: write failed")

var errScriptRead = errors.New("scripted transport: read failed")

// errScriptTimeout is returned by scripted read deadlines.
var errScriptTimeout = errors.New("scripted transport: i/o timeout")

type scriptAddr struct{}

func (scriptAddr) Network() string { return "script" }
func (scriptAddr) String() string  { return "script-conn" }

// scriptEnd is one end of an in-memory, full-duplex connection pair.
// Writes made by one end append to the other end's read buffer. Each end
// has an independent "write gate" used to park a write at the exact moment
// it is performed and later allow exactly one frame through.
type scriptEnd struct {
	peer *scriptEnd

	bufMu  sync.Mutex
	rbuf   []byte
	notify chan struct{}
	readMu sync.Mutex // serializes reads, matching one read loop per conn

	readErrMu sync.Mutex
	readErr   error

	gateMu     sync.Mutex
	gate       chan struct{}
	arrivals   chan struct{}
	writeMu    sync.Mutex
	nextWriteN int
	failNth    int
	nextErr    error

	closeMu sync.Mutex
	closed  bool
	once    sync.Once

	deadlineMu   sync.Mutex
	readDeadline time.Time
	deadlineKick chan struct{}
}

func newScriptConn() (local, peer *scriptEnd) {
	local = newScriptEnd()
	peer = newScriptEnd()
	local.peer = peer
	peer.peer = local
	return local, peer
}

func newScriptEnd() *scriptEnd {
	s := &scriptEnd{
		arrivals:     make(chan struct{}, 128),
		deadlineKick: make(chan struct{}, 16),
	}
	s.openGate()
	ch := make(chan struct{})
	close(ch)
	s.notify = ch
	return s
}

func (s *scriptEnd) openGate() {
	ch := make(chan struct{})
	close(ch)
	s.gateMu.Lock()
	s.gate = ch
	s.gateMu.Unlock()
}

// pauseWrites makes subsequent writes park in Write until releaseOneWrite
// or releaseAllWrites is called.
func (s *scriptEnd) pauseWrites() {
	s.gateMu.Lock()
	s.gate = make(chan struct{})
	s.gateMu.Unlock()
}

// releaseOneWrite allows exactly one parked (or next-arriving) write to
// proceed; the gate is re-closed for following writes.
func (s *scriptEnd) releaseOneWrite() {
	s.gateMu.Lock()
	old := s.gate
	s.gate = make(chan struct{})
	s.gateMu.Unlock()
	close(old)
}

// releaseAllWrites opens the gate permanently.
func (s *scriptEnd) releaseAllWrites() {
	s.gateMu.Lock()
	select {
	case <-s.gate:
	default:
		close(s.gate)
	}
	s.gateMu.Unlock()
}

// waitForParkedWrite blocks until a writer reaches the gate.
func (s *scriptEnd) waitForParkedWrite(t testing.TB) {
	t.Helper()
	select {
	case <-s.arrivals:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: timed out waiting for a write to park", t.Name())
	}
}

// failNthWrite makes the n-th subsequent write (1-based, per end) fail.
func (s *scriptEnd) failNthWrite(n int, err error) {
	s.writeMu.Lock()
	s.failNth = n
	s.nextWriteN = 0
	s.nextErr = err
	s.writeMu.Unlock()
}

func (s *scriptEnd) Write(p []byte) (int, error) {
	if s.isClosed() {
		return 0, io.ErrClosedPipe
	}

	s.gateMu.Lock()
	gate := s.gate
	s.gateMu.Unlock()
	select {
	case <-gate:
	default:
		select {
		case s.arrivals <- struct{}{}:
		default:
		}
		<-gate
	}

	if s.isClosed() {
		return 0, io.ErrClosedPipe
	}

	s.writeMu.Lock()
	s.nextWriteN++
	if s.failNth > 0 && s.nextWriteN == s.failNth {
		err := s.nextErr
		s.failNth = 0
		s.nextErr = nil
		s.writeMu.Unlock()
		return 0, err
	}
	s.writeMu.Unlock()

	s.peer.deliver(append([]byte(nil), p...))
	return len(p), nil
}

func (s *scriptEnd) Read(p []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	for {
		s.bufMu.Lock()
		if len(s.rbuf) > 0 {
			n := copy(p, s.rbuf)
			s.rbuf = s.rbuf[n:]
			s.bufMu.Unlock()
			return n, nil
		}
		notify := s.notify
		s.readErrMu.Lock()
		rerr := s.readErr
		s.readErrMu.Unlock()
		peerClosed := s.peer.isClosed()
		selfClosed := s.isClosed()
		s.bufMu.Unlock()

		if rerr != nil {
			s.readErrMu.Lock()
			s.readErr = nil
			s.readErrMu.Unlock()
			return 0, rerr
		}
		if peerClosed {
			return 0, io.EOF
		}
		if selfClosed {
			return 0, io.ErrClosedPipe
		}

		var timerC <-chan time.Time
		var timer *time.Timer
		dl := s.getReadDeadline()
		if !dl.IsZero() {
			if !time.Now().Before(dl) {
				return 0, errScriptTimeout
			}
			timer = time.NewTimer(time.Until(dl))
			defer timer.Stop()
			timerC = timer.C
		}

		select {
		case <-notify:
		case <-s.deadlineKick:
		case <-timerC:
			return 0, errScriptTimeout
		}
	}
}

func (s *scriptEnd) deliver(p []byte) {
	s.bufMu.Lock()
	s.rbuf = append(s.rbuf, p...)
	old := s.notify
	s.notify = make(chan struct{})
	s.bufMu.Unlock()
	close(old)
}

// Close closes this end. The peer drains buffered data, then sees EOF.
func (s *scriptEnd) Close() error {
	s.once.Do(func() {
		s.closeMu.Lock()
		s.closed = true
		s.closeMu.Unlock()
		s.bufMu.Lock()
		old := s.notify
		s.notify = make(chan struct{})
		s.bufMu.Unlock()
		close(old)
	})
	return nil
}

func (s *scriptEnd) isClosed() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed
}

func (s *scriptEnd) waitClosed(t testing.TB) {
	t.Helper()
	waitUntil(t, s.isClosed, "script end to close")
}

// injectReadError schedules a one-shot error for reads on this end.
func (s *scriptEnd) injectReadError(err error) {
	s.readErrMu.Lock()
	s.readErr = err
	s.readErrMu.Unlock()
	s.bufMu.Lock()
	old := s.notify
	s.notify = make(chan struct{})
	s.bufMu.Unlock()
	close(old)
}

func (s *scriptEnd) LocalAddr() net.Addr  { return scriptAddr{} }
func (s *scriptEnd) RemoteAddr() net.Addr { return scriptAddr{} }

func (s *scriptEnd) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	return s.SetWriteDeadline(t)
}

func (s *scriptEnd) SetReadDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.readDeadline = t
	s.deadlineMu.Unlock()
	select {
	case s.deadlineKick <- struct{}{}:
	default:
	}
	return nil
}

func (s *scriptEnd) SetWriteDeadline(time.Time) error { return nil }

func (s *scriptEnd) getReadDeadline() time.Time {
	s.deadlineMu.Lock()
	defer s.deadlineMu.Unlock()
	return s.readDeadline
}

// waitUntil polls cond until it becomes true. It contains no Sleep; the
// watchdog only fires to turn a hang into a deterministic test failure.
func waitUntil(t testing.TB, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: timed out waiting for %s", t.Name(), what)
		}
		runtime.Gosched()
	}
}

// waitChan waits for ch with a deterministic fail-safe timeout.
func waitChan(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out waiting for %s", t.Name(), what)
	}
}

// goscheds yields the processor a few times. It never establishes
// correctness by itself: every goroutine that must be parked is either
// observed through a channel (e.g. waitForParkedWrite) or the parked state
// is one of a set of outcomes explicitly asserted by the test.
func goscheds(n int) {
	for range n {
		runtime.Gosched()
	}
}

// ---------------------------------------------------------------------------
// Wire frame helpers
// ---------------------------------------------------------------------------

// encodeFrame builds one complete ttrpc frame (10 byte header + payload) so
// that each frame reaches the underlying connection as a single Write.
func encodeFrame(t testing.TB, sid uint32, mt messageType, flags uint8, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], sid)
	buf[8] = byte(mt)
	buf[9] = flags
	copy(buf[messageHeaderLength:], payload)
	return buf
}

func writeRawFrame(t testing.TB, end *scriptEnd, sid uint32, mt messageType, flags uint8, payload []byte) {
	t.Helper()
	frame := encodeFrame(t, sid, mt, flags, payload)
	if _, err := end.Write(frame); err != nil {
		t.Fatalf("%s: write raw frame (sid=%d type=%s flags=%#x): %v", t.Name(), sid, mt, flags, err)
	}
}

func marshalProto(t testing.TB, m proto.Message) []byte {
	t.Helper()
	p, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("%s: marshal: %v", t.Name(), err)
	}
	return p
}

// writeRequest writes a request frame for the given service/method.
func writeRequest(t testing.TB, end *scriptEnd, sid uint32, flags uint8, req *Request) {
	t.Helper()
	writeRawFrame(t, end, sid, messageTypeRequest, flags, marshalProto(t, req))
}

// writeData writes a data frame carrying msg (nil msg means no payload).
func writeData(t testing.TB, end *scriptEnd, sid uint32, flags uint8, msg proto.Message) {
	t.Helper()
	var p []byte
	if msg != nil {
		p = marshalProto(t, msg)
	}
	writeRawFrame(t, end, sid, messageTypeData, flags, p)
}

// writeStatus writes a final response frame carrying the given status.
func writeStatus(t testing.TB, end *scriptEnd, sid uint32, st *status.Status, payload proto.Message) {
	t.Helper()
	resp := &Response{Status: st.Proto()}
	if payload != nil {
		resp.Payload = marshalProto(t, payload)
	}
	writeRawFrame(t, end, sid, messageTypeResponse, 0, marshalProto(t, resp))
}

// readFrame reads one complete frame from end using the production header
// parser. It fails the test on any error (including EOF).
func readFrame(t testing.TB, end *scriptEnd) (messageHeader, []byte) {
	t.Helper()
	ch := newChannel(end)
	mh, p, err := ch.recv()
	if err != nil {
		t.Fatalf("%s: read frame: %v", t.Name(), err)
	}
	return mh, p
}

// assertNoWrite verifies that no writer reaches end's write gate. Tests
// pause writes before the operation under test and call this at a fully
// synchronized point, so absence is observed via the arrival signal
// instead of a timing-based read.
func assertNoWrite(t testing.TB, end *scriptEnd) {
	t.Helper()
	select {
	case <-end.arrivals:
		t.Fatalf("%s: unexpected frame write observed", t.Name())
	default:
	}
}

// echoReq returns a serialized Request for the echo test service.
func echoRequest(service, method string, seq int64) *Request {
	p, _ := proto.Marshal(&internal.EchoPayload{Seq: seq, Msg: "ping"})
	return &Request{Service: service, Method: method, Payload: p}
}

// ---------------------------------------------------------------------------
// Observed event log (stream set + delivery + completion signals)
// ---------------------------------------------------------------------------

type fsmEventKind int

const (
	evRegistered fsmEventKind = iota
	evDeleted
	evDelivered
	evConnDone
)

type fsmEvent struct {
	kind fsmEventKind
	id   uint32
	err  error
}

type fsmEventLog struct {
	mu     sync.Mutex
	events []fsmEvent
	notify chan struct{}
}

func newFSMEventLog() *fsmEventLog {
	return &fsmEventLog{notify: make(chan struct{})}
}

func (l *fsmEventLog) add(e fsmEvent) {
	l.mu.Lock()
	l.events = append(l.events, e)
	ch := l.notify
	l.notify = make(chan struct{})
	l.mu.Unlock()
	close(ch)
}

func (l *fsmEventLog) snapshot() []fsmEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]fsmEvent, len(l.events))
	copy(out, l.events)
	return out
}

func (l *fsmEventLog) waitFor(t testing.TB, pred func([]fsmEvent) bool, what string) {
	t.Helper()
	l.mu.Lock()
	ch := l.notify
	if pred(l.events) {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		l.mu.Lock()
		ch = l.notify
		ok := pred(l.events)
		l.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-ch:
		case <-deadline.C:
			t.Fatalf("%s: timed out waiting for %s", t.Name(), what)
		}
	}
}

func (l *fsmEventLog) count(kind fsmEventKind) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.kind == kind {
			n++
		}
	}
	return n
}

// ids returns the currently live stream ids derived from
// register/delete ordering.
func (l *fsmEventLog) liveIDs() map[uint32]bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	live := map[uint32]bool{}
	for _, e := range l.events {
		switch e.kind {
		case evRegistered:
			live[e.id] = true
		case evDeleted:
			delete(live, e.id)
		}
	}
	return live
}

// ---------------------------------------------------------------------------
// Client-side harness: real Client over one end of a scripted connection
// ---------------------------------------------------------------------------

type clientHarness struct {
	t      testing.TB
	client *Client
	// local is the end owned by the Client; peer is driven by the test.
	local, peer *scriptEnd
	peerCh      *channel
	log         *fsmEventLog
}

func newClientHarness(t testing.TB) *clientHarness {
	t.Helper()
	local, peer := newScriptConn()
	log := newFSMEventLog()
	hooks := &clientTestHooks{
		messageDelivered: func(id uint32, err error) {
			log.add(fsmEvent{kind: evDelivered, id: id, err: err})
		},
	}
	c := NewClient(local, withClientTestHooks(hooks))
	h := &clientHarness{t: t, client: c, local: local, peer: peer, peerCh: newChannel(peer), log: log}
	t.Cleanup(func() { c.Close() })
	return h
}

func (h *clientHarness) readFrame() (messageHeader, []byte) {
	h.t.Helper()
	mh, p, err := h.peerCh.recv()
	if err != nil {
		h.t.Fatalf("%s: read frame: %v", h.t.Name(), err)
	}
	return mh, p
}

func (h *clientHarness) readResponse() *Response {
	h.t.Helper()
	mh, p := h.readFrame()
	if mh.Type != messageTypeResponse {
		h.t.Fatalf("%s: expected response frame, got %s", h.t.Name(), mh.Type)
	}
	var resp Response
	if err := proto.Unmarshal(p[:mh.Length], &resp); err != nil {
		h.t.Fatalf("%s: unmarshal response: %v", h.t.Name(), err)
	}
	return &resp
}

func (h *clientHarness) waitDelivered(id uint32) {
	h.t.Helper()
	h.log.waitFor(h.t, func(evs []fsmEvent) bool {
		for _, e := range evs {
			if e.kind == evDelivered && e.id == id {
				return true
			}
		}
		return false
	}, "client frame delivery")
}

func (h *clientHarness) deliveredCount(id uint32) int {
	h.t.Helper()
	n := 0
	for _, e := range h.log.snapshot() {
		if e.kind == evDelivered && e.id == id {
			n++
		}
	}
	return n
}

// streamIDs snapshots the client's live stream id set.
func (h *clientHarness) streamIDs() map[streamID]bool {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	out := make(map[streamID]bool, len(h.client.streams))
	for id := range h.client.streams {
		out[id] = true
	}
	return out
}

func (h *clientHarness) hasStream(s *stream) bool {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	return h.client.streams[s.id] == s
}

func (h *clientHarness) waitUserClose() {
	h.t.Helper()
	waitChan(h.t, h.client.userCloseWaitCh, "client run loop to finish")
}

func (h *clientHarness) waitLocalClosed() {
	h.t.Helper()
	h.local.waitClosed(h.t)
}

// newStream opens a ClientStream; desc determines the four RPC shapes.
func (h *clientHarness) newStream(ctx context.Context, sc, ss bool, service, method string, req proto.Message) ClientStream {
	h.t.Helper()
	desc := &StreamDesc{StreamingClient: sc, StreamingServer: ss}
	cs, err := h.client.NewStream(ctx, desc, service, method, req)
	if err != nil {
		h.t.Fatalf("%s: NewStream: %v", h.t.Name(), err)
	}
	return cs
}

// ---------------------------------------------------------------------------
// Server-side harness: real Server over one end of a scripted connection
// ---------------------------------------------------------------------------

type scriptListener struct {
	addr   net.Addr
	conns  chan net.Conn
	mu     sync.Mutex
	closed bool
}

func newScriptListener() *scriptListener {
	return &scriptListener{addr: scriptAddr{}, conns: make(chan net.Conn, 1)}
}

func (l *scriptListener) Accept() (net.Conn, error) {
	c, ok := <-l.conns
	if !ok {
		return nil, io.ErrClosedPipe
	}
	return c, nil
}

func (l *scriptListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.conns)
	}
	return nil
}

func (l *scriptListener) Addr() net.Addr { return l.addr }

type serverHarness struct {
	t        testing.TB
	server   *Server
	listener *scriptListener
	log      *fsmEventLog
	// peer is the end the test drives as the wire client; local is owned
	// by the accepted server connection.
	peer, local *scriptEnd
	peerCh      *channel
}

func newServerHarness(t testing.TB, opts ...ServerOpt) *serverHarness {
	t.Helper()
	log := newFSMEventLog()
	hooks := &serverTestHooks{
		streamRegistered: func(id uint32) { log.add(fsmEvent{kind: evRegistered, id: id}) },
		streamDeleted:    func(id uint32) { log.add(fsmEvent{kind: evDeleted, id: id}) },
		connDone:         func() { log.add(fsmEvent{kind: evConnDone}) },
	}
	all := append([]ServerOpt{withServerTestHooks(hooks)}, opts...)
	srv := mustServer(t)(NewServer(all...))
	l := newScriptListener()
	h := &serverHarness{t: t, server: srv, listener: l, log: log}
	t.Cleanup(func() { srv.Close() })
	return h
}

func (h *serverHarness) serve() {
	go func() { _ = h.server.Serve(context.Background(), h.listener) }()
}

// dial pushes one end of a fresh scripted pair through the listener and
// returns the other end for the test to drive as the wire client.
func (h *serverHarness) dial() *scriptEnd {
	h.t.Helper()
	local, peer := newScriptConn()
	h.local = local
	h.peer = peer
	h.peerCh = newChannel(peer)
	h.listener.conns <- local
	h.log.waitFor(h.t, func(evs []fsmEvent) bool {
		return h.server.countConnection() == 1
	}, "server to register connection")
	return peer
}

func (h *serverHarness) readFrame() (messageHeader, []byte) {
	h.t.Helper()
	mh, p, err := h.peerCh.recv()
	if err != nil {
		h.t.Fatalf("%s: read frame: %v", h.t.Name(), err)
	}
	return mh, p
}

func (h *serverHarness) waitConnDone() {
	h.t.Helper()
	h.log.waitFor(h.t, func(evs []fsmEvent) bool {
		for _, e := range evs {
			if e.kind == evConnDone {
				return true
			}
		}
		return false
	}, "server connection run loop to finish")
}

func (h *serverHarness) waitRegistered(id uint32) {
	h.t.Helper()
	h.log.waitFor(h.t, func(evs []fsmEvent) bool {
		for _, e := range evs {
			if e.kind == evRegistered && e.id == id {
				return true
			}
		}
		return false
	}, "server stream registration")
}

func (h *serverHarness) waitDeleted(id uint32) {
	h.t.Helper()
	h.log.waitFor(h.t, func(evs []fsmEvent) bool {
		for _, e := range evs {
			if e.kind == evDeleted && e.id == id {
				return true
			}
		}
		return false
	}, "server stream deletion")
}

// streamEventIDs returns the ordered list of (kind,id) events, which the
// ordering assertions use to check registration/deletion timing.
func (h *serverHarness) streamEvents() []fsmEvent {
	return h.log.snapshot()
}

// registerEcho installs a service named service with the given streams.
func (h *serverHarness) register(service string, streams map[string]Stream, methods map[string]Method) {
	h.t.Helper()
	h.server.RegisterService(service, &ServiceDesc{Streams: streams, Methods: methods})
}
