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

// This file holds the deterministic, replayable test driver shared by the
// streaming state machine contract tests. Nothing here binds a real port,
// sleeps on a scheduler, or races observed state: every transport action is
// gated by an explicit channel handoff between the test and the production
// goroutine performing it.

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/protobuf/proto"
)

const fsmWaitTimeout = 5 * time.Second

// errScriptedConnClosed is returned by a scriptedConn after the test
// explicitly closes it. It mimics a dead peer so the production code applies
// filterCloseErr exactly as it would on a real torn-down socket.
var errScriptedConnClosed = errors.New("fsm: scripted connection closed")

// scriptedOp is one scripted Read result queued in advance.
type scriptedOp struct {
	p   []byte
	err error
}

type scriptedAddr struct{}

func (scriptedAddr) Network() string { return "fsm-pipe" }
func (scriptedAddr) String() string  { return "fsm-pipe" }

// scriptedWrite is one paused Write observed by the test. The test may allow
// the frame through with allow(), reject it with fail(err), or close the
// connection instead, which releases the write with a terminal error.
type scriptedWrite struct {
	frame rawFrame
	allow func()
	fail  func(error)
}

// scriptedConn is an in-memory net.Conn whose reads replay a scripted
// sequence of frames/errors and whose writes pause for explicit test
// release. autoWrite removes the pause for server-side scenarios where the
// test needs no per-frame interception but may still record the wire.
type scriptedConn struct {
	mu        sync.Mutex
	closed    bool
	autoWrite bool

	reads   chan scriptedOp
	readBuf []byte

	pendingW chan *scriptedWrite

	writtenMu sync.Mutex
	written   []rawFrame

	closedCh chan struct{}
}

func newScriptedConn(autoWrite bool) *scriptedConn {
	return &scriptedConn{
		reads:    make(chan scriptedOp, 32),
		pendingW: make(chan *scriptedWrite),
		closedCh: make(chan struct{}),
		autoWrite: autoWrite,
	}
}

func (c *scriptedConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// enqueueFrame scripts one complete frame as the next Read result.
func (c *scriptedConn) enqueueFrame(h messageHeader, p []byte) {
	c.reads <- scriptedOp{p: encodeRawFrame(h, p)}
}

// enqueueBytes scripts an arbitrary byte slice as the next Read result.
func (c *scriptedConn) enqueueBytes(p []byte) {
	c.reads <- scriptedOp{p: p}
}

// enqueueError scripts one terminal Read error.
func (c *scriptedConn) enqueueError(err error) {
	c.reads <- scriptedOp{err: err}
}

// enqueueOversizedFrame scripts a frame whose declared length exceeds the
// protocol maximum. channel.recv discards the body and returns a
// ResourceExhausted status together with the parsed header (so the error is
// routed to the implicated stream id rather than poisoning the connection).
func (c *scriptedConn) enqueueOversizedFrame(sid uint32) {
	h := messageHeader{
		Length:   uint32(messageLengthMax + 1),
		StreamID: sid,
		Type:     messageTypeData,
	}
	op := scriptedOp{p: encodeRawFrame(h, make([]byte, messageLengthMax+1))}
	c.reads <- op
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	for len(c.readBuf) == 0 {
		select {
		case op := <-c.reads:
			if op.err != nil {
				return 0, op.err
			}
			c.readBuf = op.p
		default:
			if c.isClosed() {
				// Canonical clean peer disconnect; filterCloseErr maps EOF
				// to ErrClosed exactly as on a real socket.
				return 0, io.EOF
			}
			select {
			case op := <-c.reads:
				if op.err != nil {
					return 0, op.err
				}
				c.readBuf = op.p
			case <-c.closedCh:
				return 0, io.EOF
			}
		}
	}
	n := copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	if c.autoWrite {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return 0, errScriptedConnClosed
		}
		c.recordFrame(p)
		return len(p), nil
	}

	release := make(chan error, 1)
	sw := &scriptedWrite{
		frame: parseRawFrame(p),
		allow: func() { release <- nil },
		fail:  func(err error) { release <- err },
	}
	select {
	case c.pendingW <- sw:
	case <-c.closedCh:
		return 0, errScriptedConnClosed
	}
	select {
	case err := <-release:
		if err != nil {
			return 0, err
		}
		return len(p), nil
	case <-c.closedCh:
		return 0, errScriptedConnClosed
	}
}

func (c *scriptedConn) recordFrame(p []byte) {
	c.writtenMu.Lock()
	c.written = append(c.written, parseRawFrame(p))
	c.writtenMu.Unlock()
}

// recordedFrames returns a copy of every frame written in auto mode.
func (c *scriptedConn) recordedFrames() []rawFrame {
	c.writtenMu.Lock()
	defer c.writtenMu.Unlock()
	out := make([]rawFrame, len(c.written))
	copy(out, c.written)
	return out
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closedCh)
	c.mu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// rawFrame is one fully decoded wire frame captured by the driver.
type rawFrame struct {
	h messageHeader
	p []byte
}

func encodeRawFrame(h messageHeader, p []byte) []byte {
	var buf [messageHeaderLength]byte
	binary.BigEndian.PutUint32(buf[0:4], h.Length)
	binary.BigEndian.PutUint32(buf[4:8], h.StreamID)
	buf[8] = byte(h.Type)
	buf[9] = h.Flags
	out := make([]byte, 0, messageHeaderLength+len(p))
	out = append(out, buf[:]...)
	out = append(out, p...)
	return out
}

func parseRawFrame(p []byte) rawFrame {
	if len(p) < messageHeaderLength {
		return rawFrame{h: messageHeader{Length: uint32(len(p))}}
	}
	h := messageHeader{
		Length:   binary.BigEndian.Uint32(p[0:4]),
		StreamID: binary.BigEndian.Uint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}
	body := make([]byte, len(p)-messageHeaderLength)
	copy(body, p[messageHeaderLength:])
	return rawFrame{h: h, p: body}
}

// --- frame script helpers -------------------------------------------------

// encodeDataFrame wraps a marshaled data payload as a data frame.
func encodeDataFrame(sid uint32, flags uint8, p []byte) (messageHeader, []byte) {
	return messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeData, Flags: flags}, p
}

// encodeRequestFrame marshals a Request into a request frame.
func encodeRequestFrame(sid uint32, flags uint8, req *Request) (messageHeader, []byte) {
	p := mustMarshalProto(req)
	return messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeRequest, Flags: flags}, p
}

// encodeResponseFrame marshals a Response into a response frame.
func encodeResponseFrame(sid uint32, resp *Response) (messageHeader, []byte) {
	p := mustMarshalProto(resp)
	return messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeResponse, Flags: 0}, p
}

func mustMarshalProto(m proto.Message) []byte {
	p, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return p
}

func mustUnmarshalResponse(t testing.TB, frame rawFrame) *Response {
	t.Helper()
	resp := &Response{}
	if err := proto.Unmarshal(frame.p[:frame.h.Length], resp); err != nil {
		t.Fatalf("unmarshal response frame: %v", err)
	}
	return resp
}

func mustUnmarshalRequest(t testing.TB, frame rawFrame) *Request {
	t.Helper()
	req := &Request{}
	if err := proto.Unmarshal(frame.p[:frame.h.Length], req); err != nil {
		t.Fatalf("unmarshal request frame: %v", err)
	}
	return req
}

func mustMarshalEcho(m *internal.EchoPayload) []byte {
	p, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return p
}

func mustUnmarshalEcho(t testing.TB, frame rawFrame) *internal.EchoPayload {
	t.Helper()
	out := &internal.EchoPayload{}
	if err := proto.Unmarshal(frame.p[:frame.h.Length], out); err != nil {
		t.Fatalf("unmarshal echo payload: %v", err)
	}
	return out
}

// --- generic synchronization helpers --------------------------------------

func waitCh[T any](t testing.TB, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(fsmWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func assertNoCh[T any](t testing.TB, ch <-chan T, what string) T {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected %s", what)
		var zero T
		return zero
	default:
		var zero T
		return zero
	}
}

// assertNotReady parks for a short bounded interval to prove nothing
// observable happens. It never orders correctness: every readiness assertion
// elsewhere is event driven.
func assertNotReady[T any](t testing.TB, ch <-chan T, what string) T {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s fired unexpectedly", what)
		var zero T
		return zero
	case <-time.After(20 * time.Millisecond):
		var zero T
		return zero
	}
}

// --- scripted client driver ------------------------------------------------

// scriptedClient is a ttrpc Client talking to a fully scripted peer: no
// listener, no port, no sockets.
type scriptedClient struct {
	conn   *scriptedConn
	client *Client
}

func newScriptedClient(t testing.TB) *scriptedClient {
	t.Helper()
	conn := newScriptedConn(false)
	c := NewClient(conn)
	return &scriptedClient{conn: conn, client: c}
}

func (sc *scriptedClient) waitWrite(t testing.TB) *scriptedWrite {
	t.Helper()
	return waitCh(t, sc.conn.pendingW, "paused client write")
}

// openStreamSync creates a ClientStream and releases its opening request
// frame, returning the stream plus the frame that crossed the wire.
func (sc *scriptedClient) openStreamSync(t testing.TB, desc *StreamDesc, service, method string, req any) (*clientStream, rawFrame) {
	t.Helper()
	type openResult struct {
		cs  *clientStream
		err error
	}
	done := make(chan openResult, 1)
	go func() {
		cs, err := sc.client.NewStream(context.Background(), desc, service, method, req)
		if err != nil {
			done <- openResult{nil, err}
			return
		}
		done <- openResult{cs.(*clientStream), nil}
	}()
	w := sc.waitWrite(t)
	if w.frame.h.Type != messageTypeRequest {
		t.Fatalf("expected opening request frame, got %s", w.frame.h.Type)
	}
	w.allow()
	r := waitCh(t, done, "NewStream to return")
	if r.err != nil {
		t.Fatalf("NewStream: %v", r.err)
	}
	return r.cs, w.frame
}

// callSync issues a unary dispatch against the scripted peer and releases its
// opening request frame, returning the frame and a result channel that fires
// when Call completes.
func (sc *scriptedClient) callSync(t testing.TB, service, method string, req, resp proto.Message) (rawFrame, chan error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- sc.client.Call(context.Background(), service, method, req, resp)
	}()
	w := sc.waitWrite(t)
	if w.frame.h.Type != messageTypeRequest {
		t.Fatalf("expected unary request frame, got %s", w.frame.h.Type)
	}
	w.allow()
	return w.frame, done
}

// streamSnapshot returns (id -> stream) for every live client stream.
func (sc *scriptedClient) streamSnapshot() map[streamID]*stream {
	sc.client.streamLock.RLock()
	defer sc.client.streamLock.RUnlock()
	out := make(map[streamID]*stream, len(sc.client.streams))
	for id, s := range sc.client.streams {
		out[id] = s
	}
	return out
}

func (sc *scriptedClient) streamCount() int {
	return len(sc.streamSnapshot())
}

// runExited fires after the client receive loop has terminated and all
// streams have been cleaned up.
func (sc *scriptedClient) runExited() <-chan struct{} {
	return sc.client.userCloseWaitCh
}

// close terminates the client: it closes the scripted peer, releases any
// parked write, and waits for the receive loop to exit.
func (sc *scriptedClient) close(t testing.TB) {
	t.Helper()
	sc.client.Close()
	select {
	case w := <-sc.conn.pendingW:
		w.fail(errScriptedConnClosed)
	default:
	}
	sc.conn.Close()
	waitCh(t, sc.client.userCloseWaitCh, "client run loop exit")
}

// --- scripted server driver ------------------------------------------------

type scriptedListener struct {
	connCh chan net.Conn
	closed chan struct{}
}

func newScriptedListener() *scriptedListener {
	return &scriptedListener{
		connCh: make(chan net.Conn, 1),
		closed: make(chan struct{}),
	}
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connCh:
		return c, nil
	case <-l.closed:
		return nil, errScriptedConnClosed
	}
}

func (l *scriptedListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *scriptedListener) Addr() net.Addr { return scriptedAddr{} }

// eventLog records every observed server stream-table transition in the
// exact order the production code made them visible.
type eventLog struct {
	mu     sync.Mutex
	events []streamTestEvent
	ch     chan streamTestEvent
}

func newEventLog() *eventLog {
	return &eventLog{ch: make(chan streamTestEvent, 256)}
}

func (e *eventLog) hooks() *streamTestHooks {
	return &streamTestHooks{onEvent: func(ev streamTestEvent) {
		e.mu.Lock()
		e.events = append(e.events, ev)
		e.mu.Unlock()
		e.ch <- ev
	}}
}

func (e *eventLog) snapshot() []streamTestEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]streamTestEvent, len(e.events))
	copy(out, e.events)
	return out
}

// drain returns all currently buffered events without blocking.
func (e *eventLog) drain() []streamTestEvent {
	var out []streamTestEvent
	for {
		select {
		case ev := <-e.ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// scriptedServer is a ttrpc Server driven through a scripted listener and a
// scripted auto-writing connection, so every response frame is captured.
type scriptedServer struct {
	t        testing.TB
	server   *Server
	listener *scriptedListener
	events   *eventLog
	serveCh  chan error
}

func newScriptedServer(t testing.TB, opts ...ServerOpt) *scriptedServer {
	t.Helper()
	events := newEventLog()
	server, err := newServerWithTestHooks(events.hooks(), opts...)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	l := newScriptedListener()
	ss := &scriptedServer{
		t:        t,
		server:   server,
		listener: l,
		events:   events,
		serveCh:  make(chan error, 1),
	}
	go func() {
		ss.serveCh <- server.Serve(context.Background(), l)
	}()
	return ss
}

// connect hands the server a freshly scripted auto-writing connection and
// returns the test side of it.
func (ss *scriptedServer) connect() *scriptedConn {
	c := newScriptedConn(true)
	ss.listener.connCh <- c
	return c
}

func (ss *scriptedServer) waitEvent(what string) streamTestEvent {
	ss.t.Helper()
	return waitCh(ss.t, ss.events.ch, what)
}

func (ss *scriptedServer) expectConnRunExit() {
	ss.t.Helper()
	ev := ss.waitEvent("connection run exit")
	if ev.kind != streamTestEventConnRunExit {
		ss.t.Fatalf("expected conn run exit event, got %+v", ev)
	}
}

func (ss *scriptedServer) shutdown(ctx context.Context) error {
	return ss.server.Shutdown(ctx)
}

func (ss *scriptedServer) forceClose() {
	ss.server.Close()
}

func (ss *scriptedServer) waitServeExit() error {
	ss.t.Helper()
	return waitCh(ss.t, ss.serveCh, "Serve to exit")
}

func (ss *scriptedServer) connectionCount() int {
	return ss.server.countConnection()
}

// assertEvents verifies the exact recorded event prefix.
func (ss *scriptedServer) assertEvents(want []streamTestEvent) {
	ss.t.Helper()
	got := ss.events.snapshot()
	if len(got) < len(want) {
		ss.t.Fatalf("event sequence too short:\n got %+v\nwant prefix %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			ss.t.Fatalf("event %d mismatch:\n got %+v\nwant %+v\n(full got %+v)", i, got[i], want[i], got)
		}
	}
}
