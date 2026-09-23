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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/log"
	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file provides a deterministic, replayable test driver for the ttrpc
// streaming state machine. No real ports, goroutine scheduling luck, or
// time.Sleep calls are used: all synchronization is channel/gate based.

// -----------------------------------------------------------------------------
// In-memory, scriptable net.Conn
// -----------------------------------------------------------------------------

// scriptedAddr is a dummy net.Addr for scriptedConn.
type scriptedAddr struct {
	side string
}

func (a scriptedAddr) Network() string { return "scripted" }
func (a scriptedAddr) String() string { return "scripted:" + a.side }

// pipeHalf carries bytes in one direction. "to" is the half consumed by the
// peer's Read. Closing the writer marks the peer's read half as finished (EOF).
type pipeHalf struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      bytes.Buffer
	closed   bool
	readErr  error // sticky error delivered to the reader
	writeErr error // sticky error returned to writers before delivery
	onData   func([]byte)
}

func newPipeHalf() *pipeHalf {
	h := &pipeHalf{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *pipeHalf) write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writeErr != nil {
		return 0, h.writeErr
	}
	if h.closed {
		return 0, io.ErrClosedPipe
	}
	n, err := h.buf.Write(p)
	if h.onData != nil {
		h.onData(p)
	}
	h.cond.Broadcast()
	return n, err
}

func (h *pipeHalf) read(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.buf.Len() == 0 && !h.closed && h.readErr == nil {
		h.cond.Wait()
	}
	if h.readErr != nil {
		return 0, h.readErr
	}
	if h.buf.Len() == 0 && h.closed {
		return 0, io.EOF
	}
	return h.buf.Read(p)
}

// closeWriter marks the peer's read side finished (io.EOF).
func (h *pipeHalf) closeWriter() {
	h.mu.Lock()
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *pipeHalf) setReadErr(err error) {
	h.mu.Lock()
	h.readErr = err
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *pipeHalf) setWriteErr(err error) {
	h.mu.Lock()
	h.writeErr = err
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *pipeHalf) blocked() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buf.Len() > 0
}

// scriptedConn is an in-memory net.Conn with controllable writes. Writes go
// through a gate which can pause a single write (holding the bytes back from
// the peer), release it on demand, or fail it with an injected transport
// error. Either side can be closed independently of the peer.
type scriptedConn struct {
	side string

	// readHalf is the half this conn reads from (written by the peer).
	readHalf *pipeHalf
	// writeHalf is the half this conn writes into (read by the peer).
	writeHalf *pipeHalf

	writeMu     sync.Mutex
	gateCond    *sync.Cond
	paused      bool
	releasedOne bool
	failNext    error
	failAll     error
	parked      chan struct{}
	parkedOnce  sync.Once
	closeMu     sync.Mutex
	closed      bool
}

func newScriptedConn(side string, readHalf, writeHalf *pipeHalf) *scriptedConn {
	c := &scriptedConn{
		side:      side,
		readHalf:  readHalf,
		writeHalf: writeHalf,
		parked:    make(chan struct{}),
	}
	c.gateCond = sync.NewCond(&c.writeMu)
	return c
}

// newScriptedPair returns the client-side and server-side conns of a single
// in-memory connection.
func newScriptedPair() (client *scriptedConn, server *scriptedConn) {
	cToS := newPipeHalf() // client writes, server reads
	sToC := newPipeHalf() // server writes, client reads
	return newScriptedConn("client", sToC, cToS), newScriptedConn("server", cToS, sToC)
}

// Read implements net.Conn.
func (c *scriptedConn) Read(p []byte) (int, error) { return c.readHalf.read(p) }

// Write implements net.Conn. It passes through the write gate before
// delivering the bytes to the peer.
func (c *scriptedConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	if c.closed {
		c.writeMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if c.failAll != nil {
		err := c.failAll
		c.writeMu.Unlock()
		return 0, err
	}
	if c.failNext != nil {
		err := c.failNext
		c.failNext = nil
		c.writeMu.Unlock()
		return 0, err
	}
	for c.paused {
		if c.releasedOne {
			c.releasedOne = false
			break
		}
		c.markParked()
		c.gateCond.Wait()
	}
	c.writeMu.Unlock()
	return c.writeHalf.write(p)
}

func (c *scriptedConn) markParked() {
	c.parkedOnce.Do(func() { close(c.parked) })
}

// pauseWrites parks every subsequent write until resumeWrites or releaseOne.
func (c *scriptedConn) pauseWrites() {
	c.writeMu.Lock()
	c.paused = true
	c.writeMu.Unlock()
}

// releaseOne lets exactly one parked write through and keeps further writes
// paused until resumeWrites.
func (c *scriptedConn) releaseOne() {
	c.writeMu.Lock()
	c.releasedOne = true
	c.gateCond.Broadcast()
	c.writeMu.Unlock()
}

// resumeWrites releases all parked and future writes.
func (c *scriptedConn) resumeWrites() {
	c.writeMu.Lock()
	c.paused = false
	c.releasedOne = false
	c.gateCond.Broadcast()
	c.writeMu.Unlock()
}

// failNextWrite makes the next write fail with err without delivering bytes.
func (c *scriptedConn) failNextWrite(err error) {
	c.writeMu.Lock()
	c.failNext = err
	c.gateCond.Broadcast()
	c.writeMu.Unlock()
}

// failAllWrites makes every subsequent write fail with err.
func (c *scriptedConn) failAllWrites(err error) {
	c.writeMu.Lock()
	c.failAll = err
	c.gateCond.Broadcast()
	c.writeMu.Unlock()
}

// waitParked blocks until a write is parked at the gate.
func (c *scriptedConn) waitParked() { <-c.parked }

// Close implements net.Conn. It stops local writes and ends the peer's reads.
func (c *scriptedConn) Close() error {
	c.writeMu.Lock()
	if c.closed {
		c.writeMu.Unlock()
		return nil
	}
	c.closed = true
	c.paused = false
	c.gateCond.Broadcast()
	c.writeMu.Unlock()

	c.writeHalf.closeWriter()
	return nil
}

// failPeerRead injects a read error for the peer, simulating a terminal
// transport error observed by the peer's receive loop.
func (c *scriptedConn) failPeerRead(err error) {
	c.writeHalf.setReadErr(err)
}

func (c *scriptedConn) LocalAddr() net.Addr  { return scriptedAddr{side: c.side} }
func (c *scriptedConn) RemoteAddr() net.Addr { return scriptedAddr{side: "remote"} }
func (*scriptedConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// -----------------------------------------------------------------------------
// Wire event recorder
// -----------------------------------------------------------------------------

type wireEvent struct {
	dir  string // "c2s" or "s2c"
	hdr  messageHeader
	data []byte // copied payload, nil for empty frames
}

func (e wireEvent) String() string {
	return fmt.Sprintf("%s:%s sid=%d flags=0x%x len=%d", e.dir, e.hdr.Type, e.hdr.StreamID, e.hdr.Flags, e.hdr.Length)
}

type streamLifecycle struct {
	event string // "added" or "removed"
	id    streamID
}

// eventLog is a totally ordered log of wire frames and server-side stream
// map transitions, recorded at the moment bytes cross the transport.
type eventLog struct {
	mu        sync.Mutex
	cond      *sync.Cond
	wires     []wireEvent
	lifecycle []streamLifecycle
}

func newEventLog() *eventLog {
	e := &eventLog{}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *eventLog) recordWire(ev wireEvent) {
	e.mu.Lock()
	e.wires = append(e.wires, ev)
	e.cond.Broadcast()
	e.mu.Unlock()
}

func (e *eventLog) recordLifecycle(ev streamLifecycle) {
	e.mu.Lock()
	e.lifecycle = append(e.lifecycle, ev)
	e.cond.Broadcast()
	e.mu.Unlock()
}

// waitUntil runs pred under the log lock until it accepts or d elapses. It
// returns the current snapshots without further locking.
func (e *eventLog) waitUntil(t testing.TB, what string, d time.Duration, pred func(wires []wireEvent, lc []streamLifecycle) bool) ([]wireEvent, []streamLifecycle) {
	t.Helper()
	done := make(chan struct{})
	var (
		ok     bool
		once   sync.Once
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.mu.Lock()
		defer e.mu.Unlock()
		for !pred(e.wires, e.lifecycle) {
			e.cond.Wait()
		}
		ok = true
		once.Do(func() { close(done) })
	}()
	select {
	case <-done:
	case <-time.After(d):
	}
	once.Do(func() { close(done) })
	e.mu.Lock()
	defer e.mu.Unlock()
	if !ok {
		// Wake the waiter if it is still parked on the condition; it will
		// re-check and exit on the next broadcast.
		e.cond.Broadcast()
		t.Fatalf("timed out waiting for %s; wires=%v lifecycle=%v", what, e.wires, e.lifecycle)
	}
	wires := append([]wireEvent(nil), e.wires...)
	lc := append([]streamLifecycle(nil), e.lifecycle...)
	return wires, lc
}

func (e *eventLog) wireCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.wires)
}

func (e *eventLog) lifecycleSnapshot() []streamLifecycle {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]streamLifecycle(nil), e.lifecycle...)
}

// frameAssembler collects byte chunks from a pipeHalf and emits complete
// ttrpc frames. It tolerates writes being split or coalesced.
type frameAssembler struct {
	pending []byte
}

func (a *frameAssembler) push(chunk []byte, emit func(messageHeader, []byte)) {
	a.pending = append(a.pending, chunk...)
	for len(a.pending) >= messageHeaderLength {
		hdr := messageHeader{
			Length:   binary.BigEndian.Uint32(a.pending[0:4]),
			StreamID: binary.BigEndian.Uint32(a.pending[4:8]),
			Type:     messageType(a.pending[8]),
			Flags:    a.pending[9],
		}
		total := messageHeaderLength + int(hdr.Length)
		if len(a.pending) < total {
			return
		}
		payload := append([]byte(nil), a.pending[messageHeaderLength:total]...)
		emit(hdr, payload)
		a.pending = a.pending[total:]
	}
}

// -----------------------------------------------------------------------------
// Harness
// -----------------------------------------------------------------------------

const (
	contractService = "contractService"

	methodUnary      = "Unary"
	methodC2S        = "C2S"
	methodS2C        = "S2C"
	methodBidi       = "Bidi"
	methodUnaryData  = "UnaryData"
	methodS2CNoWait  = "S2CFast"
)

type gate struct {
	ch     chan struct{}
	mu     sync.Mutex
	opened bool
}

func newGate(open bool) *gate {
	g := &gate{ch: make(chan struct{})}
	if open {
		close(g.ch)
		g.opened = true
	}
	return g
}

func (g *gate) wait() {
	if g != nil {
		<-g.ch
	}
}

func (g *gate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.opened {
		g.opened = true
		close(g.ch)
	}
}

// contractHarness bundles a server, a client connected over scripted
// in-memory conns, the shared event log, and handler coordination gates.
type contractHarness struct {
	t      *testing.T
	server *Server
	sc     *serverConn
	client *Client
	cconn  *scriptedConn
	sconn  *scriptedConn
	log    *eventLog

	// Handler synchronization. A gate is open by default; tests can replace
	// it with a closed gate to park a handler at a defined point.
	unaryGate   *gate
	s2cGate     *gate
	handlerExit map[string]chan struct{}
	exitMu      sync.Mutex
}

func newContractHarness(t *testing.T) *contractHarness {
	t.Helper()
	server := mustServer(t)(NewServer())
	cconn, sconn := newScriptedPair()
	ev := newEventLog()

	// Record wire events in the exact order bytes are handed to the
	// transport, before the peer can observe them.
	var ca, sa frameAssembler
	cconn.writeHalf.onData = func(p []byte) {
		ca.push(p, func(hdr messageHeader, data []byte) {
			ev.recordWire(wireEvent{dir: "c2s", hdr: hdr, data: data})
		})
	}
	sconn.writeHalf.onData = func(p []byte) {
		sa.push(p, func(hdr messageHeader, data []byte) {
			ev.recordWire(wireEvent{dir: "s2c", hdr: hdr, data: data})
		})
	}

	h := &contractHarness{
		t:            t,
		server:       server,
		cconn:        cconn,
		sconn:        sconn,
		log:          ev,
		unaryGate:    newGate(true),
		s2cGate:      newGate(true),
		handlerExit:  make(map[string]chan struct{}),
	}
	h.registerServices()

	sc, err := server.newConn(sconn, nil)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	sc.streamAdded = func(id streamID) { ev.recordLifecycle(streamLifecycle{"added", id}) }
	sc.streamRemoved = func(id streamID) { ev.recordLifecycle(streamLifecycle{"removed", id}) }
	h.sc = sc
	go sc.run(context.Background())

	h.client = NewClient(cconn)
	return h
}

func (h *contractHarness) signalExit(key string) {
	h.exitMu.Lock()
	ch, ok := h.handlerExit[key]
	if !ok {
		ch = make(chan struct{})
		h.handlerExit[key] = ch
	}
	h.exitMu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (h *contractHarness) waitHandlerExit(key string) {
	h.exitMu.Lock()
	ch, ok := h.handlerExit[key]
	if !ok {
		ch = make(chan struct{})
		h.handlerExit[key] = ch
	}
	h.exitMu.Unlock()
	waitChannel(h.t, "handler "+key+" exit", testTimeout, ch)
}

const testTimeout = 5 * time.Second

func (h *contractHarness) registerServices() {
	desc := &ServiceDesc{
		Methods: map[string]Method{
			methodUnary: func(ctx context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				h.unaryGate.wait()
				req.Seq++
				return &req, nil
			},
			methodUnaryData: func(ctx context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				_ = unmarshal(&req)
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			methodC2S: {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					var count int64
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								return &internal.EchoPayload{Seq: count}, nil
							}
							return nil, err
						}
						count++
					}
				},
				StreamingClient: true,
			},
			methodS2C: {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					var req internal.EchoPayload
					if err := ss.RecvMsg(&req); err != nil && err != io.EOF {
						return nil, err
					}
					h.s2cGate.wait()
					for i := int64(0); i < req.Seq; i++ {
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i + 1, Msg: "data"}); err != nil {
							return nil, err
						}
					}
					return &internal.EchoPayload{Seq: req.Seq, Msg: "done"}, nil
				},
				StreamingServer: true,
			},
			methodS2CNoWait: {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					var req internal.EchoPayload
					if err := ss.RecvMsg(&req); err != nil && err != io.EOF {
						return nil, err
					}
					if err := ss.SendMsg(&internal.EchoPayload{Seq: 100}); err != nil {
						return nil, err
					}
					return &internal.EchoPayload{Seq: 101}, nil
				},
				StreamingServer: true,
			},
			methodBidi: {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					var count int64
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								return &internal.EchoPayload{Seq: count}, nil
							}
							return nil, err
						}
						count++
						req.Msg = "echo:" + req.Msg
						if err := ss.SendMsg(&req); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	h.server.RegisterService(contractService, desc)
}

// shutdown cleanly stops the server and client, waiting for the server conn
// goroutine to exit so no goroutines or registered state escape the test.
func (h *contractHarness) shutdown() {
	h.unaryGate.open()
	h.s2cGate.open()
	h.client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_ = h.server.Shutdown(ctx)
	waitChannel(h.t, "server conn runDone", testTimeout, h.sc.runDone)
	waitChannel(h.t, "client userCloseWait", testTimeout, h.client.userCloseWaitCh)
}

// -----------------------------------------------------------------------------
// Assertion / framing helpers
// -----------------------------------------------------------------------------

func waitChannel(t testing.TB, what string, d time.Duration, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitWires blocks until at least n wire events are recorded and returns a
// snapshot of all events so far.
func (h *contractHarness) waitWires(n int) []wireEvent {
	wires, _ := h.log.waitUntil(h.t, fmt.Sprintf("%d wire events", n), testTimeout,
		func(w []wireEvent, _ []streamLifecycle) bool { return len(w) >= n })
	return wires
}

// waitWire blocks until the event log contains a wire event at the given
// index and returns it.
func (h *contractHarness) waitWire(index int) wireEvent {
	h.waitWires(index + 1)
	h.log.mu.Lock()
	defer h.log.mu.Unlock()
	return h.log.wires[index]
}

// assertWiresPrefix asserts the full wire sequence (exact length and order).
func (h *contractHarness) assertWiresExact(want []wireExpect) {
	h.t.Helper()
	wires, _ := h.log.waitUntil(h.t, fmt.Sprintf("%d exact wires", len(want)), testTimeout,
		func(w []wireEvent, _ []streamLifecycle) bool { return len(w) >= len(want) })
	if len(wires) != len(want) {
		h.t.Fatalf("wire count = %d, want %d\ngot: %s", len(wires), len(want), formatWires(wires))
	}
	for i := range want {
		assertWire(h.t, wires[i], want[i], i)
	}
}

// assertWiresPrefix asserts that the first len(want) recorded frames match.
func (h *contractHarness) assertWiresPrefix(want []wireExpect) {
	h.t.Helper()
	wires := h.waitWires(len(want))
	for i := range want {
		assertWire(h.t, wires[i], want[i], i)
	}
}

func formatWires(ws []wireEvent) string {
	var b bytes.Buffer
	for i, w := range ws {
		fmt.Fprintf(&b, "\n  [%d] %s", i, w)
	}
	return b.String()
}

// wireExpect is an expectation for one recorded wire frame.
type wireExpect struct {
	dir   string
	typ   messageType
	sid   uint32
	flags uint8
	// hasPayload asserts the payload decodes into a non-empty message when
	// true; emptyPayload asserts a zero-length payload; skipPayload ignores
	// the payload.
	hasPayload   bool
	emptyPayload bool
}

func assertWire(t testing.TB, got wireEvent, want wireExpect, idx int) {
	t.Helper()
	if got.dir != want.dir || got.hdr.Type != want.typ ||
		got.hdr.StreamID != want.sid || got.hdr.Flags != want.flags {
		t.Fatalf("wire[%d] = %s, want dir=%s type=%s sid=%d flags=0x%x",
			idx, got, want.dir, want.typ, want.sid, want.flags)
	}
	switch {
	case want.hasPayload && len(got.data) == 0:
		t.Fatalf("wire[%d] expected non-empty payload, got empty", idx)
	case want.emptyPayload && len(got.data) != 0:
		t.Fatalf("wire[%d] expected empty payload, got %d bytes", idx, len(got.data))
	}
}

// decodeResponse extracts the ttrpc Response from a response/data payload.
func decodeResponse(t testing.TB, ev wireEvent) *Response {
	t.Helper()
	resp := &Response{}
	if err := proto.Unmarshal(ev.data, resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func statusOf(resp *Response) *status.Status {
	if resp.Status == nil {
		return status.New(codes.OK, "")
	}
	return status.FromProto(resp.Status)
}

func decodeEcho(t testing.TB, ev wireEvent) *internal.EchoPayload {
	t.Helper()
	var p internal.EchoPayload
	if err := proto.Unmarshal(ev.data, &p); err != nil {
		t.Fatalf("decode echo: %v", err)
	}
	return &p
}

// -----------------------------------------------------------------------------
// Stream-set snapshots and raw frame injection
// -----------------------------------------------------------------------------

// serverStreamCount returns the current number of active server streams.
func (h *contractHarness) serverStreamCount() int {
	n := 0
	h.sc.streams.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (h *contractHarness) serverHasStream(sid streamID) bool {
	_, ok := h.sc.streams.Load(sid)
	return ok
}

func (h *contractHarness) waitServerStreams(n int) {
	_, _ = h.log.waitUntil(h.t, fmt.Sprintf("%d server streams snapshot", n), testTimeout,
		func(_ []wireEvent, _ []streamLifecycle) bool { return h.serverStreamCount() == n })
}

// clientStreamCount returns the current number of streams registered on the
// client. It takes the same lock the client uses.
func (h *contractHarness) clientStreamCount() int {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	return len(h.client.streams)
}

func (h *contractHarness) clientHasStream(sid streamID) bool {
	return h.client.getStream(sid) != nil
}

// waitLifecycle blocks until at least n lifecycle events are recorded.
func (h *contractHarness) waitLifecycle(n int) []streamLifecycle {
	_, lc := h.log.waitUntil(h.t, fmt.Sprintf("%d lifecycle events", n), testTimeout,
		func(_ []wireEvent, l []streamLifecycle) bool { return len(l) >= n })
	return lc
}

// rawSend writes one ttrpc frame from the client side, bypassing the Client.
func (h *contractHarness) rawSend(sid uint32, typ messageType, flags uint8, msg proto.Message) {
	h.t.Helper()
	var p []byte
	if msg != nil {
		var err error
		if p, err = proto.Marshal(msg); err != nil {
			h.t.Fatalf("raw marshal: %v", err)
		}
	}
	h.rawSendBytes(sid, typ, flags, p)
}

func (h *contractHarness) rawSendBytes(sid uint32, typ messageType, flags uint8, p []byte) {
	h.t.Helper()
	var hdr [messageHeaderLength]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(p)))
	binary.BigEndian.PutUint32(hdr[4:8], sid)
	hdr[8] = byte(typ)
	hdr[9] = flags
	if _, err := h.cconn.writeHalf.write(hdr[:]); err != nil {
		h.t.Fatalf("raw header write: %v", err)
	}
	if len(p) > 0 {
		if _, err := h.cconn.writeHalf.write(p); err != nil {
			h.t.Fatalf("raw payload write: %v", err)
		}
	}
}

// rawRecvFrame reads one frame on the client side, bypassing the Client
// receive loop. Used by raw-protocol scenarios.
func (h *contractHarness) rawRecvFrame() wireEvent {
	h.t.Helper()
	var hdr [messageHeaderLength]byte
	if _, err := io.ReadFull(h.cconn, hdr[:]); err != nil {
		h.t.Fatalf("raw header read: %v", err)
	}
	mh := messageHeader{
		Length:   binary.BigEndian.Uint32(hdr[0:4]),
		StreamID: binary.BigEndian.Uint32(hdr[4:8]),
		Type:     messageType(hdr[8]),
		Flags:    hdr[9],
	}
	var p []byte
	if mh.Length > 0 {
		p = make([]byte, mh.Length)
		if _, err := io.ReadFull(h.cconn, p); err != nil {
			h.t.Fatalf("raw payload read: %v", err)
		}
	}
	return wireEvent{dir: "s2c", hdr: mh, data: p}
}

// serverInjectFrame writes a frame from the server side directly into the
// client's read half, bypassing the server's send loop. The event recorder
// observes it through the onData hook, matching real wire ordering.
func (h *contractHarness) serverInjectFrame(sid uint32, typ messageType, flags uint8, msg proto.Message) {
	h.t.Helper()
	var p []byte
	if msg != nil {
		var err error
		if p, err = proto.Marshal(msg); err != nil {
			h.t.Fatalf("inject marshal: %v", err)
		}
	}
	var hdr [messageHeaderLength]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(p)))
	binary.BigEndian.PutUint32(hdr[4:8], sid)
	hdr[8] = byte(typ)
	hdr[9] = flags
	if _, err := h.sconn.writeHalf.write(hdr[:]); err != nil {
		h.t.Fatalf("inject header write: %v", err)
	}
	if len(p) > 0 {
		if _, err := h.sconn.writeHalf.write(p); err != nil {
			h.t.Fatalf("inject payload write: %v", err)
		}
	}
}

func TestMain(m *testing.M) {
	// Several scenarios deliberately trigger expected "ttrpc: ..." error
	// logs from receive loops; keep test output focused on failures.
	if err := log.SetLevel("panic"); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(m.Run())
}

// errTransportBroken is a deterministic transport error injected by tests.
var errTransportBroken = &net.OpError{
	Op:     "write",
	Net:    "scripted",
	Source: scriptedAddr{side: "client"},
	Addr:   scriptedAddr{side: "remote"},
	Err:    syscall.EPIPE,
}
