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
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// This file implements a scripted, in-memory transport used by the stream
// state-machine contract tests. It replaces real sockets with a frame-aware
// relay so tests can:
//
//   - pause a specific write (hold a frame matching a predicate),
//   - explicitly release a single held frame,
//   - close either side of the connection,
//   - inject a deterministic transport error on a write,
//   - inject crafted frames in either direction,
//
// without relying on real ports, scheduling luck, or time.Sleep. Timeouts are
// only ever used as deadlock guards, never for synchronization.

// wireFrame is a single parsed ttrpc frame on the scripted wire.
type wireFrame struct {
	header  messageHeader
	payload []byte
}

func (f wireFrame) bytes() []byte {
	b := make([]byte, messageHeaderLength+len(f.payload))
	binary.BigEndian.PutUint32(b[:4], uint32(len(f.payload)))
	binary.BigEndian.PutUint32(b[4:8], f.header.StreamID)
	b[8] = byte(f.header.Type)
	b[9] = f.header.Flags
	copy(b[messageHeaderLength:], f.payload)
	return b
}

// heldFrame is a frame paused inside a gate until the test releases it.
type heldFrame struct {
	frame wireFrame
	rel   chan struct{}
}

// wireGate sits on one direction of the wire. By default frames pass through
// synchronously. When armed, frames matching the predicate are held until the
// test releases them; the writer blocks meanwhile, exactly like a transport
// whose peer stopped reading.
type wireGate struct {
	name string
	peer *wireEndpoint

	mu     sync.Mutex
	armed  bool
	match  func(messageHeader) bool
	held   []*heldFrame
	notify chan wireFrame
	werr   error
}

func newWireGate(name string) *wireGate {
	return &wireGate{name: name, notify: make(chan wireFrame, 64)}
}

// hold arms the gate: subsequent frames for which match returns true are
// paused instead of delivered.
func (g *wireGate) hold(match func(messageHeader) bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = true
	g.match = match
}

// disarm returns the gate to pass-through mode.
func (g *wireGate) disarm() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = false
	g.match = nil
}

// fail makes all subsequent writes through the gate return err.
func (g *wireGate) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.werr = err
}

// heldNotify returns a channel that receives a copy of every held frame.
func (g *wireGate) heldNotify() <-chan wireFrame {
	return g.notify
}

// heldCount reports how many frames are currently paused in the gate.
func (g *wireGate) heldCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
}

// releaseNext releases the oldest held frame. It returns false if no frame is
// held.
func (g *wireGate) releaseNext() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.held) == 0 {
		return false
	}
	hf := g.held[0]
	g.held = g.held[1:]
	close(hf.rel)
	return true
}

// releaseAll releases every held frame and returns how many were released.
func (g *wireGate) releaseAll() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.held)
	for _, hf := range g.held {
		close(hf.rel)
	}
	g.held = nil
	return n
}

// abort releases all held frames and fails subsequent writes. Called when the
// owning endpoint is closed so blocked writers cannot leak.
func (g *wireGate) abort(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.werr == nil {
		g.werr = err
	}
	for _, hf := range g.held {
		close(hf.rel)
	}
	g.held = nil
}

func (g *wireGate) submit(f wireFrame) error {
	g.mu.Lock()
	if g.werr != nil {
		err := g.werr
		g.mu.Unlock()
		return err
	}
	if g.armed && (g.match == nil || g.match(f.header)) {
		hf := &heldFrame{frame: f, rel: make(chan struct{})}
		g.held = append(g.held, hf)
		g.mu.Unlock()
		g.notify <- f
		<-hf.rel
		g.peer.deliver(f)
		return nil
	}
	g.mu.Unlock()
	g.peer.deliver(f)
	return nil
}

// wireEndpoint is one end of the scripted wire and implements net.Conn.
type wireEndpoint struct {
	name  string
	peer  *wireEndpoint
	wgate *wireGate

	mu     sync.Mutex
	cond   *sync.Cond
	rbuf   []byte
	wbuf   []byte
	rerr   error
	closed bool
}

func newWireEndpoint(name string, g *wireGate) *wireEndpoint {
	e := &wireEndpoint{name: name, wgate: g}
	e.cond = sync.NewCond(&e.mu)
	return e
}

// deliver appends an incoming frame to the read buffer. Called by the peer's
// gate.
func (e *wireEndpoint) deliver(f wireFrame) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rbuf = append(e.rbuf, f.bytes()...)
	e.cond.Broadcast()
}

func (e *wireEndpoint) Read(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for len(e.rbuf) == 0 && e.rerr == nil {
		e.cond.Wait()
	}
	if len(e.rbuf) > 0 {
		n := copy(p, e.rbuf)
		e.rbuf = e.rbuf[n:]
		return n, nil
	}
	return 0, e.rerr
}

func (e *wireEndpoint) Write(p []byte) (int, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return 0, net.ErrClosed
	}
	e.wbuf = append(e.wbuf, p...)
	var frames []wireFrame
	for {
		if len(e.wbuf) < messageHeaderLength {
			break
		}
		length := binary.BigEndian.Uint32(e.wbuf[:4])
		if length > messageLengthMax {
			e.mu.Unlock()
			return 0, OversizedMessageError(int(length))
		}
		if len(e.wbuf) < messageHeaderLength+int(length) {
			break
		}
		mh := messageHeader{
			Length:   length,
			StreamID: binary.BigEndian.Uint32(e.wbuf[4:8]),
			Type:     messageType(e.wbuf[8]),
			Flags:    e.wbuf[9],
		}
		payload := append([]byte(nil), e.wbuf[messageHeaderLength:messageHeaderLength+length]...)
		frames = append(frames, wireFrame{header: mh, payload: payload})
		e.wbuf = e.wbuf[messageHeaderLength+length:]
	}
	e.mu.Unlock()

	for _, f := range frames {
		if err := e.wgate.submit(f); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Close shuts down both directions: the peer observes EOF after draining any
// buffered bytes, and held frames are released so no writer stays blocked.
func (e *wireEndpoint) Close() error {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		e.rerr = io.EOF
		e.cond.Broadcast()
	}
	e.mu.Unlock()

	e.wgate.abort(net.ErrClosed)

	e.peer.mu.Lock()
	if e.peer.rerr == nil {
		e.peer.rerr = io.EOF
	}
	e.peer.cond.Broadcast()
	e.peer.mu.Unlock()
	return nil
}

type wireAddr string

func (a wireAddr) Network() string { return "wire" }
func (a wireAddr) String() string  { return string(a) }

func (e *wireEndpoint) LocalAddr() net.Addr  { return wireAddr(e.name) }
func (e *wireEndpoint) RemoteAddr() net.Addr { return wireAddr(e.peer.name) }

func (e *wireEndpoint) SetDeadline(time.Time) error      { return nil }
func (e *wireEndpoint) SetReadDeadline(time.Time) error  { return nil }
func (e *wireEndpoint) SetWriteDeadline(time.Time) error { return nil }

// scriptedWire is a connected pair of endpoints with one gate per direction.
type scriptedWire struct {
	client *wireEndpoint
	server *wireEndpoint
	c2s    *wireGate
	s2c    *wireGate
}

func newScriptedWire() *scriptedWire {
	c2s := newWireGate("c2s")
	s2c := newWireGate("s2c")
	client := newWireEndpoint("client", c2s)
	server := newWireEndpoint("server", s2c)
	client.peer = server
	server.peer = client
	c2s.peer = server
	s2c.peer = client
	return &scriptedWire{client: client, server: server, c2s: c2s, s2c: s2c}
}

// injectS2C delivers a crafted frame directly into the client's read buffer,
// bypassing the server. Used to send stray or late frames.
func (w *scriptedWire) injectS2C(f wireFrame) {
	w.client.deliver(f)
}

// wireListener is an in-memory net.Listener fed by the test.
type wireListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newWireListener() *wireListener {
	return &wireListener{conns: make(chan net.Conn, 8), done: make(chan struct{})}
}

func (l *wireListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, ErrClosed
	}
}

func (l *wireListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *wireListener) Addr() net.Addr { return wireAddr("listener") }

// smFixture wires a Server and a Client together over a scriptedWire.
type smFixture struct {
	t        *testing.T
	server   *Server
	client   *Client
	wire     *scriptedWire
	listener *wireListener
	serveErr chan error
}

func newSMFixture(t *testing.T, desc *ServiceDesc) *smFixture {
	t.Helper()
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterService("sm", desc)

	listener := newWireListener()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(context.Background(), listener)
	}()

	wire := newScriptedWire()
	listener.conns <- wire.server
	client := NewClient(wire.client)

	f := &smFixture{
		t:        t,
		server:   server,
		client:   client,
		wire:     wire,
		listener: listener,
		serveErr: serveErr,
	}
	t.Cleanup(f.close)
	return f
}

func (f *smFixture) close() {
	f.client.Close()
	f.server.Close()
	f.listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f.client.UserOnCloseWait(ctx); err != nil {
		f.t.Errorf("client close wait: %v", err)
	}
	select {
	case err := <-f.serveErr:
		if err != ErrServerClosed {
			f.t.Errorf("unexpected serve error: %v", err)
		}
	case <-ctx.Done():
		f.t.Errorf("server.Serve did not return after Close")
	}
}

// serverConn waits until exactly one connection is registered with the server
// and returns it.
func (f *smFixture) serverConn() *serverConn {
	f.t.Helper()
	waitForCondition(f.t, "server connection to register", func() bool {
		return f.server.countConnection() == 1
	})
	f.server.mu.Lock()
	defer f.server.mu.Unlock()
	for c := range f.server.connections {
		return c
	}
	f.t.Fatal("no server connection")
	return nil
}

// clientStreamIDs returns the ids of the streams currently registered with
// the client.
func (f *smFixture) clientStreamIDs() []uint32 {
	f.client.streamLock.RLock()
	defer f.client.streamLock.RUnlock()
	var ids []uint32
	for id := range f.client.streams {
		ids = append(ids, uint32(id))
	}
	return ids
}

// serverStreamIDs returns the ids in the server connection's stream table.
func serverStreamIDs(sc *serverConn) []uint32 {
	var ids []uint32
	sc.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	return ids
}

// waitForCondition polls cond until it holds. It uses no sleeps; the deadline
// is purely a deadlock guard.
func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// waitSignal waits for a channel to close, with a deadlock guard.
func waitSignal(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// writeRawFrame writes a crafted frame from the raw endpoint through the
// gate.
func writeRawFrame(t *testing.T, ep *wireEndpoint, id uint32, mt messageType, flags uint8, payload []byte) {
	t.Helper()
	f := wireFrame{
		header:  messageHeader{StreamID: id, Type: mt, Flags: flags},
		payload: payload,
	}
	if _, err := ep.Write(f.bytes()); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
}

// readRawFrame reads a single frame from the raw endpoint, with a deadlock
// guard.
func readRawFrame(t *testing.T, ep *wireEndpoint) wireFrame {
	t.Helper()
	type result struct {
		f   wireFrame
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var hdr [messageHeaderLength]byte
		if _, err := io.ReadFull(ep, hdr[:]); err != nil {
			ch <- result{err: err}
			return
		}
		mh := messageHeader{
			Length:   binary.BigEndian.Uint32(hdr[:4]),
			StreamID: binary.BigEndian.Uint32(hdr[4:8]),
			Type:     messageType(hdr[8]),
			Flags:    hdr[9],
		}
		p := make([]byte, mh.Length)
		if _, err := io.ReadFull(ep, p); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{f: wireFrame{header: mh, payload: p}}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read raw frame: %v", r.err)
		}
		return r.f
	case <-time.After(30 * time.Second):
		t.Fatal("timed out reading raw frame")
		return wireFrame{}
	}
}

// marshalRequest builds the payload of a request frame for svc/method with an
// optional inner payload.
func marshalRequest(t *testing.T, svc, method string, payload []byte) []byte {
	t.Helper()
	p, err := codec{}.Marshal(&Request{Service: svc, Method: method, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
