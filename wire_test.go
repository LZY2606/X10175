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
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

// This file implements a deterministic test driver for the ttrpc stream
// state machine. Instead of real ports, goroutine scheduling luck, or
// time.Sleep-based synchronization, it provides:
//
//   - pipeListener: an in-memory net.Listener backed by net.Pipe.
//   - writeGate: a frame-level gate interposed on conn writes. Every frame
//     is recorded in order; frames matching a hold rule pause the writer
//     until the test explicitly releases the frame or fails the write with
//     an injected transport error.
//   - wireRig: a client+server pair wired through gates on both directions,
//     with accessors for the client's live stream set, the server's
//     connection count/state, and goroutine completion signals.
//
// All waiting is done on channels or bounded condition waits; no test
// depends on a fixed-duration sleep to make progress.

// wireFrame is a single parsed ttrpc frame observed on the wire.
type wireFrame struct {
	header  messageHeader
	payload []byte
}

func parseFrame(b []byte) wireFrame {
	return wireFrame{
		header: messageHeader{
			Length:   binary.BigEndian.Uint32(b[:4]),
			StreamID: binary.BigEndian.Uint32(b[4:8]),
			Type:     messageType(b[8]),
			Flags:    b[9],
		},
		payload: b[messageHeaderLength:],
	}
}

// buildFrame serializes one raw ttrpc frame, used to inject frames that the
// client/server would never produce on their own (reused stream ids, late
// frames, ...).
func buildFrame(t *testing.T, sid uint32, mt messageType, flags uint8, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeMessageHeader(&buf, make([]byte, messageHeaderLength), messageHeader{
		Length:   uint32(len(payload)),
		StreamID: sid,
		Type:     mt,
		Flags:    flags,
	}); err != nil {
		t.Fatal(err)
	}
	buf.Write(payload)
	return buf.Bytes()
}

// heldWrite is a write paused inside the gate. The test decides the outcome.
type heldWrite struct {
	frame wireFrame
	ack   chan error
}

// release lets the paused write proceed to the underlying connection.
func (h *heldWrite) release() { h.ack <- nil }

// fail makes the paused write return err to its caller, injecting a
// deterministic transport error without touching the underlying connection.
func (h *heldWrite) fail(err error) { h.ack <- err }

// writeGate intercepts every Write on one direction of a connection.
type writeGate struct {
	mu      sync.Mutex
	holds   []func(wireFrame) bool
	log     []wireFrame
	frames  chan wireFrame  // every write attempt, in order
	pending chan *heldWrite // writes currently paused by a hold rule
	wmu     sync.Mutex      // serializes underlying writes (write vs inject)
}

func newWriteGate() *writeGate {
	return &writeGate{
		frames:  make(chan wireFrame, 1<<16),
		pending: make(chan *heldWrite, 16),
	}
}

// addHold pauses subsequent frames matching pred until released or failed.
// pred is evaluated under the gate lock, so closures may capture plain
// counters without extra synchronization.
func (g *writeGate) addHold(pred func(wireFrame) bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.holds = append(g.holds, pred)
}

func (g *writeGate) clearHolds() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.holds = nil
}

// snapshot returns the ordered log of every frame attempted so far.
func (g *writeGate) snapshot() []wireFrame {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]wireFrame(nil), g.log...)
}

func (g *writeGate) write(conn net.Conn, p []byte) (int, error) {
	b := append([]byte(nil), p...)
	f := parseFrame(b)
	g.mu.Lock()
	g.log = append(g.log, f)
	held := false
	for _, h := range g.holds {
		if h(f) {
			held = true
			break
		}
	}
	g.mu.Unlock()
	g.frames <- f
	if held {
		hw := &heldWrite{frame: f, ack: make(chan error, 1)}
		g.pending <- hw
		if err := <-hw.ack; err != nil {
			return 0, err
		}
	}
	g.wmu.Lock()
	n, err := conn.Write(b)
	g.wmu.Unlock()
	return n, err
}

// inject writes a raw frame outside the client/server send paths, serialized
// against gated writes so frames never interleave on the wire.
func (g *writeGate) inject(conn net.Conn, frame []byte) error {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	_, err := conn.Write(frame)
	return err
}

// next returns the next frame attempted on this gate, failing the test if
// none arrives.
func (g *writeGate) next(t *testing.T, what string) wireFrame {
	t.Helper()
	select {
	case f := <-g.frames:
		return f
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for frame: %s", what)
		return wireFrame{}
	}
}

// holdNext returns the next write paused by a hold rule.
func (g *writeGate) holdNext(t *testing.T, what string) *heldWrite {
	t.Helper()
	select {
	case hw := <-g.pending:
		return hw
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for held frame: %s", what)
		return nil
	}
}

// gatedConn routes all writes through a writeGate.
type gatedConn struct {
	net.Conn
	gate *writeGate
}

func (c *gatedConn) Write(p []byte) (int, error) {
	return c.gate.write(c.Conn, p)
}

// pipeListener is an in-memory net.Listener; each connect delivers the
// server end of a net.Pipe to Accept.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr("pipe") }

// wireRig bundles a client and server connected through gated pipes.
type wireRig struct {
	t          *testing.T
	server     *Server
	listener   *pipeListener
	client     *Client
	clientConn *gatedConn
	serverConn *gatedConn
	clientGate *writeGate // client -> server frames
	serverGate *writeGate // server -> client frames
}

// connect establishes a new gated client connection to the rig's server.
func (r *wireRig) connect() *Client {
	c1, c2 := net.Pipe()
	sc := &gatedConn{Conn: c2, gate: r.serverGate}
	r.listener.conns <- sc
	return NewClient(&gatedConn{Conn: c1, gate: r.clientGate})
}

func newWireRig(t *testing.T, serviceName string, desc *ServiceDesc) *wireRig {
	t.Helper()
	r := &wireRig{
		t:          t,
		listener:   newPipeListener(),
		clientGate: newWriteGate(),
		serverGate: newWriteGate(),
	}
	r.server = mustServer(t)(NewServer())
	if desc != nil {
		r.server.RegisterService(serviceName, desc)
	}
	go r.server.Serve(context.Background(), r.listener)

	c1, c2 := net.Pipe()
	r.serverConn = &gatedConn{Conn: c2, gate: r.serverGate}
	r.clientConn = &gatedConn{Conn: c1, gate: r.clientGate}
	r.listener.conns <- r.serverConn
	r.client = NewClient(r.clientConn)

	t.Cleanup(func() {
		r.client.Close()
		r.server.Close()
		r.listener.Close()
	})
	return r
}

// streamIDs returns the client's currently registered stream ids.
func (r *wireRig) streamIDs() []streamID {
	r.client.streamLock.RLock()
	defer r.client.streamLock.RUnlock()
	ids := make([]streamID, 0, len(r.client.streams))
	for id := range r.client.streams {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// connCount reports how many connections the server currently tracks.
func (r *wireRig) connCount() int {
	return r.server.countConnection()
}

// serverConnState returns the state of the (single) server connection.
func (r *wireRig) serverConnState() (connState, bool) {
	r.server.mu.Lock()
	defer r.server.mu.Unlock()
	for c := range r.server.connections {
		return c.getState()
	}
	return 0, false
}

// waitFor fails the test unless cond becomes true before the timeout. It is
// a bounded condition wait, never a substitute for synchronization.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	timeout := time.After(10 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-tick.C:
		case <-timeout:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// recvMsg calls RecvMsg, failing the test instead of hanging forever.
func recvMsg(t *testing.T, cs ClientStream, m any) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- cs.RecvMsg(m) }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("RecvMsg blocked unexpectedly")
		return nil
	}
}

// userOnCloseWait asserts the client's run goroutine has fully completed
// (receive loop exited, streams cleaned up, on-close callbacks done).
func userOnCloseWait(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.UserOnCloseWait(ctx); err != nil {
		t.Fatalf("client run goroutine did not finish: %v", err)
	}
}
