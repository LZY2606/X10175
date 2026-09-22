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
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// This file contains the deterministic state-machine test driver used by
// the streaming contract tests.
//
// The driver never opens real ports and never relies on scheduling or
// time.Sleep: transport is a pair of net.Pipe connections, every write on
// either side goes through a writeGate that can pause individual frames,
// release a specific frame, or fail a write with a scripted error, and all
// synchronization happens through channels (frame delivery, server
// lifecycle events and goroutine completion).

// testWaitTimeout is a watchdog for deadlocked tests only; it is not used
// for ordering or scheduling.
const testWaitTimeout = 5 * time.Second

// writeAttempt describes one paused write. Releasing permit (exactly one
// of permit/deny, exactly once) decides the write's result. In both cases
// the underlying write has already been (or is about to be) serviced by
// the peer: with net.Pipe, delivering the header first is what lets the
// peer's receive loop make progress, so headers and payloads are written
// while the verdict channel is parked.
type writeAttempt struct {
	header messageHeader
	body   []byte
	permit chan error
}

// writeGate serializes and controls writes for one side of a connection.
//
// In automatic mode every write is delivered immediately (the gate still
// records each frame header). In manual mode each write is parked and must
// be released via permitNext or failed via failNext.
type writeGate struct {
	mu       sync.Mutex
	auto     bool
	pending  []*writeAttempt
	attempts chan *writeAttempt
	fail     error
	records  []messageHeader
}

func newWriteGate(auto bool) *writeGate {
	g := &writeGate{auto: auto}
	if !auto {
		g.attempts = make(chan *writeAttempt, 256)
	}
	return g
}

func (g *writeGate) setAuto(auto bool) {
	g.mu.Lock()
	g.auto = auto
	g.mu.Unlock()
}

// isAuto reports the current mode under the lock.
func (g *writeGate) isAuto() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.auto
}

// count returns the number of frames that reached a verdict (delivered or
// failed).
func (g *writeGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.records)
}

// headers returns a snapshot of decided frame headers.
func (g *writeGate) headers() []messageHeader {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]messageHeader, len(g.records))
	copy(out, g.records)
	return out
}

// pendingCount returns frames parked waiting for a verdict.
func (g *writeGate) pendingCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending)
}

// waitAttempt waits for the next parked write or fails the test.
func (g *writeGate) waitAttempt(t testing.TB) *writeAttempt {
	t.Helper()
	select {
	case a := <-g.attempts:
		return a
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for a paused write")
		return nil
	}
}

// waitPending blocks until at least one write is parked.
func (g *writeGate) waitPending(t testing.TB) {
	t.Helper()
	waitFor(t, "paused write", func() bool { return g.pendingCount() > 0 })
}

// permitNext waits for the next parked write and delivers it.
func (g *writeGate) permitNext(t testing.TB) *writeAttempt {
	t.Helper()
	a := g.waitAttempt(t)
	g.mu.Lock()
	g.records = append(g.records, a.header)
	g.pending = g.pending[1:]
	g.mu.Unlock()
	a.permit <- nil
	return a
}

// failNext waits for the next parked write and completes it with err.
// The frame is withheld from the underlying connection, so the peer
// never observes it.
func (g *writeGate) failNext(t testing.TB, err error) *writeAttempt {
	t.Helper()
	a := g.waitAttempt(t)
	g.mu.Lock()
	g.records = append(g.records, a.header)
	g.pending = g.pending[1:]
	g.mu.Unlock()
	a.permit <- err
	return a
}

// permitAll releases every currently parked write.
func (g *writeGate) permitAll(t testing.TB) {
	t.Helper()
	for {
		g.mu.Lock()
		n := len(g.pending)
		g.mu.Unlock()
		if n == 0 {
			return
		}
		g.permitNext(t)
	}
}

// scriptedConn wraps a net.Conn and routes Write through a writeGate.
type scriptedConn struct {
	net.Conn
	gate *writeGate
}

func (sc *scriptedConn) Write(b []byte) (int, error) {
	if len(b) < messageHeaderLength {
		return sc.Conn.Write(b)
	}
	hdr, err := readMessageHeader(make([]byte, messageHeaderLength), bytes.NewReader(b[:messageHeaderLength]))
	if err != nil {
		panic(err)
	}
	a := &writeAttempt{header: hdr, permit: make(chan error, 1)}

	sc.gate.mu.Lock()
	if sc.gate.auto {
		sc.gate.records = append(sc.gate.records, hdr)
		auto := true
		sc.gate.mu.Unlock()
		if auto {
			return sc.Conn.Write(b)
		}
	} else {
		sc.gate.pending = append(sc.gate.pending, a)
		sc.gate.mu.Unlock()
		sc.gate.attempts <- a
	}

	if werr := <-a.permit; werr != nil {
		return 0, werr
	}
	return sc.Conn.Write(b)
}

// wireFrame is a parsed ttrpc frame.
type wireFrame struct {
	header  messageHeader
	payload []byte
}

// readFrame reads one full frame from r.
func readFrame(t testing.TB, r net.Conn) wireFrame {
	t.Helper()
	hdrbuf := make([]byte, messageHeaderLength)
	if _, err := readFull(r, hdrbuf); err != nil {
		t.Fatalf("reading frame header: %v", err)
	}
	hdr, err := readMessageHeader(hdrbuf, bytes.NewReader(hdrbuf))
	if err != nil {
		t.Fatalf("parsing frame header: %v", err)
	}
	f := wireFrame{header: hdr}
	if hdr.Length > 0 {
		f.payload = make([]byte, hdr.Length)
		if _, err := readFull(r, f.payload); err != nil {
			t.Fatalf("reading frame payload: %v", err)
		}
	}
	return f
}

func readFull(r net.Conn, b []byte) (int, error) {
	got := 0
	for got < len(b) {
		n, err := r.Read(b[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

// writeRawFrame writes an un-gated frame directly to a connection.
func writeRawFrame(t testing.TB, c net.Conn, hdr messageHeader, payload []byte) {
	t.Helper()
	if payload != nil {
		hdr.Length = uint32(len(payload))
	}
	buf := make([]byte, messageHeaderLength+len(payload))
	if err := writeMessageHeader(&sliceWriter{buf: buf[:0]}, make([]byte, messageHeaderLength), hdr); err != nil {
		t.Fatal(err)
	}
	if len(payload) > 0 {
		copy(buf[messageHeaderLength:], payload)
	}
	if _, err := c.Write(buf); err != nil {
		t.Fatalf("raw frame write: %v", err)
	}
}

type sliceWriter struct{ buf []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// marshalRequest builds the payload of a request frame.
func marshalRequest(t testing.TB, service, method string, payload proto.Message) []byte {
	t.Helper()
	var p []byte
	if payload != nil {
		var err error
		p, err = proto.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	b, err := proto.Marshal(&Request{Service: service, Method: method, Payload: p})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustMarshal(t testing.TB, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// waitFor blocks until fn returns true or the watchdog fires.
func waitFor(t testing.TB, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		runtime.Gosched()
	}
	if !fn() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitEvent receives the next server lifecycle event or fails the test.
func awaitEvent(t testing.TB, ch <-chan serverTestEvent) serverTestEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for server test event")
		return serverTestEvent{}
	}
}

// drainEvents reads all currently buffered events.
func drainEvents(ch <-chan serverTestEvent) []serverTestEvent {
	var out []serverTestEvent
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

var errInjectedTransport = errors.New("ttrpc-test: injected transport error")

// withClientTestHooks installs test hooks on a new client.
func withClientTestHooks(h *clientTestHooks) ClientOpts {
	return func(c *Client) {
		c.testHooks = h
	}
}

// withServerTestHooks installs test hooks on a new server.
func withServerTestHooks(h *serverTestHooks) ServerOpt {
	return func(c *serverConfig) error {
		c.testHooks = h
		return nil
	}
}

// clientStreamIDs returns a sorted-free snapshot of the client's streams.
func (c *Client) clientStreamIDs() []streamID {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	out := make([]streamID, 0, len(c.streams))
	for id := range c.streams {
		out = append(out, id)
	}
	return out
}

// testConn is the server-side connection handle exposed to tests.
func (c *serverConn) testActiveStreamIDs() []uint32 {
	if c.testStreamSnapshot == nil {
		return nil
	}
	return c.testStreamSnapshot()
}

// testConnState reports the cached conn state.
func (c *serverConn) testConnState() (connState, bool) {
	return c.getState()
}

// newServerConnForTest attaches an already established (scripted)
// connection to the server, mirroring what Serve does after Accept, but
// without a listener or port. The returned done channel is closed when the
// connection goroutine exits.
func runTestConn(ctx context.Context, s *Server, conn net.Conn) {
	sc, err := s.newConn(conn, nil)
	if err != nil {
		panic(err)
	}
	go sc.run(ctx)
}
