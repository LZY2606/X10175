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
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// This file implements a deterministic state-machine driver for ttrpc
// streaming tests. Instead of a real socket, client and server endpoints
// are connected through a scripted in-memory transport:
//
//   - every write lands in a per-direction "pending" buffer and is
//     invisible to the peer until the test explicitly releases the frame,
//     so any single write can be paused and replayed frame by frame;
//   - either side can be closed, or a deterministic read/write transport
//     error can be injected, at an exact point in the event sequence;
//   - the client and server stream sets and goroutine completion signals
//     are readable at every step.
//
// No ports, no time.Sleep and no scheduling luck: every synchronization
// point waits on an observable state transition (a pending frame, a map
// size, a closed channel) and only uses a timeout as a failure safeguard.

// scriptWire is one direction of the scripted transport.
type scriptWire struct {
	mu      sync.Mutex
	cond    sync.Cond
	pending []byte // written by the local endpoint, awaiting release by the test
	ready   []byte // released by the test, awaiting reads by the peer endpoint
	werr    error  // injected write error
	rerr    error  // injected read error, delivered once ready is drained
	closed  bool
}

func newScriptWire() *scriptWire {
	w := &scriptWire{}
	w.cond.L = &w.mu
	return w
}

func (w *scriptWire) Read(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.ready) == 0 && w.rerr == nil && !w.closed {
		w.cond.Wait()
	}
	if len(w.ready) > 0 {
		n := copy(p, w.ready)
		w.ready = w.ready[n:]
		return n, nil
	}
	if w.rerr != nil {
		return 0, w.rerr
	}
	return 0, io.EOF
}

func (w *scriptWire) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.werr != nil {
		return 0, w.werr
	}
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	w.pending = append(w.pending, p...)
	return len(p), nil
}

func (w *scriptWire) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.cond.Broadcast()
}

func (w *scriptWire) setWriteErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.werr = err
}

func (w *scriptWire) setReadErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rerr = err
	w.cond.Broadcast()
}

// parseFrame decodes one ttrpc frame from the front of b, returning the
// header and the total frame size. ok is false if b does not yet hold a
// complete frame.
func parseFrame(b []byte) (mh messageHeader, total int, ok bool) {
	if len(b) < messageHeaderLength {
		return messageHeader{}, 0, false
	}
	mh = messageHeader{
		Length:   binary.BigEndian.Uint32(b[:4]),
		StreamID: binary.BigEndian.Uint32(b[4:8]),
		Type:     messageType(b[8]),
		Flags:    b[9],
	}
	total = messageHeaderLength + int(mh.Length)
	if len(b) < total {
		return messageHeader{}, 0, false
	}
	return mh, total, true
}

// scriptConn is a net.Conn whose reads and writes are served by two
// scriptWires, allowing the test to interpose on every frame.
type scriptConn struct {
	rd *scriptWire
	wr *scriptWire
}

func (c *scriptConn) Read(p []byte) (int, error)  { return c.rd.Read(p) }
func (c *scriptConn) Write(p []byte) (int, error) { return c.wr.Write(p) }

func (c *scriptConn) Close() error {
	c.rd.close()
	c.wr.close()
	return nil
}

type scriptAddr string

func (a scriptAddr) Network() string { return "script" }
func (a scriptAddr) String() string  { return string(a) }

func (c *scriptConn) LocalAddr() net.Addr              { return scriptAddr("local") }
func (c *scriptConn) RemoteAddr() net.Addr             { return scriptAddr("remote") }
func (c *scriptConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptConn) SetWriteDeadline(time.Time) error { return nil }

// contractHarness drives one client/server pair over the scripted
// transport and records the released frame sequence for order assertions.
type contractHarness struct {
	t *testing.T

	c2s *scriptWire // client -> server
	s2c *scriptWire // server -> client

	client       *Client // nil in server-only (raw frame) mode
	server       *Server
	sc           *serverConn
	serverConnIO *scriptConn

	events []string // ordered log of released/injected frames, test goroutine only
}

func newContractHarness(t *testing.T, desc *ServiceDesc) *contractHarness {
	t.Helper()
	h := newServerHarness(t, desc)
	h.client = NewClient(&scriptConn{rd: h.s2c, wr: h.c2s})
	t.Cleanup(func() {
		h.client.Close()
		h.waitClientDone()
	})
	return h
}

// newServerHarness builds the server half only; the test drives the
// client-to-server wire with raw frames.
func newServerHarness(t *testing.T, desc *ServiceDesc) *contractHarness {
	t.Helper()
	h := &contractHarness{
		t:   t,
		c2s: newScriptWire(),
		s2c: newScriptWire(),
	}
	h.serverConnIO = &scriptConn{rd: h.c2s, wr: h.s2c}

	h.server = mustServer(t)(NewServer())
	if desc != nil {
		h.server.RegisterService("contract", desc)
	}
	sc, err := h.server.newConn(h.serverConnIO, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.sc = sc
	go sc.run(context.Background())

	t.Cleanup(func() {
		h.serverConnIO.Close()
		h.server.Close()
		h.waitServerConns(0)
	})
	return h
}

// waitFor spins until cond holds. The condition must be monotonic and
// guaranteed by the protocol, so the spin never depends on scheduling
// luck; the deadline is only a safeguard against a broken implementation.
func (h *contractHarness) waitFor(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timeout waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// awaitPending blocks until at least one complete frame is pending on w.
func (h *contractHarness) awaitPending(w *scriptWire, what string) {
	h.t.Helper()
	h.waitFor("pending frame: "+what, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		_, _, ok := parseFrame(w.pending)
		return ok
	})
}

// release moves exactly one frame from pending to ready, unblocking the
// peer's read of precisely that frame, and logs the event.
func (h *contractHarness) release(w *scriptWire, dir string) (messageHeader, []byte) {
	h.t.Helper()
	h.awaitPending(w, dir)
	w.mu.Lock()
	mh, total, _ := parseFrame(w.pending)
	payload := append([]byte(nil), w.pending[messageHeaderLength:total]...)
	w.ready = append(w.ready, w.pending[:total]...)
	w.pending = w.pending[total:]
	w.cond.Broadcast()
	w.mu.Unlock()
	h.logFrame(dir, mh)
	return mh, payload
}

func (h *contractHarness) releaseClientFrame() (messageHeader, []byte) {
	h.t.Helper()
	return h.release(h.c2s, "c2s")
}

func (h *contractHarness) releaseServerFrame() (messageHeader, []byte) {
	h.t.Helper()
	return h.release(h.s2c, "s2c")
}

// peekPending returns the next pending frame without delivering it.
func (h *contractHarness) peekPending(w *scriptWire, dir string) (messageHeader, []byte) {
	h.t.Helper()
	h.awaitPending(w, dir)
	w.mu.Lock()
	defer w.mu.Unlock()
	mh, total, _ := parseFrame(w.pending)
	payload := append([]byte(nil), w.pending[messageHeaderLength:total]...)
	return mh, payload
}

// dropPending discards the next pending frame without delivering it.
func (h *contractHarness) dropPending(w *scriptWire, dir string) {
	h.t.Helper()
	h.awaitPending(w, dir)
	w.mu.Lock()
	_, total, _ := parseFrame(w.pending)
	w.pending = w.pending[total:]
	w.mu.Unlock()
}

// writeFrame injects a raw frame as if the peer endpoint had sent it,
// bypassing the peer's ttrpc stack entirely.
func (h *contractHarness) writeFrame(w *scriptWire, dir string, id uint32, mt messageType, flags uint8, payload []byte) {
	h.t.Helper()
	var hdr [messageHeaderLength]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], id)
	hdr[8] = byte(mt)
	hdr[9] = flags
	w.mu.Lock()
	w.ready = append(w.ready, hdr[:]...)
	w.ready = append(w.ready, payload...)
	w.cond.Broadcast()
	w.mu.Unlock()
	h.logFrame(dir, messageHeader{Length: uint32(len(payload)), StreamID: id, Type: mt, Flags: flags})
}

func (h *contractHarness) logFrame(dir string, mh messageHeader) {
	h.events = append(h.events, fmt.Sprintf("%s %s id=%d flags=0x%x", dir, mh.Type, mh.StreamID, mh.Flags))
}

func (h *contractHarness) pendingFrames(w *scriptWire) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	b := w.pending
	for {
		_, total, ok := parseFrame(b)
		if !ok {
			return n
		}
		n++
		b = b[total:]
	}
}

func (h *contractHarness) clientStreamIDs() []uint32 {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	ids := make([]uint32, 0, len(h.client.streams))
	for id := range h.client.streams {
		ids = append(ids, uint32(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (h *contractHarness) serverStreamIDs() []uint32 {
	var ids []uint32
	h.sc.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (h *contractHarness) waitClientStreamIDs(want ...uint32) {
	h.t.Helper()
	h.waitFor(fmt.Sprintf("client stream set == %v", want), func() bool {
		return equalIDs(h.clientStreamIDs(), want)
	})
}

func (h *contractHarness) waitServerStreamIDs(want ...uint32) {
	h.t.Helper()
	h.waitFor(fmt.Sprintf("server stream set == %v", want), func() bool {
		return equalIDs(h.serverStreamIDs(), want)
	})
}

func equalIDs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (h *contractHarness) waitServerConns(n int) {
	h.t.Helper()
	h.waitFor(fmt.Sprintf("server connection count == %d", n), func() bool {
		return h.server.countConnection() == n
	})
}

// waitClientDone waits for the client's run goroutine to fully exit.
func (h *contractHarness) waitClientDone() {
	h.t.Helper()
	h.waitFor("client run loop exit", func() bool {
		select {
		case <-h.client.userCloseWaitCh:
			return true
		default:
			return false
		}
	})
}

// assertEvents pins the exact ordered frame sequence observed so far.
func (h *contractHarness) assertEvents(want ...string) {
	h.t.Helper()
	if len(h.events) != len(want) {
		h.t.Fatalf("event count = %d (%v), want %d (%v)", len(h.events), h.events, len(want), want)
	}
	for i := range want {
		if h.events[i] != want[i] {
			h.t.Fatalf("event %d = %q, want %q (all: %v)", i, h.events[i], want[i], h.events)
		}
	}
}
