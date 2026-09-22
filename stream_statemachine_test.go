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

// This file implements a deterministic, replayable state-machine test
// driver for the streaming implementation. It uses net.Pipe instead of real
// ports, gates individual wire writes so the test can pause and release a
// single frame, injects deterministic transport errors, and observes the
// client/server stream tables through the package-private hooks in
// testhooks.go. No time.Sleep is used for synchronization: every step waits
// on a condition that is guaranteed to occur, with a timeout only as a
// deadlock guard.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

const smTimeout = 10 * time.Second

// errInjected is a deterministic transport error injected by the driver. It
// must survive filterCloseErr unmodified so tests can assert its identity.
var errInjected = errors.New("ttrpc-test: injected transport failure")

// eventLog records state transitions observed through the test hooks in a
// single ordered log.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(ev string) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *eventLog) count(ev string) int {
	n := 0
	for _, e := range l.snapshot() {
		if e == ev {
			n++
		}
	}
	return n
}

func (l *eventLog) index(ev string) int {
	for i, e := range l.snapshot() {
		if e == ev {
			return i
		}
	}
	return -1
}

// waitFor spins until pred holds over the event log or the deadline passes.
// The condition is guaranteed to occur in a correct implementation, so this
// never depends on scheduling luck; the deadline is only a deadlock guard.
func (l *eventLog) waitFor(t *testing.T, desc string, pred func([]string) bool) []string {
	t.Helper()
	deadline := time.Now().Add(smTimeout)
	for {
		snap := l.snapshot()
		if pred(snap) {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; events so far: %v", desc, snap)
		}
		runtime.Gosched()
	}
}

// writeGate pauses individual writes on a connection so the test can release
// or fail them one frame at a time.
type writeGate struct {
	enabled atomic.Bool
	closed  chan struct{}
	pending chan *gatedWrite
}

type gatedWrite struct {
	frame   []byte
	release chan error
}

func newWriteGate() *writeGate {
	return &writeGate{
		closed:  make(chan struct{}),
		pending: make(chan *gatedWrite, 16),
	}
}

func (g *writeGate) pause()  { g.enabled.Store(true) }
func (g *writeGate) resume() { g.enabled.Store(false) }
func (g *writeGate) close() {
	select {
	case <-g.closed:
	default:
		close(g.closed)
	}
}

// next waits for the next gated write. It fails the test if no write arrives.
func (g *writeGate) next(t *testing.T, desc string) *gatedWrite {
	t.Helper()
	select {
	case gw := <-g.pending:
		return gw
	case <-time.After(smTimeout):
		t.Fatalf("timed out waiting for gated write: %s", desc)
		return nil
	}
}

func (gw *gatedWrite) proceed() { gw.release <- nil }
func (gw *gatedWrite) fail(err error) {
	gw.release <- err
}

// header parses the ttrpc frame header of the gated write.
func (gw *gatedWrite) header(t *testing.T) messageHeader {
	t.Helper()
	if len(gw.frame) < messageHeaderLength {
		t.Fatalf("gated write too short for a header: %d bytes", len(gw.frame))
	}
	p := gw.frame
	return messageHeader{
		Length:   binary.BigEndian.Uint32(p[:4]),
		StreamID: binary.BigEndian.Uint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}
}

func (gw *gatedWrite) payload() []byte {
	if len(gw.frame) <= messageHeaderLength {
		return nil
	}
	return gw.frame[messageHeaderLength:]
}

// gatedConn wraps one end of a net.Pipe with a write gate and an injectable
// read error.
type gatedConn struct {
	net.Conn
	gate    *writeGate
	readErr atomic.Value // stores error
}

func newGatedConn(c net.Conn) *gatedConn {
	return &gatedConn{Conn: c, gate: newWriteGate()}
}

func (c *gatedConn) Write(p []byte) (int, error) {
	if c.gate.enabled.Load() {
		gw := &gatedWrite{frame: append([]byte(nil), p...), release: make(chan error, 1)}
		select {
		case c.gate.pending <- gw:
		case <-c.gate.closed:
			return 0, net.ErrClosed
		}
		select {
		case err := <-gw.release:
			if err != nil {
				return 0, err
			}
		case <-c.gate.closed:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(p)
}

func (c *gatedConn) Read(p []byte) (int, error) {
	if err, ok := c.readErr.Load().(error); ok && err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func (c *gatedConn) injectReadError(err error) { c.readErr.Store(err) }

func (c *gatedConn) Close() error {
	c.gate.close()
	return c.Conn.Close()
}

// smHarness wires a client and a server connection over net.Pipe with full
// control over both write directions.
type smHarness struct {
	t *testing.T

	events *eventLog

	server     *Server
	sconn      *serverConn
	serverDone chan struct{}

	client     *Client
	clientDone chan struct{}

	clientConn *gatedConn // client end of the pipe (client writes gated here)
	serverConn *gatedConn // server end of the pipe (server writes gated here)
}

func installHooks(t *testing.T, events *eventLog) {
	t.Helper()
	clientStreamTableHook = func(id uint32, added bool) {
		if added {
			events.add(fmt.Sprintf("c+%d", id))
		} else {
			events.add(fmt.Sprintf("c-%d", id))
		}
	}
	serverStreamTableHook = func(id uint32, added bool) {
		if added {
			events.add(fmt.Sprintf("s+%d", id))
		} else {
			events.add(fmt.Sprintf("s-%d", id))
		}
	}
	streamDeliverHook = func(id uint32) {
		events.add(fmt.Sprintf("d%d", id))
	}
	t.Cleanup(func() {
		clientStreamTableHook = nil
		serverStreamTableHook = nil
		streamDeliverHook = nil
	})
}

func newSMHarness(t *testing.T) *smHarness {
	t.Helper()
	events := &eventLog{}
	installHooks(t, events)

	c1, c2 := net.Pipe()
	h := &smHarness{
		t:          t,
		events:     events,
		server:     mustServer(t)(NewServer()),
		serverDone: make(chan struct{}),
		clientDone: make(chan struct{}),
		clientConn: newGatedConn(c1),
		serverConn: newGatedConn(c2),
	}
	return h
}

// connect starts the server connection loop and the client. Services must be
// registered before calling connect.
func (h *smHarness) connect() {
	h.t.Helper()
	sc, err := h.server.newConn(h.serverConn, nil)
	if err != nil {
		h.t.Fatalf("newConn: %v", err)
	}
	h.sconn = sc
	go func() {
		sc.run(context.Background())
		close(h.serverDone)
	}()
	h.client = NewClient(h.clientConn, WithOnClose(func() { close(h.clientDone) }))
}

// close tears down the harness and waits for every goroutine to finish so no
// state leaks into subsequent tests.
func (h *smHarness) close() {
	h.clientConn.Close()
	h.serverConn.Close()
	if h.client != nil {
		h.client.Close()
	}
	waitSignal(h.t, h.clientDone, "client run loop")
	waitSignal(h.t, h.serverDone, "server conn run loop")
	h.server.Close()
}

func waitSignal(t *testing.T, ch <-chan struct{}, desc string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(smTimeout):
		t.Fatalf("timed out waiting for %s", desc)
	}
}

// clientStreamIDs returns the sorted ids currently in the client stream table.
func (h *smHarness) clientStreamIDs() []uint32 {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	ids := make([]uint32, 0, len(h.client.streams))
	for id := range h.client.streams {
		ids = append(ids, uint32(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// serverStreamSet replays the event log to compute the current server-side
// stream table contents. Only meaningful at quiescent points.
func (h *smHarness) serverStreamSet() map[uint32]bool {
	set := map[uint32]bool{}
	for _, ev := range h.events.snapshot() {
		var id uint32
		if strings.HasPrefix(ev, "s+") {
			fmt.Sscanf(ev, "s+%d", &id)
			set[id] = true
		} else if strings.HasPrefix(ev, "s-") {
			fmt.Sscanf(ev, "s-%d", &id)
			delete(set, id)
		}
	}
	return set
}

func (h *smHarness) assertClientStreams(desc string, want ...uint32) {
	h.t.Helper()
	got := h.clientStreamIDs()
	if len(got) != len(want) {
		h.t.Fatalf("%s: client stream table = %v, want %v (events: %v)", desc, got, want, h.events.snapshot())
	}
	for i := range want {
		if got[i] != want[i] {
			h.t.Fatalf("%s: client stream table = %v, want %v (events: %v)", desc, got, want, h.events.snapshot())
		}
	}
}

// asyncCall runs fn in a goroutine and returns a channel for its result.
func asyncCall(fn func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return ch
}

func mustRecvErr(t *testing.T, ch <-chan error, desc string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(smTimeout):
		t.Fatalf("timed out waiting for %s", desc)
		return nil
	}
}

// echoHandler returns a bidi echo handler used by several scenarios.
func echoHandler() StreamHandler {
	return func(_ context.Context, ss StreamServer) (any, error) {
		for {
			var req internal.EchoPayload
			if err := ss.RecvMsg(&req); err != nil {
				if err == io.EOF {
					err = nil
				}
				return nil, err
			}
			req.Seq++
			if err := ss.SendMsg(&req); err != nil {
				return nil, err
			}
		}
	}
}

func registerEchoService(s *Server, name string, h StreamHandler, sc, ss bool) {
	s.RegisterService(name, &ServiceDesc{
		Streams: map[string]Stream{
			"Stream": {Handler: h, StreamingClient: sc, StreamingServer: ss},
		},
	})
}

// writeRawFrame writes one raw ttrpc frame to w.
func writeRawFrame(t *testing.T, w io.Writer, sid uint32, mt messageType, flags uint8, p []byte) {
	t.Helper()
	hdr := make([]byte, messageHeaderLength)
	if err := writeMessageHeader(w, hdr, messageHeader{Length: uint32(len(p)), StreamID: sid, Type: mt, Flags: flags}); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if len(p) > 0 {
		if _, err := w.Write(p); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
}

// readRawFrame reads one raw ttrpc frame from r.
func readRawFrame(t *testing.T, r io.Reader) (messageHeader, []byte) {
	t.Helper()
	hdr := make([]byte, messageHeaderLength)
	mh, err := readMessageHeader(hdr, r)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	var p []byte
	if mh.Length > 0 {
		p = make([]byte, mh.Length)
		if _, err := io.ReadFull(r, p); err != nil {
			t.Fatalf("read payload: %v", err)
		}
	}
	return mh, p
}

func marshalRequest(t *testing.T, service, method string, payload []byte) []byte {
	t.Helper()
	p, err := (codec{}).Marshal(&Request{Service: service, Method: method, Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return p
}

func marshalEcho(t *testing.T, seq int64, msg string) []byte {
	t.Helper()
	p, err := (codec{}).Marshal(&internal.EchoPayload{Seq: seq, Msg: msg})
	if err != nil {
		t.Fatalf("marshal echo: %v", err)
	}
	return p
}

// expectFrame asserts the type/flags/stream of a gated write.
func expectFrame(t *testing.T, gw *gatedWrite, sid uint32, mt messageType, flags uint8) {
	t.Helper()
	mh := gw.header(t)
	if mh.StreamID != sid || mh.Type != mt || mh.Flags != flags {
		t.Fatalf("frame = {sid:%d type:%v flags:%#x}, want {sid:%d type:%v flags:%#x}",
			mh.StreamID, mh.Type, mh.Flags, sid, mt, flags)
	}
}
