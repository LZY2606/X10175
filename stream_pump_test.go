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
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

// memConn is an in-memory net.Conn with TCP-like close semantics: closing
// one endpoint delivers io.EOF to the peer's reads once drained (like a
// FIN), while reads and writes on the closed endpoint itself fail with
// io.ErrClosedPipe. ttrpc's server connection cleanup is triggered by EOF,
// so these semantics matter for replaying disconnect scenarios.
type memConn struct {
	r *io.PipeReader
	w *io.PipeWriter
}

// memConnPair returns the two endpoints of a bidirectional in-memory
// connection.
func memConnPair() (a, b *memConn) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return &memConn{r: ar, w: aw}, &memConn{r: br, w: bw}
}

func (c *memConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *memConn) Write(p []byte) (int, error) { return c.w.Write(p) }

func (c *memConn) Close() error {
	// Own pending reads fail; the peer observes EOF once drained.
	c.r.CloseWithError(io.ErrClosedPipe)
	return c.w.Close()
}

func (c *memConn) LocalAddr() net.Addr              { return pipeAddr{} }
func (c *memConn) RemoteAddr() net.Addr             { return pipeAddr{} }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// pumpTimeout bounds every wait in the state machine tests. It is never
// used for synchronization: every condition waited on is guaranteed to be
// reached by the code under test. The bound only turns deadlocks into
// test failures instead of hanging the suite.
const pumpTimeout = 10 * time.Second

// pumpDir identifies a direction of travel through the wirePump.
type pumpDir int

const (
	dirC2S pumpDir = iota // client to server
	dirS2C                // server to client
)

func (d pumpDir) String() string {
	if d == dirC2S {
		return "c2s"
	}
	return "s2c"
}

// pumpFrame is a single ttrpc frame captured by the wirePump. The frame is
// held until the test releases it (forwarded to the destination) or drops
// it, which gives tests exact control over event ordering on the wire.
type pumpFrame struct {
	dir     pumpDir
	header  messageHeader
	payload []byte

	releaseCh chan struct{}
	drop      bool
}

// release forwards the captured frame to its destination.
func (f *pumpFrame) release() {
	close(f.releaseCh)
}

// discard drops the captured frame instead of forwarding it.
func (f *pumpFrame) discard() {
	f.drop = true
	close(f.releaseCh)
}

func (f *pumpFrame) String() string {
	return fmt.Sprintf("%s %s stream=%d flags=%#x len=%d", f.dir, f.header.Type, f.header.StreamID, f.header.Flags, f.header.Length)
}

// wirePump sits between a ttrpc client and server connected by in-memory
// pipes. Every frame is captured and held until the test releases it, so
// tests can pause a particular write, release frames one at a time, inject
// crafted frames, or fail the transport at an exact point, all without
// real ports, sleeps, or scheduling luck.
type wirePump struct {
	clientEnd net.Conn // endpoint handed to the ttrpc client
	serverEnd net.Conn // endpoint handed to the ttrpc server

	pumpClientEnd net.Conn // pump side of the client leg
	pumpServerEnd net.Conn // pump side of the server leg

	framesC2S chan *pumpFrame
	framesS2C chan *pumpFrame

	writeMu [2]sync.Mutex // serializes forwarded and injected writes per direction

	done      chan struct{}
	closeOnce sync.Once
}

func newWirePump() *wirePump {
	clientEnd, pumpClientEnd := memConnPair()
	pumpServerEnd, serverEnd := memConnPair()
	p := &wirePump{
		clientEnd:     clientEnd,
		serverEnd:     serverEnd,
		pumpClientEnd: pumpClientEnd,
		pumpServerEnd: pumpServerEnd,
		framesC2S:     make(chan *pumpFrame, 128),
		framesS2C:     make(chan *pumpFrame, 128),
		done:          make(chan struct{}),
	}
	go p.pump(dirC2S, pumpClientEnd, pumpServerEnd)
	go p.pump(dirS2C, pumpServerEnd, pumpClientEnd)
	return p
}

// pump copies frames from src to dst one at a time, holding each frame
// until the test releases it. A terminal read error closes dst so the
// disconnect propagates to the peer, mirroring a real connection.
func (p *wirePump) pump(dir pumpDir, src, dst net.Conn) {
	hbuf := make([]byte, messageHeaderLength)
	out := p.framesC2S
	if dir == dirS2C {
		out = p.framesS2C
	}
	for {
		mh, err := readMessageHeader(hbuf, src)
		if err != nil {
			dst.Close()
			return
		}
		var payload []byte
		if mh.Length > 0 {
			payload = make([]byte, mh.Length)
			if _, err := io.ReadFull(src, payload); err != nil {
				dst.Close()
				return
			}
		}
		f := &pumpFrame{
			dir:       dir,
			header:    mh,
			payload:   payload,
			releaseCh: make(chan struct{}),
		}
		select {
		case out <- f:
		case <-p.done:
			return
		}
		select {
		case <-f.releaseCh:
		case <-p.done:
			return
		}
		if f.drop {
			continue
		}
		if err := p.writeFrame(dir, dst, mh, payload); err != nil {
			src.Close()
			return
		}
	}
}

func (p *wirePump) writeFrame(dir pumpDir, dst net.Conn, mh messageHeader, payload []byte) error {
	hbuf := make([]byte, messageHeaderLength)
	p.writeMu[dir].Lock()
	defer p.writeMu[dir].Unlock()
	if err := writeMessageHeader(dst, hbuf, mh); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := dst.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// next waits for the next captured frame in the given direction.
func (p *wirePump) next(t *testing.T, dir pumpDir) *pumpFrame {
	t.Helper()
	ch := p.framesC2S
	if dir == dirS2C {
		ch = p.framesS2C
	}
	select {
	case f := <-ch:
		return f
	case <-time.After(pumpTimeout):
		t.Fatalf("timed out waiting for %s frame", dir)
		return nil
	}
}

// forward waits for the next frame in the given direction and releases it.
func (p *wirePump) forward(t *testing.T, dir pumpDir) *pumpFrame {
	t.Helper()
	f := p.next(t, dir)
	f.release()
	return f
}

// inject writes a crafted frame directly to the destination of the given
// direction, bypassing capture. Callers must ensure no captured frame in
// the same direction is pending release so the write order is unambiguous.
func (p *wirePump) inject(t *testing.T, dir pumpDir, mh messageHeader, payload []byte) {
	t.Helper()
	dst := p.pumpServerEnd
	if dir == dirS2C {
		dst = p.pumpClientEnd
	}
	if err := p.writeFrame(dir, dst, mh, payload); err != nil {
		t.Fatalf("failed to inject %s frame: %v", dir, err)
	}
}

// closeClientLeg fails the client side of the transport; the client
// observes closed-pipe errors on its next read or write.
func (p *wirePump) closeClientLeg() {
	p.pumpClientEnd.Close()
}

// closeServerLeg fails the server side of the transport; the server
// observes a closed-pipe error on its next read or write.
func (p *wirePump) closeServerLeg() {
	p.pumpServerEnd.Close()
}

func (p *wirePump) close() {
	p.closeOnce.Do(func() {
		close(p.done)
		p.pumpClientEnd.Close()
		p.pumpServerEnd.Close()
	})
}

// smHarness wires a ttrpc client and server together through a wirePump
// and tracks their goroutine completion signals.
type smHarness struct {
	t       *testing.T
	server  *Server
	sc      *serverConn
	client  *Client
	pump    *wirePump
	runDone chan struct{} // closed when the server connection's run loop exits
}

func newSMHarness(t *testing.T, register func(s *Server)) *smHarness {
	t.Helper()
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	if register != nil {
		register(server)
	}
	pump := newWirePump()
	sc, err := server.newConn(pump.serverEnd, nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan struct{})
	go func() {
		sc.run(context.Background())
		close(runDone)
	}()
	h := &smHarness{
		t:       t,
		server:  server,
		sc:      sc,
		client:  NewClient(pump.clientEnd),
		pump:    pump,
		runDone: runDone,
	}
	t.Cleanup(h.close)
	return h
}

func (h *smHarness) close() {
	h.client.Close()
	h.server.Close()
	h.pump.close()
	select {
	case <-h.runDone:
	case <-time.After(pumpTimeout):
		h.t.Fatalf("server connection run loop did not terminate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), pumpTimeout)
	defer cancel()
	if err := h.client.UserOnCloseWait(ctx); err != nil {
		h.t.Fatalf("client run goroutine did not terminate: %v", err)
	}
}

// clientStreamIDs returns the ids of the streams currently registered in
// the client's stream table.
func clientStreamIDs(c *Client) []streamID {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	ids := make([]streamID, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// serverStreamIDs returns the ids of the stream handlers currently tracked
// by the server connection.
func serverStreamIDs(sc *serverConn) []uint32 {
	var ids []uint32
	sc.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// checkClientStreams asserts the exact set of streams registered in the
// client's stream table. Call only at points where the table's contents
// are fully determined by prior events.
func checkClientStreams(t *testing.T, c *Client, want ...streamID) {
	t.Helper()
	got := clientStreamIDs(c)
	if len(got) != len(want) {
		t.Fatalf("client streams: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("client streams: got %v, want %v", got, want)
		}
	}
}

// waitServerStreams waits until the server connection tracks exactly the
// given set of stream ids. The condition must be guaranteed to be reached;
// the poll only absorbs goroutine scheduling between the receive goroutine
// (which stores streams) and the run loop (which deletes them).
func waitServerStreams(t *testing.T, sc *serverConn, want ...uint32) {
	t.Helper()
	waitFor(t, fmt.Sprintf("server streams to become %v", want), func() bool {
		got := serverStreamIDs(sc)
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	})
}

// waitFor polls cond until it holds, failing the test after a generous
// bound. cond must be guaranteed to become true by the code under test;
// the bound exists only to turn deadlocks into failures.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(pumpTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitClosed fails the test unless ch is closed promptly.
func waitClosed(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(pumpTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
