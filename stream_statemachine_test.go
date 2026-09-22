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

// This file implements a deterministic, replayable state-machine harness for
// ttrpc streaming. The harness never binds a real port, never relies on
// scheduler luck and never calls time.Sleep: every interaction with the
// system under test is driven by an explicit event (frame release, peer
// close, injected transport error) and every wait is a happens-before
// observation (a received frame, a stream-table transition, a closed
// goroutine-done channel).

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// frame is a single ttrpc wire message observed by the harness.
type frame struct {
	header  messageHeader
	payload []byte
}

func (f frame) String() string {
	return fmt.Sprintf("{stream=%d type=%s flags=0x%x len=%d}",
		f.header.StreamID, f.header.Type, f.header.Flags, f.header.Length)
}

// errInjectedTransport is the deterministic transport error the harness can
// inject into either direction of a connection.
var errInjectedTransport = errors.New("ttrpc-harness: injected transport error")

// gatedConn is a net.Conn wrapper whose writes are serialized into ttrpc
// frames and passed through a test-controlled gate. While paused, each frame
// is held inside Write until the test releases exactly one frame. The peer
// goroutine reading the other end of the pipe therefore observes frames in
// the exact order the test releases them.
type gatedConn struct {
	net.Conn

	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
	buf    []byte // unparsed remainder of the write stream

	writeErr error // injected into Write, deterministic
	closed   bool
	held     int // frames currently held inside Write
}

func newGatedConn(c net.Conn) *gatedConn {
	g := &gatedConn{Conn: c}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// setPaused determines whether subsequent frames are held inside Write.
func (g *gatedConn) setPaused(paused bool) {
	g.mu.Lock()
	g.paused = paused
	g.mu.Unlock()
	g.cond.Broadcast()
}

// releaseOneFrame allows exactly one held frame to pass. Because the gate
// re-arms after every frame while paused, a single broadcast admits exactly
// one frame.
func (g *gatedConn) releaseOneFrame() {
	g.mu.Lock()
	g.mu.Unlock()
	g.cond.Broadcast()
}

// waitHeld blocks until a frame is actually held inside Write, so the test
// can release it without relying on scheduling luck.
func (g *gatedConn) waitHeld(t testing.TB) {
	t.Helper()
	waitFor(t, "a frame held by the write gate", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.held > 0
	})
}

// injectWriteError makes all subsequent Write calls fail with err.
func (g *gatedConn) injectWriteError(err error) {
	g.mu.Lock()
	g.writeErr = err
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *gatedConn) closeGate() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.cond.Broadcast()
}

// Write parses frames out of the byte stream and, while paused, blocks until
// each frame has been individually released by the test.
func (g *gatedConn) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.writeErr != nil {
		return 0, g.writeErr
	}
	if g.closed {
		return 0, net.ErrClosed
	}

	g.buf = append(g.buf, p...)
	for {
		if len(g.buf) < messageHeaderLength {
			return len(p), nil
		}
		fl := int(binary.BigEndian.Uint32(g.buf[:4])) + messageHeaderLength
		if len(g.buf) < fl {
			return len(p), nil
		}
		// One full frame is available; hold it until released.
		for g.paused && !g.closed && g.writeErr == nil {
			g.held++
			g.cond.Wait()
			g.held--
		}
		if g.writeErr != nil {
			return 0, g.writeErr
		}
		if g.closed {
			return 0, net.ErrClosed
		}
		if _, err := g.Conn.Write(g.buf[:fl]); err != nil {
			return 0, err
		}
		g.buf = g.buf[fl:]
		if g.paused {
			// Paused again for the next frame; keep holding if more
			// frames are already buffered.
			continue
		}
	}
}

// framePump reads frames from a connection and records them in order. It is
// the harness's observation point for everything the system under test
// writes.
type framePump struct {
	conn net.Conn

	mu       sync.Mutex
	frames   []frame
	released int // frames the test has acknowledged
	done     chan struct{}
	err      error // terminal read error (nil if conn closed cleanly)
	eof      bool
}

func newFramePump(conn net.Conn) *framePump {
	fp := &framePump{conn: conn, done: make(chan struct{})}
	go fp.run()
	return fp
}

func (fp *framePump) run() {
	defer close(fp.done)
	hbuf := make([]byte, messageHeaderLength)
	for {
		mh, err := readMessageHeader(hbuf, fp.conn)
		if err != nil {
			fp.mu.Lock()
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
				strings.Contains(err.Error(), "use of closed network connection") ||
				errors.Is(err, io.ErrClosedPipe) {
				fp.eof = true
			} else {
				fp.err = err
			}
			fp.mu.Unlock()
			return
		}
		var p []byte
		if mh.Length > 0 {
			p = make([]byte, mh.Length)
			if _, err := io.ReadFull(fp.conn, p); err != nil {
				fp.mu.Lock()
				fp.err = err
				fp.mu.Unlock()
				return
			}
		}
		fp.mu.Lock()
		fp.frames = append(fp.frames, frame{header: mh, payload: p})
		fp.mu.Unlock()
	}
}

// waitFrames blocks until at least n frames have been observed.
func (fp *framePump) waitFrames(t testing.TB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		fp.mu.Lock()
		got := len(fp.frames)
		err, done := fp.err, fp.eof
		fp.mu.Unlock()
		if got >= n {
			return
		}
		if err != nil || done {
			t.Fatalf("pump terminated with %d frames (want %d), err=%v eof=%v", got, n, err, done)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d frames, observed %d", n, got)
		}
		runtime.Gosched()
	}
}

// frameAt returns the i-th observed frame (0-based).
func (fp *framePump) frameAt(t testing.TB, i int) frame {
	t.Helper()
	fp.waitFrames(t, i+1)
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.frames[i]
}

// waitDone blocks until the pump's read loop has terminated.
func (fp *framePump) waitDone(t testing.TB) {
	t.Helper()
	select {
	case <-fp.done:
	case <-time.After(10 * time.Second):
		t.Fatal("pump read loop did not terminate")
	}
}

func (fp *framePump) isEOF() bool {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.eof
}

// waitFor polls cond until it holds, failing the test after a bounded
// number of scheduler yields. cond must be a pure happens-before observation
// (channel close, atomic/map state), never a timing assumption.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition never satisfied: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// assertNotYet fails the test if cond becomes true within a few scheduler
// yields. It is used to assert ordering ("B must not happen before A") in
// the negative direction; the positive direction is always asserted via
// waitFor on an explicit event.
func assertNotYet(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			t.Fatalf("condition satisfied too early: %s", what)
		}
		runtime.Gosched()
	}
}

// clientStreamCount reports how many streams the client currently tracks.
func clientStreamCount(c *Client) int {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	return len(c.streams)
}

// clientHasStream reports whether the client tracks the given stream id.
func clientHasStream(c *Client, id streamID) bool {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	_, ok := c.streams[id]
	return ok
}

// waitStreamGone blocks until the client drops the stream from its table.
func waitStreamGone(t testing.TB, c *Client, id streamID) {
	t.Helper()
	waitFor(t, fmt.Sprintf("stream %d removed from client table", id),
		func() bool { return !clientHasStream(c, id) })
}

// waitStreamClosed blocks until the stream's recvClose is closed, i.e. the
// client has run cleanup/closeWithError for it.
func waitStreamClosed(t testing.TB, s *stream) {
	t.Helper()
	waitFor(t, fmt.Sprintf("stream %d closed", s.id), func() bool {
		select {
		case <-s.recvClose:
			return true
		default:
			return false
		}
	})
}

// clientHarness drives a *Client connected to a scripted peer. The peer
// (playing the server) is fully deterministic: it only sends frames the test
// explicitly releases, and it records every frame the client writes.
type clientHarness struct {
	t        *testing.T
	client   *Client
	gate     *gatedConn // client writes pass through here
	pump     *framePump // observes client writes
	peer     net.Conn   // test writes server frames here
	peerDone chan struct{}
}

func newClientHarness(t *testing.T) *clientHarness {
	t.Helper()
	c1, c2 := net.Pipe()
	h := &clientHarness{
		t:        t,
		gate:     newGatedConn(c1),
		pump:     newFramePump(c2),
		peer:     c2,
		peerDone: make(chan struct{}),
	}
	h.client = NewClient(h.gate)
	t.Cleanup(func() {
		h.client.Close()
		h.gate.closeGate()
		h.peer.Close()
		h.pump.waitDone(t)
	})
	return h
}

// sendFrame writes one raw frame from the scripted server to the client.
func (h *clientHarness) sendFrame(sid uint32, mt messageType, flags uint8, payload []byte) {
	h.t.Helper()
	buf := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], sid)
	buf[8] = byte(mt)
	buf[9] = flags
	copy(buf[messageHeaderLength:], payload)
	if _, err := h.peer.Write(buf); err != nil {
		h.t.Fatalf("scripted peer write failed: %v", err)
	}
}

// sendResponse delivers a messageTypeResponse frame carrying the given
// status and payload, as a real server would.
func (h *clientHarness) sendResponse(sid uint32, st *status.Status, payload []byte) {
	h.t.Helper()
	resp := &Response{}
	if st != nil {
		resp.Status = st.Proto()
	}
	resp.Payload = payload
	p, err := proto.Marshal(resp)
	if err != nil {
		h.t.Fatal(err)
	}
	h.sendFrame(sid, messageTypeResponse, 0, p)
}

func (h *clientHarness) sendData(sid uint32, flags uint8, payload []byte) {
	h.t.Helper()
	h.sendFrame(sid, messageTypeData, flags, payload)
}

// echoPayload marshals a test payload for use as a data-frame body.
func echoPayload(t testing.TB, seq int64, msg string) []byte {
	t.Helper()
	p, err := proto.Marshal(&internal.EchoPayload{Seq: seq, Msg: msg})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// requestFrame builds the payload of a messageTypeRequest frame.
func requestPayload(t testing.TB, service, method string, payload []byte) []byte {
	t.Helper()
	p, err := proto.Marshal(&Request{Service: service, Method: method, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// decodeResponse unmarshals a messageTypeResponse frame payload.
func decodeResponse(t testing.TB, f frame) *Response {
	t.Helper()
	if f.header.Type != messageTypeResponse {
		t.Fatalf("expected response frame, got %v", f)
	}
	var resp Response
	if err := proto.Unmarshal(f.payload, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

// serverHarness drives a *Server connected to a scripted client peer over
// net.Pipe, without binding any listener.
type serverHarness struct {
	t      *testing.T
	server *Server
	sc     *serverConn
	gate   *gatedConn // server writes pass through here
	pump   *framePump // observes server writes
	peer   net.Conn   // test writes client frames here
	runDone chan struct{}
}

func newServerHarness(t *testing.T, register func(s *Server)) *serverHarness {
	t.Helper()
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	if register != nil {
		register(srv)
	}
	c1, c2 := net.Pipe()
	h := &serverHarness{
		t:       t,
		server:  srv,
		gate:    newGatedConn(c1),
		pump:    newFramePump(c2),
		peer:    c2,
		runDone: make(chan struct{}),
	}
	sc, err := srv.newConn(h.gate, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.sc = sc
	go func() {
		sc.run(context.Background())
		close(h.runDone)
	}()
	t.Cleanup(func() {
		h.peer.Close()
		h.gate.closeGate()
		srv.Close()
		select {
		case <-h.runDone:
		case <-time.After(10 * time.Second):
			t.Errorf("server connection run loop did not exit")
		}
		h.pump.waitDone(t)
	})
	return h
}

// sendFrame writes one raw frame from the scripted client to the server.
func (h *serverHarness) sendFrame(sid uint32, mt messageType, flags uint8, payload []byte) {
	h.t.Helper()
	buf := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], sid)
	buf[8] = byte(mt)
	buf[9] = flags
	copy(buf[messageHeaderLength:], payload)
	if _, err := h.peer.Write(buf); err != nil {
		h.t.Fatalf("scripted peer write failed: %v", err)
	}
}

func (h *serverHarness) sendRequest(sid uint32, service, method string, payload []byte) {
	h.t.Helper()
	h.sendFrame(sid, messageTypeRequest, flagRemoteClosed, requestPayload(h.t, service, method, payload))
}

func (h *serverHarness) sendStreamRequest(sid uint32, service, method string, streamingClient bool, payload []byte) {
	h.t.Helper()
	var flags uint8
	if streamingClient {
		flags = flagRemoteOpen
	}
	h.sendFrame(sid, messageTypeRequest, flags, requestPayload(h.t, service, method, payload))
}

// connCount reports how many connections the server tracks.
func (h *serverHarness) connCount() int { return h.server.countConnection() }

// waitRunDone blocks until the server-side connection loop has exited.
func (h *serverHarness) waitRunDone(t testing.TB) {
	t.Helper()
	select {
	case <-h.runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server connection run loop did not exit")
	}
}

// streamCount returns the number of streams the server connection tracks.
// It requires the test hook installed in server.go.
func (h *serverHarness) streamCount() int { return h.sc.streamCount() }

func checkStatus(t testing.TB, err error, code codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error with code %v, got %v", code, err)
	}
	if st.Code() != code {
		t.Fatalf("expected status code %v, got %v (err=%v)", code, st.Code(), err)
	}
}
