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
	"fmt"
	"io"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// smGuardTimeout bounds how long a state-machine test waits for an event
// that must happen. It is never used to synchronize: every event it guards
// is produced deterministically by the code under test, so the timeout only
// ever fires when the implementation is broken (it is a deadlock detector,
// not a sleep).
const smGuardTimeout = 15 * time.Second

// smFrame is one parsed ttrpc wire frame.
type smFrame struct {
	header  messageHeader
	payload []byte
}

func (f smFrame) String() string {
	return fmt.Sprintf("{sid=%d type=%s flags=%#x len=%d}", f.header.StreamID, f.header.Type, f.header.Flags, f.header.Length)
}

// parseSMFrames splits a raw write into the sequence of wire frames it
// carries. A single bufio flush may contain more than one frame (for
// example when a previous flush failed and its bytes were retained).
func parseSMFrames(raw []byte) ([]smFrame, error) {
	var frames []smFrame
	r := bytes.NewReader(raw)
	hdr := make([]byte, messageHeaderLength)
	for r.Len() > 0 {
		mh, err := readMessageHeader(hdr, r)
		if err != nil {
			return frames, fmt.Errorf("bad frame header: %w", err)
		}
		if uint32(r.Len()) < mh.Length {
			return frames, fmt.Errorf("truncated frame: header announces %d bytes, %d available", mh.Length, r.Len())
		}
		p := make([]byte, mh.Length)
		if _, err := io.ReadFull(r, p); err != nil {
			return frames, fmt.Errorf("bad frame payload: %w", err)
		}
		frames = append(frames, smFrame{header: mh, payload: p})
	}
	return frames, nil
}

// smWrite is a single Write call observed on a gatedConn. The write is
// paused until the test either releases it (the bytes are forwarded to the
// peer) or fails it (the write returns the injected error and no bytes are
// forwarded, mirroring a transient transport failure).
type smWrite struct {
	frames []smFrame
	ack    chan error
}

// release lets the paused write proceed to the peer.
func (w *smWrite) release() { w.ack <- nil }

// fail makes the paused write return err without forwarding any bytes.
func (w *smWrite) fail(err error) { w.ack <- err }

// gatedConn pauses every Write until the test explicitly releases or fails
// it via smWrite. Reads pass through untouched.
type gatedConn struct {
	net.Conn
	writes chan *smWrite
}

func (g *gatedConn) Write(p []byte) (int, error) {
	frames, err := parseSMFrames(p)
	if err != nil {
		return 0, fmt.Errorf("gatedConn: unparseable write: %w", err)
	}
	w := &smWrite{frames: frames, ack: make(chan error, 1)}
	g.writes <- w
	if aerr := <-w.ack; aerr != nil {
		return 0, aerr
	}
	return g.Conn.Write(p)
}

// smPeer is the scripted remote end of a connection. A drainer goroutine
// continuously reads frames so the code under test never blocks on a full
// pipe; frames are surfaced to the test in arrival order.
type smPeer struct {
	conn     net.Conn
	frames   chan smFrame
	drainErr chan error
}

func newSMPeer(conn net.Conn) *smPeer {
	p := &smPeer{
		conn:     conn,
		frames:   make(chan smFrame, 128),
		drainErr: make(chan error, 1),
	}
	go p.drain()
	return p
}

func (p *smPeer) drain() {
	defer close(p.frames)
	hdr := make([]byte, messageHeaderLength)
	for {
		mh, err := readMessageHeader(hdr, p.conn)
		if err != nil {
			p.drainErr <- err
			return
		}
		var payload []byte
		if mh.Length > 0 {
			payload = make([]byte, mh.Length)
			if _, err := io.ReadFull(p.conn, payload); err != nil {
				p.drainErr <- err
				return
			}
		}
		p.frames <- smFrame{header: mh, payload: payload}
	}
}

// send writes one raw frame to the code under test. It blocks until the
// code under test has read the frame off the wire.
func (p *smPeer) send(sid uint32, mt messageType, flags uint8, payload []byte) error {
	hdr := make([]byte, messageHeaderLength)
	if err := writeMessageHeader(p.conn, hdr, messageHeader{
		Length:   uint32(len(payload)),
		StreamID: sid,
		Type:     mt,
		Flags:    flags,
	}); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := p.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// expectFrame returns the next frame the code under test sent to the peer.
func (p *smPeer) expectFrame(t *testing.T) smFrame {
	t.Helper()
	select {
	case f, ok := <-p.frames:
		if !ok {
			t.Fatalf("peer: expected a frame, connection closed (drain error: %v)", p.drainError())
		}
		return f
	case <-time.After(smGuardTimeout):
		t.Fatal("peer: timed out waiting for frame")
		return smFrame{}
	}
}

func (p *smPeer) drainError() error {
	select {
	case err := <-p.drainErr:
		return err
	default:
		return nil
	}
}

// expectClosed asserts the code under test closed its side of the
// connection. All frames sent before the close must have been consumed
// with expectFrame first.
func (p *smPeer) expectClosed(t *testing.T) {
	t.Helper()
	select {
	case f, ok := <-p.frames:
		if ok {
			t.Fatalf("peer: expected connection close, got frame %v", f)
		}
	case <-time.After(smGuardTimeout):
		t.Fatal("peer: timed out waiting for connection close")
	}
}

// clientHarness drives a *Client over an in-memory pipe. Every client write
// is paused at the gate until the test releases or fails it; the peer side
// is fully scripted by the test.
type clientHarness struct {
	client  *Client
	gate    *gatedConn
	peer    *smPeer
	onClose chan struct{} // closed when the client's run goroutine exits
}

func newClientHarness(t *testing.T) *clientHarness {
	t.Helper()
	c1, c2 := net.Pipe()
	h := &clientHarness{
		gate:    &gatedConn{Conn: c1, writes: make(chan *smWrite, 32)},
		peer:    newSMPeer(c2),
		onClose: make(chan struct{}),
	}
	h.client = NewClient(h.gate, WithOnClose(func() { close(h.onClose) }))
	t.Cleanup(func() {
		h.client.Close()
		c2.Close()
	})
	return h
}

// expectWrite pauses until the client attempts exactly one Write and
// returns it without releasing it.
func (h *clientHarness) expectWrite(t *testing.T) *smWrite {
	t.Helper()
	select {
	case w := <-h.gate.writes:
		return w
	case <-time.After(smGuardTimeout):
		t.Fatal("client: timed out waiting for write")
		return nil
	}
}

// expectNoWrite asserts no client write is in flight. It is only meaningful
// right after a synchronous client call returned: had that call attempted a
// write, it would still be blocked waiting for a release.
func (h *clientHarness) expectNoWrite(t *testing.T) {
	t.Helper()
	select {
	case w := <-h.gate.writes:
		t.Fatalf("client: unexpected write: %v", w.frames)
	default:
	}
}

// releaseAndForward releases a paused write and asserts its frames arrive
// at the peer unchanged.
func (h *clientHarness) releaseAndForward(t *testing.T, w *smWrite) {
	t.Helper()
	w.release()
	for _, want := range w.frames {
		got := h.peer.expectFrame(t)
		if got.header != want.header {
			t.Fatalf("peer: forwarded header = %+v, want %+v", got.header, want.header)
		}
		if !bytes.Equal(got.payload, want.payload) {
			t.Fatalf("peer: forwarded payload = %x, want %x", got.payload, want.payload)
		}
	}
}

// streamIDs snapshots the client's stream table.
func (h *clientHarness) streamIDs() []uint32 {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	ids := make([]uint32, 0, len(h.client.streams))
	for id := range h.client.streams {
		ids = append(ids, uint32(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// awaitClose waits for the client's run goroutine to exit.
func (h *clientHarness) awaitClose(t *testing.T) {
	t.Helper()
	select {
	case <-h.onClose:
	case <-time.After(smGuardTimeout):
		t.Fatal("client: run goroutine did not exit")
	}
}

// unaryRoundTrip drives one full unary call through the harness, asserting
// it uses the expected stream id, and checks the echoed sequence number.
func (h *clientHarness) unaryRoundTrip(t *testing.T, wantSID uint32, reqSeq, respSeq int64) {
	t.Helper()
	resultCh := make(chan error, 1)
	var resp internal.EchoPayload
	go func() {
		resultCh <- h.client.Call(context.Background(), "sm.svc", "Echo", &internal.EchoPayload{Seq: reqSeq}, &resp)
	}()
	w := h.expectWrite(t)
	if len(w.frames) != 1 {
		t.Fatalf("unary request write carried %d frames, want 1", len(w.frames))
	}
	if got := w.frames[0].header.StreamID; got != wantSID {
		t.Fatalf("unary call stream id = %d, want %d", got, wantSID)
	}
	h.releaseAndForward(t, w)
	if err := h.peer.send(wantSID, messageTypeResponse, 0, smResponsePayload(t, codes.OK, "", mustMarshalSM(t, &internal.EchoPayload{Seq: respSeq}))); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("unary call = %v, want nil", err)
		}
	case <-time.After(smGuardTimeout):
		t.Fatal("unary call did not return")
	}
	if resp.Seq != respSeq {
		t.Fatalf("unary response Seq = %d, want %d", resp.Seq, respSeq)
	}
}

// serverHarness drives a single serverConn over an in-memory pipe, with the
// test playing the client. No listener is involved.
type serverHarness struct {
	server  *Server
	sc      *serverConn
	peer    *smPeer
	runDone chan struct{} // closed when the serverConn run goroutine exits
}

func newServerHarness(t *testing.T, register func(s *Server)) *serverHarness {
	t.Helper()
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	if register != nil {
		register(server)
	}
	c1, c2 := net.Pipe()
	sc, err := server.newConn(c1, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &serverHarness{
		server:  server,
		sc:      sc,
		peer:    newSMPeer(c2),
		runDone: make(chan struct{}),
	}
	go func() {
		sc.run(context.Background())
		close(h.runDone)
	}()
	t.Cleanup(func() {
		server.Close()
		c2.Close()
		select {
		case <-h.runDone:
		case <-time.After(smGuardTimeout):
			t.Error("server: connection goroutine did not exit")
		}
	})
	return h
}

func mustMarshalSM(t *testing.T, m any) []byte {
	t.Helper()
	p, err := protoMarshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustUnmarshalSM(t *testing.T, p []byte, m any) {
	t.Helper()
	if err := protoUnmarshal(p, m); err != nil {
		t.Fatal(err)
	}
}

// smRequestPayload builds the wire payload of a request frame.
func smRequestPayload(t *testing.T, service, method string, payload []byte) []byte {
	t.Helper()
	return mustMarshalSM(t, &Request{Service: service, Method: method, Payload: payload})
}

// smResponsePayload builds the wire payload of a response frame carrying
// the given status and message payload.
func smResponsePayload(t *testing.T, code codes.Code, msg string, payload []byte) []byte {
	t.Helper()
	return mustMarshalSM(t, &Response{Status: status.New(code, msg).Proto(), Payload: payload})
}

// expectEvent asserts the next event in a scripted sequence.
func expectEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("event = %q, want %q", got, want)
		}
	case <-time.After(smGuardTimeout):
		t.Fatalf("timed out waiting for event %q", want)
	}
}
