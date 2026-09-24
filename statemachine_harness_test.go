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
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// errInjected is the deterministic transport error the harness injects into
// reads or writes. It must survive filterCloseErr unmodified so tests can
// tell injected failures apart from ordinary close errors.
var errInjected = errors.New("ttrpc statemachine test: injected transport error")

// direction identifies which way a frame travels on the harness wire.
type direction int

const (
	clientToServer direction = iota
	serverToClient
)

func (d direction) String() string {
	if d == clientToServer {
		return "c->s"
	}
	return "s->c"
}

// wireFrame is a single frame captured at the harness gate. The writing
// goroutine stays blocked until the test calls release or fail, which is
// what lets a test pause one specific write and then explicitly let it
// through.
type wireFrame struct {
	dir     direction
	header  messageHeader
	payload []byte
	raw     []byte
	ack     chan error
}

// release delivers the frame to the peer and unblocks the writer.
func (f *wireFrame) release() { f.ack <- nil }

// fail makes the blocked Write call return err; the frame is not delivered.
func (f *wireFrame) fail(err error) { f.ack <- err }

// wireEvent records one observed frame (gated or injected) in wire order.
type wireEvent struct {
	dir   direction
	typ   messageType
	flags uint8
	sid   uint32
}

func ev(dir direction, typ messageType, flags uint8, sid uint32) wireEvent {
	return wireEvent{dir: dir, typ: typ, flags: flags, sid: sid}
}

// harnessConn is a net.Conn whose writes are gated by the test and whose
// reads are fed by the peer's released writes. No real ports are involved.
type harnessConn struct {
	in      *io.PipeReader
	out     *io.PipeWriter
	readSrc *io.PipeWriter // the peer writer feeding in; closing it fails our reads
	wire    *smWire
	dir     direction // frames written by this conn travel in this direction
	closed  chan struct{}
	once    sync.Once
}

func (c *harnessConn) Read(b []byte) (int, error) { return c.in.Read(b) }

func (c *harnessConn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	n := 0
	for len(b) > 0 {
		if len(b) < messageHeaderLength {
			return n, fmt.Errorf("sm harness: short write of %d bytes", len(b))
		}
		mh := messageHeader{
			Length:   binary.BigEndian.Uint32(b[:4]),
			StreamID: binary.BigEndian.Uint32(b[4:8]),
			Type:     messageType(b[8]),
			Flags:    b[9],
		}
		fl := messageHeaderLength + int(mh.Length)
		if fl > len(b) {
			// bufio splits frames larger than its buffer across writes;
			// the harness requires whole frames per flush, so tests must
			// keep payloads small.
			return n, fmt.Errorf("sm harness: frame split across writes (have %d bytes, need %d)", len(b), fl)
		}
		f := &wireFrame{
			dir:     c.dir,
			header:  mh,
			payload: append([]byte(nil), b[messageHeaderLength:fl]...),
			raw:     append([]byte(nil), b[:fl]...),
			ack:     make(chan error),
		}
		if err := c.wire.gate(f, c.closed); err != nil {
			return n, err
		}
		if _, err := c.out.Write(f.raw); err != nil {
			return n, err
		}
		b = b[fl:]
		n += fl
	}
	return n, nil
}

func (c *harnessConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.out.Close() // peer reads observe EOF
		c.in.Close()  // peer writes observe io.ErrClosedPipe
	})
	return nil
}

// failReads makes future reads on this conn fail with err, like a
// deterministic transport failure on the peer's write path.
func (c *harnessConn) failReads(err error) {
	c.readSrc.CloseWithError(err)
}

type smAddr string

func (a smAddr) Network() string { return "sm" }
func (a smAddr) String() string  { return string(a) }

func (c *harnessConn) LocalAddr() net.Addr              { return smAddr("local") }
func (c *harnessConn) RemoteAddr() net.Addr             { return smAddr("remote") }
func (c *harnessConn) SetDeadline(time.Time) error      { return nil }
func (c *harnessConn) SetReadDeadline(time.Time) error  { return nil }
func (c *harnessConn) SetWriteDeadline(time.Time) error { return nil }

// smWire is the test driver: a pair of gated conns plus an ordered event
// log. Tests pause individual writes, release frames one at a time, close
// either side, or inject deterministic transport errors, then observe the
// stream tables and goroutine completion signals of both endpoints.
type smWire struct {
	t *testing.T

	client *harnessConn
	server *harnessConn

	mu     sync.Mutex
	events []wireEvent
	auto   map[direction]bool

	gates map[direction]chan *wireFrame
}

func newSMWire(t *testing.T) *smWire {
	t.Helper()
	c2sR, c2sW := io.Pipe()
	s2cR, s2cW := io.Pipe()
	w := &smWire{
		t:    t,
		auto: map[direction]bool{},
		gates: map[direction]chan *wireFrame{
			clientToServer: make(chan *wireFrame),
			serverToClient: make(chan *wireFrame),
		},
	}
	w.client = &harnessConn{in: s2cR, out: c2sW, readSrc: s2cW, wire: w, dir: clientToServer, closed: make(chan struct{})}
	w.server = &harnessConn{in: c2sR, out: s2cW, readSrc: c2sW, wire: w, dir: serverToClient, closed: make(chan struct{})}
	return w
}

// gate records a frame written by one endpoint. In manual mode the frame is
// handed to the test and the writer blocks until the test resolves it.
func (w *smWire) gate(f *wireFrame, closed chan struct{}) error {
	w.mu.Lock()
	w.events = append(w.events, wireEvent{dir: f.dir, typ: f.header.Type, flags: f.header.Flags, sid: f.header.StreamID})
	auto := w.auto[f.dir]
	w.mu.Unlock()
	if auto {
		return nil
	}
	select {
	case w.gates[f.dir] <- f:
	case <-closed:
		return io.ErrClosedPipe
	}
	select {
	case err := <-f.ack:
		return err
	case <-closed:
		return io.ErrClosedPipe
	}
}

// setAuto switches a direction to pass-through mode. Frames are still
// recorded in the event log but no longer pause for the test.
func (w *smWire) setAuto(dir direction, on bool) {
	w.mu.Lock()
	w.auto[dir] = on
	w.mu.Unlock()
}

// frameC exposes the gate channel for tests that need to select on it.
func (w *smWire) frameC(dir direction) <-chan *wireFrame { return w.gates[dir] }

// expectFrame waits for the next gated frame written in dir.
func (w *smWire) expectFrame(dir direction) *wireFrame {
	w.t.Helper()
	select {
	case f := <-w.gates[dir]:
		return f
	case <-time.After(10 * time.Second):
		w.t.Fatalf("sm harness: timed out waiting for %v frame", dir)
		return nil
	}
}

// expectNoFrame asserts that no frame is written in dir within a short
// window. Only used for negative assertions where a correct implementation
// never emits a frame at all, so the bounded wait cannot hide a failure.
func (w *smWire) expectNoFrame(dir direction) {
	w.t.Helper()
	select {
	case f := <-w.gates[dir]:
		w.t.Fatalf("sm harness: unexpected %v frame: type=%v flags=%#x sid=%d", dir, f.header.Type, f.header.Flags, f.header.StreamID)
	case <-time.After(50 * time.Millisecond):
	}
}

// inject writes a frame directly into the wire as if the endpoint on the
// source side of dir had sent it. Used when the test plays that endpoint.
// It blocks until the receiving endpoint reads the frame.
func (w *smWire) inject(dir direction, typ messageType, flags uint8, sid uint32, payload []byte) {
	w.t.Helper()
	mh := messageHeader{Length: uint32(len(payload)), StreamID: sid, Type: typ, Flags: flags}
	raw := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(raw[0:4], mh.Length)
	binary.BigEndian.PutUint32(raw[4:8], mh.StreamID)
	raw[8] = byte(mh.Type)
	raw[9] = mh.Flags
	copy(raw[messageHeaderLength:], payload)

	w.mu.Lock()
	w.events = append(w.events, wireEvent{dir: dir, typ: typ, flags: flags, sid: sid})
	w.mu.Unlock()

	out := w.client.out
	if dir == serverToClient {
		out = w.server.out
	}
	if _, err := out.Write(raw); err != nil {
		w.t.Fatalf("sm harness: inject %v frame failed: %v", dir, err)
	}
}

// drain discards frames arriving at the destination of dir. Required on
// directions where the test (not a real endpoint) receives released frames,
// because the underlying pipe is synchronous.
func (w *smWire) drain(dir direction) {
	in := w.server.in
	if dir == serverToClient {
		in = w.client.in
	}
	go io.Copy(io.Discard, in) //nolint:errcheck // test drain; exits when conns close
}

// assertEvents pins the exact ordered transcript of frames on the wire.
func (w *smWire) assertEvents(want ...wireEvent) {
	w.t.Helper()
	w.mu.Lock()
	got := append([]wireEvent(nil), w.events...)
	w.mu.Unlock()
	if len(got) != len(want) {
		w.t.Fatalf("sm harness: event count mismatch:\n got: %v\nwant: %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			w.t.Fatalf("sm harness: event %d mismatch: got %+v, want %+v (all: %v)", i, got[i], want[i], got)
		}
	}
}

// waitFor polls cond until it holds. cond must be monotonically guaranteed
// by the scenario, so this never relies on scheduling luck; the deadline
// only turns a broken implementation into a test failure instead of a hang.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// waitClosed waits for a completion-signal channel to close.
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// recvResult waits for one asynchronous call result.
func recvResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for call result")
		return nil
	}
}

// clientStreamIDs snapshots the client's stream table.
func clientStreamIDs(c *Client) []streamID {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	ids := make([]streamID, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	return ids
}

// serverStreamIDs snapshots a server connection's stream table.
func serverStreamIDs(sc *serverConn) []uint32 {
	var ids []uint32
	sc.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	return ids
}

const smServiceName = "smService"

// newSMClient builds a Client on the harness wire. The test plays the
// server: it releases gated client frames and injects server frames.
func newSMClient(t *testing.T, w *smWire) *Client {
	t.Helper()
	w.drain(clientToServer)
	c := NewClient(w.client)
	t.Cleanup(func() {
		w.server.Close()
		w.client.Close()
		c.Close()
		waitClosed(t, c.userCloseWaitCh, "client run loop exit")
	})
	return c
}

// newSMServer builds a Server with one harness connection, bypassing
// listeners entirely. The test plays the client: it injects client frames
// and releases gated server frames.
func newSMServer(t *testing.T, w *smWire, desc *ServiceDesc) (*Server, *serverConn) {
	t.Helper()
	w.drain(serverToClient)
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	if desc != nil {
		srv.RegisterService(smServiceName, desc)
	}
	sc, err := srv.newConn(w.server, nil)
	if err != nil {
		t.Fatal(err)
	}
	go sc.run(context.Background())
	t.Cleanup(func() {
		w.client.Close()
		w.server.Close()
		srv.Close()
		waitClosed(t, sc.done, "server connection exit")
	})
	return srv, sc
}

// requestPayload marshals the frame payload of a Request message.
func requestPayload(t *testing.T, service, method string, m proto.Message) []byte {
	t.Helper()
	req := &Request{Service: service, Method: method}
	if m != nil {
		p, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		req.Payload = p
	}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// responsePayload marshals the frame payload of a Response message.
func responsePayload(t *testing.T, st *status.Status, m proto.Message) []byte {
	t.Helper()
	resp := &Response{}
	if st != nil {
		resp.Status = st.Proto()
	}
	if m != nil {
		p, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		resp.Payload = p
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// decodeResponse unmarshals the payload of a captured Response frame.
func decodeResponse(t *testing.T, f *wireFrame) *Response {
	t.Helper()
	var resp Response
	if err := proto.Unmarshal(f.payload, &resp); err != nil {
		t.Fatalf("sm harness: decode response frame: %v", err)
	}
	return &resp
}

// statusCode extracts the grpc code carried by a Response frame.
func statusCode(t *testing.T, f *wireFrame) codes.Code {
	t.Helper()
	resp := decodeResponse(t, f)
	if resp.Status == nil {
		return codes.OK
	}
	return codes.Code(resp.Status.Code)
}
