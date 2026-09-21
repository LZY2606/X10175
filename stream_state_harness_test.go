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
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// errScriptedWrite is the deterministic transport error injected by
// scriptConn. It is wrapped so filterCloseErr does not rewrite it to
// ErrClosed, letting tests assert the exact surfaced error.
var errScriptedWrite = errors.New("ttrpc-test: scripted transport write failure")

type parkedWrite struct {
	release chan struct{}
}

// scriptConn wraps one end of a net.Pipe and gives tests exact control over
// writes: a gate can pause the next write (which then blocks until explicitly
// released) or make it fail with a deterministic error. Reads are served
// directly by net.Pipe (the peer must drain or answer), so framing stays
// byte-identical to a real connection.
type scriptConn struct {
	net.Conn

	mu       sync.Mutex
	parked   chan<- *parkedWrite
	inject   error
	gate     bool // pause the next write until releaseOne
	pending  *parkedWrite
	pendingN int
}

func newScriptConn(t testing.TB) (*scriptConn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	sc := &scriptConn{Conn: a}
	return sc, b
}

// parkNextWrite arms the gate for exactly one write. When that write starts,
// its parkedWrite is reported on parked and the write blocks until release or
// failWrite.
func (sc *scriptConn) parkNextWrite(parked chan<- *parkedWrite) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.gate = true
	sc.parked = parked
	sc.inject = nil
}

// failNextWrite arms the gate so the next write returns err once it has been
// observed as parked.
func (sc *scriptConn) failNextWrite(parked chan<- *parkedWrite, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.gate = true
	sc.parked = parked
	sc.inject = err
}

func (sc *scriptConn) Write(p []byte) (int, error) {
	sc.mu.Lock()
	if sc.gate {
		sc.gate = false
		pw := &parkedWrite{release: make(chan struct{})}
		sc.pending = pw
		parked := sc.parked
		inject := sc.inject
		sc.parked = nil
		sc.inject = nil
		sc.pendingN++
		sc.mu.Unlock()

		select {
		case parked <- pw:
		default:
		}
		<-pw.release
		if inject != nil {
			return 0, inject
		}
	} else {
		sc.mu.Unlock()
	}
	return sc.Conn.Write(p)
}

// pendingParked returns the currently parked write, if any.
func (sc *scriptConn) pendingParked() *parkedWrite {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.pending
}

func (sc *scriptConn) clearPending(pw *parkedWrite) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.pending == pw {
		sc.pending = nil
	}
}

// frame is one raw wire message observed or synthesized by the fake peer.
type frame struct {
	header  messageHeader
	payload []byte
}

func (f frame) flagsContains(flag uint8) bool { return f.header.Flags&flag == flag }

func mustMarshalRequest(t testing.TB, service, method string, req proto.Message, flags uint8) frame {
	t.Helper()
	var payload []byte
	if req != nil {
		p, err := proto.Marshal(req)
		if err != nil {
			t.Fatalf("marshal request payload: %v", err)
		}
		payload = p
	}
	r := &Request{Service: service, Method: method, Payload: payload}
	p, err := proto.Marshal(r)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return frame{
		header: messageHeader{Length: uint32(len(p)), Type: messageTypeRequest, Flags: flags},
		payload: p,
	}
}

func mustMarshalData(t testing.TB, msg proto.Message, flags uint8) frame {
	t.Helper()
	var p []byte
	if msg != nil {
		b, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
		p = b
	}
	return frame{
		header:  messageHeader{Length: uint32(len(p)), Type: messageTypeData, Flags: flags},
		payload: p,
	}
}

func responseFrame(t testing.TB, sid uint32, st *status.Status, msg proto.Message) frame {
	t.Helper()
	var data []byte
	if msg != nil {
		b, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal response payload: %v", err)
		}
		data = b
	}
	resp := &Response{Payload: data}
	if st != nil {
		resp.Status = st.Proto()
	}
	p, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return frame{
		header:  messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeResponse, Flags: 0},
		payload: p,
	}
}

func dataFrame(sid uint32, msg proto.Message, flags uint8) frame {
	var p []byte
	if msg != nil {
		b, _ := proto.Marshal(msg)
		p = b
	}
	return frame{
		header:  messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeData, Flags: flags},
		payload: p,
	}
}

func writeFrame(t testing.TB, conn net.Conn, f frame) {
	t.Helper()
	f.header.Length = uint32(len(f.payload))
	var hbuf [messageHeaderLength]byte
	if err := writeMessageHeader(conn, hbuf[:], f.header); err != nil {
		t.Fatalf("write frame header: %v", err)
	}
	if len(f.payload) > 0 {
		if _, err := conn.Write(f.payload); err != nil {
			t.Fatalf("write frame payload: %v", err)
		}
	}
}

func readFrame(t testing.TB, conn net.Conn) frame {
	t.Helper()
	ch := newChannel(conn)
	mh, p, err := ch.recv()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame{header: mh, payload: p}
}

// fakePeer is the other end of a connection. Frames the production side
// sends can either be drained one at a time or answered by an injected
// function. All operations are test-driven, so nothing races.
type fakePeer struct {
	t    testing.TB
	conn net.Conn
	ch   *channel
	mu   sync.Mutex
}

func newFakePeer(t testing.TB, conn net.Conn) *fakePeer {
	t.Helper()
	return &fakePeer{t: t, conn: conn, ch: newChannel(conn)}
}

func (p *fakePeer) recv() frame {
	p.mu.Lock()
	defer p.mu.Unlock()
	mh, payload, err := p.ch.recv()
	if err != nil {
		p.t.Fatalf("fake peer recv: %v", err)
	}
	return frame{header: mh, payload: payload}
}

// recvErr is like recv but surfaces the read error instead of failing the
// test (used when the production side is expected to close the connection).
func (p *fakePeer) recvErr() (frame, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	mh, payload, err := p.ch.recv()
	return frame{header: mh, payload: payload}, err
}

func (p *fakePeer) send(f frame) {
	p.mu.Lock()
	defer p.mu.Unlock()
	writeFrame(p.t, p.conn, f)
}

func (p *fakePeer) close() { p.conn.Close() }

// decodeResponse unmarshals a response frame.
func decodeResponse(t testing.TB, f frame) *Response {
	t.Helper()
	var resp Response
	if err := proto.Unmarshal(f.payload, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return &resp
}

func decodeEcho(t testing.TB, f frame) *internal.EchoPayload {
	t.Helper()
	var out internal.EchoPayload
	if err := proto.Unmarshal(f.payload[:f.header.Length], &out); err != nil {
		t.Fatalf("decode echo payload: %v", err)
	}
	return &out
}

// clientHarness pairs a Client over a scriptConn with the fake peer on the
// other end and a shared event log wired to the package-private hooks.
type clientHarness struct {
	t    *testing.T
	log  *orderedEventLog
	sc   *scriptConn
	peer *fakePeer
	cl   *Client
}

func newClientHarness(t *testing.T) *clientHarness {
	t.Helper()
	silenceStandardLogger(t)
	sc, peerConn := newScriptConn(t)
	events := newOrderedEventLog()
	withTestHooks(t, streamTestHooks{
		clientStreamRegistered: func(sid streamID) { events.record(evClientRegistered) },
		clientStreamDeleted:    func(sid streamID) { events.record(evClientDeleted) },
		clientReceiveBlocked:   func(sid streamID) { events.record(evClientBlocked) },
	})
	cl := NewClient(sc, WithOnClose(func() { events.record(evClientRun) }))
	t.Cleanup(func() {
		cl.Close()
		_ = peerConn.Close()
	})
	return &clientHarness{
		t:    t,
		log:  events,
		sc:   sc,
		peer: newFakePeer(t, peerConn),
		cl:   cl,
	}
}

func (h *clientHarness) streams() []streamID {
	h.cl.streamLock.RLock()
	defer h.cl.streamLock.RUnlock()
	ids := make([]streamID, 0, len(h.cl.streams))
	for id := range h.cl.streams {
		ids = append(ids, id)
	}
	return ids
}

func (h *clientHarness) hasStream(id streamID) bool {
	h.cl.streamLock.RLock()
	defer h.cl.streamLock.RUnlock()
	_, ok := h.cl.streams[id]
	return ok
}

func (h *clientHarness) getStreamForTest(sid streamID) *stream {
	h.cl.streamLock.RLock()
	defer h.cl.streamLock.RUnlock()
	s := h.cl.streams[sid]
	if s == nil {
		h.t.Fatalf("stream %d not registered", sid)
	}
	return s
}

// waitRecvBuffer blocks until the stream's receive buffer holds at least n
// messages, giving the receive loop a deterministic rendezvous point.
func (h *clientHarness) waitRecvBuffer(t *testing.T, sid streamID, n int) {
	t.Helper()
	s := h.getStreamForTest(sid)
	deadline := time.Now().Add(testWaitTimeout)
	for len(s.recv) < n {
		if time.Now().After(deadline) {
			t.Fatalf("stream %d buffer len = %d, want >= %d", sid, len(s.recv), n)
		}
		runtime.Gosched()
	}
}

// drainRequest receives and answers one request frame as a unary response.
func (h *clientHarness) answerUnary(st *status.Status, resp proto.Message) (uint32, frame) {
	f := h.peer.recv()
	sid := f.header.StreamID
	h.peer.send(responseFrame(h.t, sid, st, resp))
	return sid, f
}

func waitErr(t testing.TB, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for goroutine result")
		return nil
	}
}

func statusCode(err error) codes.Code { return status.Code(err) }

var _ = context.Background
