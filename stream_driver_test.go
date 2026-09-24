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

// Deterministic state-machine test driver for ttrpc streaming.
//
// The drivers in this file let a test replay exact wire-level event
// sequences without real ports, goroutine scheduling luck, or time.Sleep:
//
//   - smRig/relay: a man-in-the-middle sitting between a real Client and a
//     real serverConn over in-memory net.Pipe connections. Because net.Pipe
//     is synchronous, a peer's write stays paused until the relay reads it,
//     and a frame is only delivered to the other side when the test
//     explicitly forwards it. The relay records every observed frame so
//     tests can assert exact event ordering.
//   - scriptedSender: a sender implementation for unit-level stream tests
//     that records frames, can pause a send until the test releases it, and
//     can return an injected transport error.
//   - unit harnesses for clientStream and streamHandler constructed directly
//     on top of a scriptedSender / recorded respond function.
//
// All blocking waits use smGuard purely as a deadlock detector; no
// synchronization is achieved by sleeping.

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// smGuard bounds every blocking wait in these tests. It is a failure
// detector, never a synchronization mechanism.
const smGuard = 30 * time.Second

// frame is one wire message observed or injected by the test driver.
type frame struct {
	header  messageHeader
	payload []byte
}

func (f frame) event(dir string) string {
	return fmt.Sprintf("%s %s sid=%d flags=%02x", dir, f.header.Type, f.header.StreamID, f.header.Flags)
}

// relay is a deterministic man-in-the-middle between a real Client and a
// real serverConn. Nothing crosses unless the test moves it.
type relay struct {
	clientConn net.Conn // relay endpoint facing the client
	serverConn net.Conn // relay endpoint facing the server
	clientCh   *channel // reads frames written by the client, writes frames to the client
	serverCh   *channel // reads frames written by the server, writes frames to the server

	mu     sync.Mutex
	events []string
}

func (r *relay) record(dir string, f frame) {
	r.mu.Lock()
	r.events = append(r.events, f.event(dir))
	r.mu.Unlock()
}

// readClient pauses until the client emits a frame, then returns it without
// forwarding. The client's write stays blocked inside channel.send until
// this call reads it off the pipe.
func (r *relay) readClient(t *testing.T) frame {
	t.Helper()
	f := recvFrame(t, r.clientCh)
	r.record("c->s", f)
	return f
}

// readServer pauses until the server emits a frame, then returns it without
// forwarding.
func (r *relay) readServer(t *testing.T) frame {
	t.Helper()
	f := recvFrame(t, r.serverCh)
	r.record("s->c", f)
	return f
}

// forwardToServer releases a previously read client frame to the server.
func (r *relay) forwardToServer(t *testing.T, f frame) {
	t.Helper()
	sendFrame(t, r.serverCh, f.header.StreamID, f.header.Type, f.header.Flags, f.payload)
}

// forwardToClient releases a previously read server frame to the client.
func (r *relay) forwardToClient(t *testing.T, f frame) {
	t.Helper()
	sendFrame(t, r.clientCh, f.header.StreamID, f.header.Type, f.header.Flags, f.payload)
}

// passClientFrame reads one client frame and forwards it to the server.
func (r *relay) passClientFrame(t *testing.T) frame {
	t.Helper()
	f := r.readClient(t)
	r.forwardToServer(t, f)
	return f
}

// passServerFrame reads one server frame and forwards it to the client.
func (r *relay) passServerFrame(t *testing.T) frame {
	t.Helper()
	f := r.readServer(t)
	r.forwardToClient(t, f)
	return f
}

// injectToClient writes a fabricated frame towards the client, as if the
// server had sent it.
func (r *relay) injectToClient(t *testing.T, sid uint32, mt messageType, flags uint8, p []byte) {
	t.Helper()
	f := frame{header: messageHeader{Length: uint32(len(p)), StreamID: sid, Type: mt, Flags: flags}, payload: p}
	r.record("s->c", f)
	sendFrame(t, r.clientCh, sid, mt, flags, p)
}

// closeToServer injects a deterministic transport failure towards the
// server: the server's receive loop observes EOF.
func (r *relay) closeToServer(t *testing.T) {
	t.Helper()
	if err := r.serverConn.Close(); err != nil {
		t.Fatalf("relay: close server side: %v", err)
	}
}

// expectEvents asserts the exact ordered sequence of frames the relay has
// observed so far.
func (r *relay) expectEvents(t *testing.T, want ...string) {
	t.Helper()
	r.mu.Lock()
	got := append([]string(nil), r.events...)
	r.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("event count mismatch:\n got: %q\nwant: %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d mismatch:\n got: %q\nwant: %q", i, got, want)
		}
	}
}

// expectServerEOF asserts that the server side of the connection has been
// closed, i.e. serverConn.run returned and ran its deferred conn.Close.
// Because the run loop only honors shutdown while idle, observing this EOF
// proves the server drained every active stream first.
func (r *relay) expectServerEOF(t *testing.T) {
	t.Helper()
	expectEOF(t, r.serverCh)
}

// smRig wires a real Client to a real serverConn through a relay.
type smRig struct {
	relay  *relay
	client *Client
	server *Server
	sc     *serverConn
	cancel context.CancelFunc
}

func newSMRig(t *testing.T) *smRig {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	clientEnd, relayClientEnd := net.Pipe()
	relayServerEnd, serverEnd := net.Pipe()

	server := mustServer(t)(NewServer())
	sc, err := server.newConn(serverEnd, nil)
	if err != nil {
		t.Fatalf("create server conn: %v", err)
	}
	go sc.run(ctx)

	client := NewClient(clientEnd)

	rig := &smRig{
		relay: &relay{
			clientConn: relayClientEnd,
			serverConn: relayServerEnd,
			clientCh:   newChannel(relayClientEnd),
			serverCh:   newChannel(relayServerEnd),
		},
		client: client,
		server: server,
		sc:     sc,
		cancel: cancel,
	}
	t.Cleanup(rig.close)
	return rig
}

func (r *smRig) close() {
	r.client.Close()
	r.sc.close()
	r.server.Close()
	r.relay.clientConn.Close()
	r.relay.serverConn.Close()
	r.cancel()
}

// recvFrame reads a single frame off ch, failing the test if none arrives.
func recvFrame(t *testing.T, ch *channel) frame {
	t.Helper()
	type result struct {
		mh  messageHeader
		p   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		mh, p, err := ch.recv()
		done <- result{mh, p, err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("recv frame: %v", got.err)
		}
		return frame{header: got.mh, payload: got.p}
	case <-time.After(smGuard):
		t.Fatal("timed out waiting for frame")
		return frame{}
	}
}

// sendFrame writes a single frame to ch, failing the test if the peer never
// reads it.
func sendFrame(t *testing.T, ch *channel, sid uint32, mt messageType, flags uint8, p []byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- ch.send(sid, mt, flags, p)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("send frame: %v", err)
		}
	case <-time.After(smGuard):
		t.Fatal("timed out sending frame")
	}
}

// expectEOF asserts the peer closed its end of ch.
func expectEOF(t *testing.T, ch *channel) {
	t.Helper()
	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, _, err := ch.recv()
		done <- result{err}
	}()
	select {
	case got := <-done:
		if got.err != io.EOF {
			t.Fatalf("expected EOF, got %v", got.err)
		}
	case <-time.After(smGuard):
		t.Fatal("timed out waiting for EOF")
	}
}

// waitClosed fails the test unless ch is closed promptly.
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(smGuard):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitFor polls cond until it holds. It is only used for conditions that are
// guaranteed to become true (e.g. a state machine reaching idle); polling
// merely observes the transition.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(smGuard)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// streamIDs returns the client's currently registered stream ids, sorted.
func streamIDs(c *Client) []streamID {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()
	ids := make([]streamID, 0, len(c.streams))
	for id := range c.streams {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// sentFrame records one send through a scriptedSender.
type sentFrame struct {
	id    uint32
	mt    messageType
	flags uint8
	p     []byte
}

// scriptedSender is a sender that records frames, can pause a send until the
// test explicitly releases it, and can return an injected transport error.
type scriptedSender struct {
	mu      sync.Mutex
	frames  []sentFrame
	err     error // returned by every send while set
	pause   bool  // while true, sends block until released
	waiting chan struct{}
	release chan struct{}
}

func newScriptedSender() *scriptedSender {
	return &scriptedSender{
		waiting: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
}

func (s *scriptedSender) send(id uint32, mt messageType, flags uint8, p []byte) error {
	s.mu.Lock()
	s.frames = append(s.frames, sentFrame{id: id, mt: mt, flags: flags, p: append([]byte(nil), p...)})
	err := s.err
	pause := s.pause
	s.mu.Unlock()
	if pause {
		s.waiting <- struct{}{}
		<-s.release
	}
	return err
}

func (s *scriptedSender) sent() []sentFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentFrame(nil), s.frames...)
}

// waitPaused blocks until one send is paused inside the gate.
func (s *scriptedSender) waitPaused(t *testing.T) {
	t.Helper()
	waitClosed(t, s.waiting, "paused send")
}

// releaseOne releases exactly one paused send.
func (s *scriptedSender) releaseOne() {
	s.release <- struct{}{}
}

// newUnitClientStream builds a clientStream (and its owning Client stream
// table) directly on top of a scriptedSender, with no connection involved.
func newUnitClientStream(ctx context.Context, desc *StreamDesc, recvBuf int) (*clientStream, *Client, *stream, *scriptedSender) {
	sender := newScriptedSender()
	c := &Client{
		codec:   codec{},
		streams: make(map[streamID]*stream),
		channel: &channel{},
	}
	s := newStream(1, sender, recvBuf)
	c.streams[s.id] = s
	cs := &clientStream{ctx: ctx, s: s, c: c, desc: desc}
	return cs, c, s, sender
}

// deliver pushes one inbound message into s exactly the way the client
// receive loop does.
func deliver(t *testing.T, s *stream, mt messageType, flags uint8, p []byte) {
	t.Helper()
	mh := messageHeader{Length: uint32(len(p)), StreamID: uint32(s.id), Type: mt, Flags: flags}
	if err := s.receive(context.Background(), &streamMessage{header: mh, payload: p}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
}

// responsePayload marshals a Response message as it would appear on the wire.
func responsePayload(t *testing.T, code codes.Code, msg string, payload []byte) []byte {
	t.Helper()
	p, err := proto.Marshal(&Response{Status: status.New(code, msg).Proto(), Payload: payload})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return p
}

// requestPayload marshals a Request message as it would appear on the wire.
func requestPayload(t *testing.T, service, method string, payload []byte) []byte {
	t.Helper()
	p, err := proto.Marshal(&Request{Service: service, Method: method, Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return p
}

// unmarshalResponse decodes a response frame's payload.
func unmarshalResponse(t *testing.T, f frame) *Response {
	t.Helper()
	resp := &Response{}
	if err := proto.Unmarshal(f.payload, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

// assertStatusCode fails unless the frame carries a Response with the given
// status code whose message contains wantMsg.
func assertStatusFrame(t *testing.T, f frame, sid uint32, code codes.Code, wantMsg string) {
	t.Helper()
	if f.header.Type != messageTypeResponse {
		t.Fatalf("expected response frame, got %s", f.header.Type)
	}
	if f.header.StreamID != sid {
		t.Fatalf("expected sid %d, got %d", sid, f.header.StreamID)
	}
	resp := unmarshalResponse(t, f)
	if resp.Status.GetCode() != int32(code) {
		t.Fatalf("expected status %v, got %v (%q)", code, resp.Status.GetCode(), resp.Status.GetMessage())
	}
	if !strings.Contains(resp.Status.GetMessage(), wantMsg) {
		t.Fatalf("expected status message containing %q, got %q", wantMsg, resp.Status.GetMessage())
	}
}
