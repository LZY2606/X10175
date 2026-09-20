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
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const contractService = "contractService"

var errScriptedTransport = errors.New("scripted transport error")

type scriptedFrame struct {
	streamID uint32
	typeName messageType
	flags    uint8
	payload  []byte
}

type contractEvent struct {
	kind   string
	id     uint32
	state  connState
	header messageHeader
}

type contractHooks struct {
	eventCh chan contractEvent
	pending []contractEvent
}

func newContractHooks() *contractHooks {
	return &contractHooks{eventCh: make(chan contractEvent, 4096)}
}

func (h *contractHooks) add(event contractEvent) {
	select {
	case h.eventCh <- event:
	default:
	}
}

func (h *contractHooks) streamDelivered(id uint32, header messageHeader) {
	h.add(contractEvent{kind: "client-delivered", id: id, header: header})
}

func (h *contractHooks) streamInactive(id uint32, header messageHeader) {
	h.add(contractEvent{kind: "client-inactive", id: id, header: header})
}

func (h *contractHooks) streamHalfClosed(id uint32) {
	h.add(contractEvent{kind: "server-half-closed", id: id})
}

func (h *contractHooks) streamClosing(id uint32) {
	h.add(contractEvent{kind: "server-stream-closing", id: id})
}

func (h *contractHooks) streamDataRejected(id uint32) {
	h.add(contractEvent{kind: "server-data-rejected", id: id})
}

func (h *contractHooks) streamClosed(id uint32) {
	h.add(contractEvent{kind: "server-stream-closed", id: id})
}

func (h *contractHooks) connStateChanged(state connState) {
	h.add(contractEvent{kind: "conn-state", state: state})
}

func (h *contractHooks) shutdownLoop() {
	h.add(contractEvent{kind: "shutdown-loop"})
}

func (h *contractHooks) nextEvent(ctx context.Context, match func(contractEvent) bool) (contractEvent, bool) {
	for {
		for i := range h.pending {
			if match(h.pending[i]) {
				event := h.pending[i]
				h.pending = append(h.pending[:i], h.pending[i+1:]...)
				return event, true
			}
		}
		select {
		case event := <-h.eventCh:
			if match(event) {
				return event, true
			}
			h.pending = append(h.pending, event)
		case <-ctx.Done():
			return contractEvent{}, false
		}
	}
}

type contractHarness struct {
	t         *testing.T
	clientNet *scriptedConn
	serverNet *scriptedConn
	server    *Server
	conn      *serverConn
	client    *Client
	hooks     *contractHooks
}

func newContractHarness(t *testing.T) *contractHarness {
	t.Helper()
	clientNet, serverNet := newScriptedConnPair()
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	hooks := newContractHooks()
	server.testHooks = hooks
	conn, err := server.newConn(serverNet, nil)
	if err != nil {
		t.Fatal(err)
	}
	go conn.run(context.Background())
	client := NewClient(clientNet)
	client.testHooks = hooks

	h := &contractHarness{
		t:         t,
		clientNet: clientNet,
		serverNet: serverNet,
		server:    server,
		conn:      conn,
		client:    client,
		hooks:     hooks,
	}
	t.Cleanup(func() {
		client.Close()
		waitDone(t, conn.runDone, "server connection shutdown")
		server.Close()
	})
	return h
}

func (h *contractHarness) testContext() (context.Context, context.CancelFunc) {
	if deadline, ok := h.t.Deadline(); ok {
		return context.WithDeadline(context.Background(), deadline.Add(-2*time.Second))
	}
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (h *contractHarness) registerStandardService() {
	h.server.RegisterService(contractService, &ServiceDesc{
		Methods: map[string]Method{
			"Unary": h.unaryHandler,
		},
		Streams: map[string]Stream{
			"ClientStream": {
				Handler:         h.clientStreamHandler,
				StreamingClient: true,
			},
			"ServerStream": {
				Handler:         h.serverStreamHandler,
				StreamingServer: true,
			},
			"Bidi": {
				Handler:         h.bidiHandler,
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})
}

func (h *contractHarness) unaryHandler(_ context.Context, unmarshal func(any) error) (any, error) {
	var msg internal.EchoPayload
	if err := unmarshal(&msg); err != nil {
		return nil, err
	}
	msg.Seq++
	return &msg, nil
}

func (h *contractHarness) clientStreamHandler(_ context.Context, ss StreamServer) (any, error) {
	var count int64
	for {
		var msg internal.EchoPayload
		if err := ss.RecvMsg(&msg); err != nil {
			if err == io.EOF {
				return &internal.EchoPayload{Seq: count, Msg: "client-stream-final"}, nil
			}
			return nil, err
		}
		count++
	}
}

func (h *contractHarness) serverStreamHandler(_ context.Context, ss StreamServer) (any, error) {
	for seq := int64(1); seq <= 2; seq++ {
		if err := ss.SendMsg(&internal.EchoPayload{Seq: seq, Msg: "server-stream"}); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (h *contractHarness) bidiHandler(ctx context.Context, ss StreamServer) (any, error) {
	for {
		var msg internal.EchoPayload
		if err := ss.RecvMsg(&msg); err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
		msg.Seq++
		if err := ss.SendMsg(&msg); err != nil {
			return nil, err
		}
	}
}

func (h *contractHarness) requestFrame(t *testing.T, id uint32, desc *StreamDesc, method string, req *internal.EchoPayload) scriptedFrame {
	t.Helper()
	var payload []byte
	if req != nil {
		payload = framePayload(t, req)
	}
	encoded, err := codec{}.Marshal(&Request{Service: contractService, Method: method, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	flags := uint8(0)
	if desc != nil && desc.StreamingClient {
		flags = flagRemoteOpen
	} else {
		flags = flagRemoteClosed
	}
	return scriptedFrame{streamID: id, typeName: messageTypeRequest, flags: flags, payload: encoded}
}

func (h *contractHarness) dataFrame(t *testing.T, id uint32, msg *internal.EchoPayload, flags uint8) scriptedFrame {
	t.Helper()
	var payload []byte
	if msg != nil {
		payload = framePayload(t, msg)
	}
	return scriptedFrame{streamID: id, typeName: messageTypeData, flags: flags, payload: payload}
}

func (h *contractHarness) decodeResponse(t *testing.T, frame scriptedFrame) Response {
	t.Helper()
	var resp Response
	if err := (codec{}).Unmarshal(frame.payload, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func (h *contractHarness) waitEvent(t *testing.T, ctx context.Context, kind string, id uint32) contractEvent {
	t.Helper()
	event, ok := h.hooks.nextEvent(ctx, func(event contractEvent) bool {
		return event.kind == kind && (id == 0 || event.id == id)
	})
	if !ok {
		t.Fatalf("timed out waiting for event %q for stream %d", kind, id)
	}
	return event
}

func (h *contractHarness) waitTypedEvent(t *testing.T, ctx context.Context, kind string, id uint32, typ messageType) contractEvent {
	t.Helper()
	event, ok := h.hooks.nextEvent(ctx, func(event contractEvent) bool {
		return event.kind == kind && event.id == id && event.header.Type == typ
	})
	if !ok {
		t.Fatalf("timed out waiting for typed event %q/%s for stream %d", kind, typ, id)
	}
	return event
}

func (h *contractHarness) waitFrame(t *testing.T, ctx context.Context, conn *scriptedConn, id uint32, typ messageType) scriptedFrame {
	t.Helper()
	stop := context.AfterFunc(ctx, func() {
		conn.mu.Lock()
		conn.ready.Broadcast()
		conn.mu.Unlock()
	})
	defer stop()

	conn.mu.Lock()
	defer conn.mu.Unlock()
	for {
		for i := range conn.outbox {
			if conn.outbox[i].streamID == id && conn.outbox[i].typeName == typ {
				frame := conn.outbox[i]
				conn.outbox = append(conn.outbox[:i], conn.outbox[i+1:]...)
				return frame
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for frame type %s on stream %d: %v", typ, id, ctx.Err())
		}
		conn.ready.Wait()
	}
}

func waitDone(t testing.TB, ch <-chan struct{}, name string) {
	t.Helper()
	waitDoneContext(t, context.Background(), ch, name)
}

func waitDoneContext(t testing.TB, parent context.Context, ch <-chan struct{}, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", name, ctx.Err())
	}
}

type scriptedConn struct {
	peer *scriptedConn

	mu        sync.Mutex
	cond      *sync.Cond
	inbox     []byte
	readErr   error
	writeErr  error
	closed    bool
	outbox    []scriptedFrame
	writeBuf  []byte
	paused    bool
	gate      chan struct{}
	writeWait chan struct{}
	pauseID   uint64
	waitID    uint64
	ready     *sync.Cond
}

func newScriptedConnPair() (*scriptedConn, *scriptedConn) {
	clientConn := &scriptedConn{}
	serverConn := &scriptedConn{}
	clientConn.ready = sync.NewCond(&clientConn.mu)
	serverConn.ready = sync.NewCond(&serverConn.mu)
	clientConn.peer = serverConn
	serverConn.peer = clientConn
	clientConn.cond = sync.NewCond(&clientConn.mu)
	serverConn.cond = sync.NewCond(&serverConn.mu)
	return clientConn, serverConn
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	for len(c.inbox) == 0 && c.readErr == nil && !c.closed {
		c.cond.Wait()
	}
	if len(c.inbox) > 0 {
		n := copy(p, c.inbox)
		c.inbox = c.inbox[n:]
		c.mu.Unlock()
		return n, nil
	}
	err := c.readErr
	if err == nil {
		err = io.EOF
	}
	c.mu.Unlock()
	return 0, err
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	err := c.writeErr
	c.writeErr = nil
	if err == nil && c.closed {
		err = net.ErrClosed
	}
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}

	n := len(p)
	c.mu.Lock()
	c.writeBuf = append(c.writeBuf, p...)
	var frame *scriptedFrame
	if len(c.writeBuf) >= messageHeaderLength {
		length := int(binary.BigEndian.Uint32(c.writeBuf[:4]))
		if len(c.writeBuf) >= messageHeaderLength+length {
			decoded := decodeContractFrame(c.writeBuf[:messageHeaderLength+length])
			frame = &decoded
			c.writeBuf = c.writeBuf[messageHeaderLength+length:]
		}
	}
	if frame != nil {
		c.outbox = append(c.outbox, *frame)
		if c.paused {
			c.pauseID++
			pauseID := c.pauseID
			gate := c.gate
			c.waitID = pauseID
			c.ready.Broadcast()
			c.mu.Unlock()
			<-gate
			c.mu.Lock()
			if c.pauseID == pauseID {
				c.waitID = 0
			}
		}
		delivered := *frame
		c.peer.deliverFrame(delivered)
	}
	c.mu.Unlock()
	c.ready.Broadcast()
	return n, nil
}

func (c *scriptedConn) deliverFrame(frame scriptedFrame) {
	c.mu.Lock()
	if !c.closed {
		c.inbox = append(c.inbox, encodeContractFrame(frame)...)
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *scriptedConn) queueFrame(frame scriptedFrame) {
	c.deliverFrame(frame)
}

func (c *scriptedConn) pauseWrites() {
	c.mu.Lock()
	c.gate = make(chan struct{}, 1)
	c.paused = true
	c.writeWait = nil
	c.waitID = 0
	c.mu.Unlock()
}

func (c *scriptedConn) allowOneWrite() {
	c.mu.Lock()
	gate := c.gate
	c.mu.Unlock()
	if gate != nil {
		select {
		case gate <- struct{}{}:
		default:
		}
	}
}

func (c *scriptedConn) resumeWrites() {
	c.mu.Lock()
	gate := c.gate
	c.paused = false
	c.gate = nil
	wait := c.writeWait
	c.writeWait = nil
	c.mu.Unlock()
	if gate != nil {
		select {
		case gate <- struct{}{}:
		default:
		}
	}
	if wait != nil {
		<-wait
	}
}

func (c *scriptedConn) waitForPausedWrite(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.ready.Broadcast()
		c.mu.Unlock()
	})
	defer stop()

	c.mu.Lock()
	defer c.mu.Unlock()
	for c.paused && c.waitID == 0 {
		if ctx.Err() != nil {
			waitEvent(ctx, make(chan struct{}), "paused write")
		}
		c.ready.Wait()
	}
}

func (c *scriptedConn) failNextWrite(err error) {
	c.mu.Lock()
	c.writeErr = err
	c.mu.Unlock()
}

func (c *scriptedConn) failRead(err error) {
	c.mu.Lock()
	c.readErr = err
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()

	c.peer.mu.Lock()
	if c.peer.readErr == nil {
		c.peer.readErr = io.EOF
	}
	c.peer.cond.Broadcast()
	c.peer.mu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type scriptedAddr struct{}

func (scriptedAddr) Network() string { return "scripted" }
func (scriptedAddr) String() string  { return "scripted" }

func encodeContractFrame(frame scriptedFrame) []byte {
	var header [messageHeaderLength]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(frame.payload)))
	binary.BigEndian.PutUint32(header[4:8], frame.streamID)
	header[8] = byte(frame.typeName)
	header[9] = frame.flags
	return append(header[:], frame.payload...)
}

func decodeContractFrame(p []byte) scriptedFrame {
	payload := append([]byte(nil), p[messageHeaderLength:]...)
	return scriptedFrame{
		streamID: binary.BigEndian.Uint32(p[4:8]),
		typeName: messageType(p[8]),
		flags:    p[9],
		payload:  payload,
	}
}

func waitEvent(ctx context.Context, ch <-chan struct{}, name string) {
	select {
	case <-ch:
	case <-ctx.Done():
		panic("timed out waiting for " + name + ": " + ctx.Err().Error())
	}
}

func waitResult[T any](ctx context.Context, ch <-chan T, name string) T {
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		panic(fmt.Sprintf("timed out waiting for %s", name))
	}
}

func framePayload(t testing.TB, m any) []byte {
	t.Helper()
	p, err := codec{}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func responsePayload(t testing.TB, code codes.Code, message string) []byte {
	t.Helper()
	resp := &Response{Status: status.New(code, message).Proto()}
	p, err := codec{}.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func assertStatusCode(t testing.TB, err error, code codes.Code) {
	t.Helper()
	if code == codes.OK {
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected status code %s, got non-status error %v", code, err)
	}
	if st.Code() != code {
		t.Fatalf("expected status code %s, got %s: %v", code, st.Code(), err)
	}
}

func assertBytesFrame(t testing.TB, got scriptedFrame, want scriptedFrame) {
	t.Helper()
	if got.streamID != want.streamID || got.typeName != want.typeName || got.flags != want.flags || !bytes.Equal(got.payload, want.payload) {
		t.Fatalf("unexpected frame: got id=%d type=%s flags=%#x payload=%v; want id=%d type=%s flags=%#x payload=%v",
			got.streamID, got.typeName, got.flags, got.payload,
			want.streamID, want.typeName, want.flags, want.payload)
	}
}

func assertMapContains(t testing.TB, ids map[uint32]*streamHandler, id uint32) {
	t.Helper()
	if _, ok := ids[id]; !ok {
		t.Fatalf("stream %d not in server map: %v", id, ids)
	}
}

func assertMapMissing(t testing.TB, ids map[uint32]*streamHandler, id uint32) {
	t.Helper()
	if _, ok := ids[id]; ok {
		t.Fatalf("stream %d unexpectedly remains in server map: %v", id, ids)
	}
}

func assertClientStreamMissing(t testing.TB, c *Client, id streamID) {
	t.Helper()
	if _, ok := c.testStreamIDs()[id]; ok {
		t.Fatalf("client stream %d was not removed", id)
	}
}

func assertClientStreamPresent(t testing.TB, c *Client, id streamID) {
	t.Helper()
	if _, ok := c.testStreamIDs()[id]; !ok {
		t.Fatalf("client stream %d is missing", id)
	}
}

func mustEchoPayload(t testing.TB, seq int64) *internal.EchoPayload {
	t.Helper()
	return &internal.EchoPayload{Seq: seq, Msg: fmt.Sprintf("m-%d", seq)}
}
