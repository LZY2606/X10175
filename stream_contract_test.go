package ttrpc

import (
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

var errContractTransport = errors.New("deterministic contract transport error")

const contractService = "contract.Service"

type scriptedAddr struct{}

func (scriptedAddr) Network() string { return "scripted" }
func (scriptedAddr) String() string  { return "scripted" }

type wireFrame struct {
	header  messageHeader
	payload []byte
}

type writeEvent struct {
	frame wireFrame
	done  chan error
}

type scriptedConn struct {
	peer    *scriptedConn
	name    string
	inbound chan wireFrame

	readMu  sync.Mutex
	pending []byte
	readErr error

	writeGate   *frameGate
	writeEvents chan writeEvent

	closeOnce sync.Once
	closed    chan struct{}
}

func newScriptedConnPair() (*scriptedConn, *scriptedConn) {
	clientToServer := make(chan wireFrame, 128)
	serverToClient := make(chan wireFrame, 128)
	clientEvents := make(chan writeEvent, 128)
	serverEvents := make(chan writeEvent, 128)
	client := &scriptedConn{
		name:        "client",
		inbound:     serverToClient,
		writeGate:   newFrameGate(),
		writeEvents: clientEvents,
		closed:      make(chan struct{}),
	}
	server := &scriptedConn{
		name:        "server",
		inbound:     clientToServer,
		writeGate:   newFrameGate(),
		writeEvents: serverEvents,
		closed:      make(chan struct{}),
	}
	client.peer = server
	server.peer = client
	return client, server
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for len(c.pending) == 0 && c.readErr == nil {
		select {
		case frame := <-c.inbound:
			c.pending = append(c.pending, encodeWireFrame(frame)...)
		case <-c.closed:
			c.readErr = io.EOF
		case <-c.peer.closed:
			c.readErr = io.EOF
		}
	}
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	return 0, c.readErr
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	frame, err := decodeWireFrame(p)
	if err != nil {
		return 0, err
	}
	select {
	case c.peer.inbound <- frame:
	case <-c.closed:
		return 0, io.ErrClosedPipe
	case <-c.peer.closed:
		return 0, io.ErrClosedPipe
	}
	event := writeEvent{frame: frame, done: make(chan error, 1)}
	c.writeEvents <- event
	if err := c.writeGate.enter(event, 10*time.Second); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *scriptedConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func (c *scriptedConn) deliver(frame wireFrame) {
	c.inbound <- frame
}

func (c *scriptedConn) failRead(err error) {
	c.readMu.Lock()
	c.readErr = err
	c.readMu.Unlock()
}

func (c *scriptedConn) waitWrite() writeEvent {
	return <-c.writeEvents
}

type frameGate struct {
	mu      sync.Mutex
	hold    bool
	held    chan writeEvent
	release chan struct{}
}

func newFrameGate() *frameGate {
	return &frameGate{}
}

func (g *frameGate) holdNext() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hold = true
	g.held = make(chan writeEvent, 1)
	g.release = make(chan struct{})
}

func (g *frameGate) release() {
	g.mu.Lock()
	if g.hold {
		g.hold = false
		close(g.release)
	}
	g.mu.Unlock()
}

func (g *frameGate) heldFrame() writeEvent {
	return <-g.held
}

func (g *frameGate) enter(event writeEvent, timeout time.Duration) error {
	g.mu.Lock()
	if !g.hold {
		g.mu.Unlock()
		return nil
	}
	release := g.release
	g.hold = false
	g.mu.Unlock()
	g.held <- event
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-release:
		return nil
	case <-timer.C:
		return errors.New("timed out waiting for gated write")
	}
}

func encodeWireFrame(frame wireFrame) []byte {
	buf := make([]byte, messageHeaderLength+len(frame.payload))
	binary.BigEndian.PutUint32(buf[0:4], frame.header.Length)
	binary.BigEndian.PutUint32(buf[4:8], frame.header.StreamID)
	buf[8] = byte(frame.header.Type)
	buf[9] = frame.header.Flags
	copy(buf[messageHeaderLength:], frame.payload)
	return buf
}

func decodeWireFrame(p []byte) (wireFrame, error) {
	if len(p) < messageHeaderLength {
		return wireFrame{}, io.ErrUnexpectedEOF
	}
	mh := messageHeader{
		Length:   binary.BigEndian.Uint32(p[0:4]),
		StreamID: binary.BigEndian.Uint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}
	payload := append([]byte(nil), p[messageHeaderLength:]...)
	return wireFrame{header: mh, payload: payload}, nil
}

type streamEvent struct {
	kind string
	id   uint32
}

type contractHarness struct {
	t          *testing.T
	server     *Server
	conn       *serverConn
	client     *Client
	clientConn *scriptedConn
	serverConn *scriptedConn
	events     chan streamEvent
	done       chan struct{}

	started   chan struct{}
	recv      chan *internal.EchoPayload
	send      chan *internal.EchoPayload
	returnOnce sync.Once
	returnNow chan struct{}
	method    string
	streams   *sync.Map
}

type manualShutdownTicker struct {
	tick chan time.Time
}

func newManualShutdownTicker() *manualShutdownTicker {
	return &manualShutdownTicker{tick: make(chan time.Time, 16)}
}

func (t *manualShutdownTicker) C() <-chan time.Time { return t.tick }
func (t *manualShutdownTicker) Stop()               {}
func (t *manualShutdownTicker) fire()               { t.tick <- time.Time{} }

func newContractHarness(t *testing.T, method string, withClient bool) *contractHarness {
	t.Helper()

	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	server.shutdownTicker = newManualShutdownTicker()

	clientConn, serverConn := newScriptedConnPair()
	h := &contractHarness{
		t:          t,
		server:     server,
		clientConn: clientConn,
		serverConn: serverConn,
		events:     make(chan streamEvent, 64),
		done:       make(chan struct{}),
		started:    make(chan struct{}),
		recv:       make(chan *internal.EchoPayload, 128),
		send:       make(chan *internal.EchoPayload, 128),
		returnNow:  make(chan struct{}),
		method:     method,
	}

	h.server.RegisterService(contractService, &ServiceDesc{Streams: map[string]Stream{
		method: {
			Handler:         h.handler,
			StreamingClient: true,
			StreamingServer: true,
		},
	}})

	h.conn = &serverConn{
		server:   server,
		conn:     serverConn,
		shutdown: make(chan struct{}),
		onStreamAdd: func(id uint32, _ *streamHandler, streams *sync.Map) {
			h.streams = streams
			h.events <- streamEvent{kind: "add", id: id}
		},
		onStreamDelete: func(id uint32, streams *sync.Map) {
			h.events <- streamEvent{kind: "delete", id: id}
		},
		onConnDone: func() {
			close(h.done)
		},
	}
	h.conn.setState(connStateIdle)
	server.connections[h.conn] = struct{}{}

	if withClient {
		h.client = NewClient(clientConn)
		t.Cleanup(func() { h.client.Close() })
	}
	t.Cleanup(func() {
		h.conn.close()
		<-h.done
	})
	go h.conn.run(context.Background())
	return h
}

func (h *contractHarness) handler(ctx context.Context, ss StreamServer) (any, error) {
	close(h.started)
	for {
		var msg internal.EchoPayload
		if err := ss.RecvMsg(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		h.recv <- &msg
		select {
		case out := <-h.send:
			if err := ss.SendMsg(out); err != nil {
				return nil, err
			}
		default:
		}
	}
	<-h.returnNow
	return &internal.EchoPayload{Seq: 900, Msg: "final"}, nil
}

func (h *contractHarness) newStream(desc *StreamDesc, req any) ClientStream {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h.t.Cleanup(cancel)
	stream, err := h.client.NewStream(ctx, desc, contractService, h.method, req)
	if err != nil {
		h.t.Fatal(err)
	}
	return stream
}

func (h *contractHarness) waitEvent(want streamEvent) {
	h.t.Helper()
	select {
	case got := <-h.events:
		if got != want {
			h.t.Fatalf("event = %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		h.t.Fatalf("timed out waiting for event %+v", want)
	}
}

func (h *contractHarness) clientStreamCount() int {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	return len(h.client.streams)
}

func (h *contractHarness) clientHasStream(id streamID) bool {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	_, ok := h.client.streams[id]
	return ok
}

func (h *contractHarness) serverHasStream(id uint32) bool {
	var found bool
	h.conn.server.connRange(func(key any, _ any) bool {
		if key.(uint32) == id {
			found = true
			return false
		}
		return true
	})
	return found
}

func (h *contractHarness) waitClientWrite() writeEvent {
	h.t.Helper()
	select {
	case event := <-h.clientConn.writeEvents:
		return event
	case <-time.After(5 * time.Second):
		h.t.Fatal("timed out waiting for client frame")
		return writeEvent{}
	}
}

func (h *contractHarness) waitServerWrite() writeEvent {
	h.t.Helper()
	select {
	case event := <-h.serverConn.writeEvents:
		return event
	case <-time.After(5 * time.Second):
		h.t.Fatal("timed out waiting for server frame")
		return writeEvent{}
	}
}

func (h *contractHarness) waitHandlerStart() {
	h.t.Helper()
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		h.t.Fatal("timed out waiting for handler to start")
	}
}

func (h *contractHarness) allowHandlerReturn() {
	close(h.returnNow)
}

func marshalRequest(t *testing.T, method string) []byte {
	t.Helper()
	p, err := codec{}.Marshal(&Request{Service: contractService, Method: method})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func marshalEcho(t *testing.T, msg *internal.EchoPayload) []byte {
	t.Helper()
	p, err := protoMarshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func unmarshalResponse(t *testing.T, p []byte) *Response {
	t.Helper()
	var resp Response
	if err := protoUnmarshal(p, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func unmarshalEcho(t *testing.T, p []byte) *internal.EchoPayload {
	t.Helper()
	var msg internal.EchoPayload
	if err := protoUnmarshal(p, &msg); err != nil {
		t.Fatal(err)
	}
	return &msg
}

func mustStatusCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want code %s", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v has no status code", err)
	}
	if st.Code() != want {
		t.Fatalf("status code = %s, want %s: %v", st.Code(), want, err)
	}
}
