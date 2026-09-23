package ttrpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

// This file implements a deterministic, replayable test driver for the
// streaming state machine. It uses scripted, in-memory net.Conn pairs
// instead of real sockets:
//
//   - every wire write is recorded in an ordered frame log and can be
//     paused at an exact write index and explicitly released;
//   - either side can be closed and a one-shot write or read error can be
//     injected;
//   - the client's stream table, the server connection state and the
//     server connection set can be read at any time;
//   - goroutine completion is observed through channels.
//
// All ordering is derived from these explicit signals; the driver never
// relies on time.Sleep or on scheduling luck. Wall-clock deadlines are
// only used to fail fast when an expected event never happens.

const contractService = "contractsvc"

// contractWaitDeadline is the upper bound for a deterministic wait to
// materialize. It is only reached when a test breaks or the code deadlocks.
const contractWaitDeadline = 30 * time.Second

func contractWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(contractWaitDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func contractAwaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(contractWaitDeadline):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func contractAwaitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(contractWaitDeadline):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func contractAwaitInt64(t *testing.T, ch <-chan int64, what string) int64 {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(contractWaitDeadline):
		t.Fatalf("timed out waiting for %s", what)
		return 0
	}
}

func contractGo(fn func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return ch
}

func contractAwait(t *testing.T, ch <-chan error, what string) error {
	return contractAwaitErr(t, ch, what)
}

func contractNoResultYet(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s completed unexpectedly with %v", what, err)
	default:
	}
}

// contractFrameEvent is one recorded wire frame. status is decoded for
// response frames so tests can assert the exact status code on the wire.
type contractFrameEvent struct {
	dir    string // "C2S" client->server or "S2C" server->client
	hdr    messageHeader
	status *status.Status
}

type contractFrameLog struct {
	mu     sync.Mutex
	events []contractFrameEvent
}

func (l *contractFrameLog) append(dir string, p []byte) {
	hdr, ok := parseContractHeader(p)
	if !ok {
		return
	}
	ev := contractFrameEvent{dir: dir, hdr: hdr}
	if hdr.Type == messageTypeResponse && len(p) > messageHeaderLength {
		var resp Response
		if err := proto.Unmarshal(p[messageHeaderLength:], &resp); err == nil {
			ev.status = status.FromProto(resp.GetStatus())
			if ev.status == nil {
				ev.status = status.New(codes.OK, "")
			}
		}
	}
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *contractFrameLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

func (l *contractFrameLog) snapshot() []contractFrameEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]contractFrameEvent, len(l.events))
	copy(out, l.events)
	return out
}

func (l *contractFrameLog) waitCount(t *testing.T, n int) []contractFrameEvent {
	t.Helper()
	contractWait(t, fmt.Sprintf("%d recorded frames", n), func() bool {
		return l.count() >= n
	})
	return l.snapshot()
}

func parseContractHeader(p []byte) (messageHeader, bool) {
	if len(p) < messageHeaderLength {
		return messageHeader{}, false
	}
	return messageHeader{
		Length:   binary.BigEndian.Uint32(p[:4]),
		StreamID: binary.BigEndian.Uint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}, true
}

func contractHeaderBytes(hdr messageHeader) []byte {
	b := make([]byte, messageHeaderLength)
	binary.BigEndian.PutUint32(b[:4], hdr.Length)
	binary.BigEndian.PutUint32(b[4:8], hdr.StreamID)
	b[8] = byte(hdr.Type)
	b[9] = hdr.Flags
	return b
}

func contractRequestFrame(t *testing.T, sid uint32, flags uint8, service, method string, payload []byte) (messageHeader, []byte) {
	t.Helper()
	b, err := proto.Marshal(&Request{Service: service, Method: method, Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return messageHeader{Length: uint32(len(b)), StreamID: sid, Type: messageTypeRequest, Flags: flags}, b
}

func contractResponseFrame(t *testing.T, sid uint32, st *status.Status, payload []byte) (messageHeader, []byte) {
	t.Helper()
	resp := &Response{Payload: payload}
	if st != nil {
		resp.Status = st.Proto()
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return messageHeader{Length: uint32(len(b)), StreamID: sid, Type: messageTypeResponse, Flags: 0}, b
}

func contractDataFrame(sid uint32, flags uint8, payload []byte) (messageHeader, []byte) {
	return messageHeader{Length: uint32(len(payload)), StreamID: sid, Type: messageTypeData, Flags: flags}, payload
}

func contractAssertFrame(t *testing.T, ev contractFrameEvent, dir string, mt messageType, flags uint8, sid uint32) {
	t.Helper()
	if ev.dir != dir || ev.hdr.Type != mt || ev.hdr.Flags != flags || ev.hdr.StreamID != sid {
		t.Fatalf("unexpected frame: got %s %s(id=%d,flags=%#x,len=%d), want %s %s(id=%d,flags=%#x)",
			ev.dir, ev.hdr.Type, ev.hdr.StreamID, ev.hdr.Flags, ev.hdr.Length,
			dir, mt, sid, flags)
	}
}

func contractAssertStatus(t *testing.T, ev contractFrameEvent, code codes.Code) {
	t.Helper()
	if ev.status == nil || ev.status.Code() != code {
		t.Fatalf("unexpected wire status for stream %d: got %v, want %s",
			ev.hdr.StreamID, ev.status, code)
	}
}

// contractReadItem is one queued read for a contractConn: either a raw
// frame or a terminal read error.
type contractReadItem struct {
	frame []byte
	err   error
}

// contractConn is an in-memory, fully scriptable net.Conn. Writes are
// delivered whole to the peer's read queue (ttrpc always writes one
// complete frame per send call). The next write, or the write at an
// exact index, can be parked until explicitly released, and a one-shot
// write error can be injected. Closing the connection unblocks pending
// readers and writers on both endpoints, exactly like a real broken
// socket.
type contractConn struct {
	name string
	dir  string
	log  *contractFrameLog

	peer *contractConn

	readCh   chan contractReadItem
	closedCh chan struct{}
	closeMu  sync.Mutex
	closed   bool

	writeMu    sync.Mutex
	writeCount int
	holdAt     int // index of the write to park; -1 disables parking
	parked     chan struct{}
	releaseCh  chan struct{}
	writeErr   error
}

func newContractConnPair(log *contractFrameLog) (client, server *contractConn) {
	client = &contractConn{
		name:     "client",
		dir:      "C2S",
		log:      log,
		readCh:   make(chan contractReadItem, 4096),
		closedCh: make(chan struct{}),
		holdAt:   -1,
	}
	server = &contractConn{
		name:     "server",
		dir:      "S2C",
		log:      log,
		readCh:   make(chan contractReadItem, 4096),
		closedCh: make(chan struct{}),
		holdAt:   -1,
	}
	client.peer = server
	server.peer = client
	return client, server
}

func (c *contractConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	index := c.writeCount
	c.writeCount++
	c.log.append(c.dir, p)
	if c.holdAt >= 0 && index == c.holdAt {
		c.holdAt = -1
		parked, release := c.parked, c.releaseCh
		c.writeMu.Unlock()
		close(parked)
		select {
		case <-release:
		case <-c.closedCh:
			return 0, io.ErrClosedPipe
		}
		c.writeMu.Lock()
	}
	err := c.writeErr
	c.writeErr = nil
	c.writeMu.Unlock()

	if err != nil {
		return 0, err
	}

	frame := make([]byte, len(p))
	copy(frame, p)
	select {
	case c.peer.readCh <- contractReadItem{frame: frame}:
		return len(p), nil
	case <-c.closedCh:
		return 0, io.ErrClosedPipe
	}
}

func (c *contractConn) Read(p []byte) (int, error) {
	item := <-c.readCh
	if item.err != nil {
		return 0, item.err
	}
	if len(item.frame) > len(p) {
		return 0, fmt.Errorf("contractConn %s: frame (%d bytes) exceeds read buffer (%d)",
			c.name, len(item.frame), len(p))
	}
	return copy(p, item.frame), nil
}

func (c *contractConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closedCh)
	// Unblock a pending Read on each endpoint, mirroring a real socket
	// close. The queues are generously buffered so these sends never block.
	select {
	case c.readCh <- contractReadItem{err: io.EOF}:
	default:
	}
	select {
	case c.peer.readCh <- contractReadItem{err: io.EOF}:
	default:
	}
	c.closeMu.Unlock()

	c.writeMu.Lock()
	select {
	case <-c.releaseCh:
	default:
		if c.releaseCh != nil {
			close(c.releaseCh)
		}
	}
	c.writeMu.Unlock()
	return nil
}

// holdNextWrite parks the next write on this connection.
func (c *contractConn) holdNextWrite() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.holdAt = c.writeCount
	c.parked = make(chan struct{})
	c.releaseCh = make(chan struct{})
}

// holdWriteAt parks the write at the given per-connection write index.
func (c *contractConn) holdWriteAt(index int) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.holdAt = index
	c.parked = make(chan struct{})
	c.releaseCh = make(chan struct{})
}

func (c *contractConn) waitParked(t *testing.T) {
	t.Helper()
	contractAwaitSignal(t, c.parked, c.name+" write parked")
}

func (c *contractConn) releaseWrite() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.releaseCh:
	default:
		if c.releaseCh != nil {
			close(c.releaseCh)
		}
	}
}

// injectWriteError makes the next write return err without delivering a
// frame to the peer. The error is consumed by that single write.
func (c *contractConn) injectWriteError(err error) {
	c.writeMu.Lock()
	c.writeErr = err
	c.writeMu.Unlock()
}

func (c *contractConn) injectReadError(err error) {
	c.readCh <- contractReadItem{err: err}
}

type contractAddr string

func (a contractAddr) Network() string { return "contract" }
func (a contractAddr) String() string  { return string(a) }

func (c *contractConn) LocalAddr() net.Addr              { return contractAddr(c.name) }
func (c *contractConn) RemoteAddr() net.Addr             { return contractAddr(c.peer.name) }
func (c *contractConn) SetDeadline(time.Time) error      { return nil }
func (c *contractConn) SetReadDeadline(time.Time) error  { return nil }
func (c *contractConn) SetWriteDeadline(time.Time) error { return nil }

// contractListener hands a single pre-connected net.Conn to Serve.
type contractListener struct {
	connCh chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func newContractListener() *contractListener {
	return &contractListener{
		connCh: make(chan net.Conn, 1),
		done:   make(chan struct{}),
	}
}

func (l *contractListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connCh:
		return conn, nil
	case <-l.done:
		return nil, ErrServerClosed
	}
}

func (l *contractListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *contractListener) Addr() net.Addr { return contractAddr("listener") }

// contractHarness wires a real Server and a real Client over a scripted
// connection pair, and registers a set of services whose handlers expose
// their progress through channels.
type contractHarness struct {
	t *testing.T

	log *contractFrameLog

	server *Server
	client *Client
	sconn  *serverConn

	clientConn *contractConn
	serverConn *contractConn

	serveDone chan error

	// handler coordination
	echoStreamStarted chan struct{}
	echoStreamDone    chan struct{}
	collectCount      chan int64
	collectRelease    chan struct{}
	s2cStart          chan struct{}
	hangStarted       chan struct{}
	hangErr           chan error
}

func newContractHarness(t *testing.T) *contractHarness {
	t.Helper()

	h := &contractHarness{
		t:                 t,
		log:               &contractFrameLog{},
		serveDone:         make(chan error, 1),
		echoStreamStarted: make(chan struct{}),
		echoStreamDone:    make(chan struct{}),
		collectCount:      make(chan int64, 1),
		collectRelease:    make(chan struct{}),
		s2cStart:          make(chan struct{}),
		hangStarted:       make(chan struct{}),
		hangErr:           make(chan error, 1),
	}

	srv, err := NewServer()
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h.server = srv
	h.registerContractServices(srv)

	// Install the connection observation hook for this harness only.
	sconnCh := make(chan *serverConn, 1)
	prevHook := newConnTestHook
	newConnTestHook = func(sc *serverConn) { sconnCh <- sc }
	t.Cleanup(func() { newConnTestHook = prevHook })

	listener := newContractListener()
	go func() { h.serveDone <- srv.Serve(context.Background(), listener) }()

	h.clientConn, h.serverConn = newContractConnPair(h.log)
	listener.connCh <- h.serverConn

	select {
	case h.sconn = <-sconnCh:
	case <-time.After(contractWaitDeadline):
		t.Fatal("timed out waiting for server connection")
	}

	h.client = NewClient(h.clientConn)

	t.Cleanup(func() {
		h.client.Close()
		h.server.Close()
		select {
		case <-h.serveDone:
		case <-time.After(contractWaitDeadline):
			t.Errorf("server did not stop")
		}
	})
	return h
}

func (h *contractHarness) registerContractServices(srv *Server) {
	srv.RegisterService(contractService, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
			"Fail": func(_ context.Context, _ func(any) error) (any, error) {
				return nil, status.Error(codes.PermissionDenied, "contract: denied")
			},
		},
		Streams: map[string]Stream{
			// EchoStream echoes each message back with Seq+1 and
			// returns when the client half-closes.
			"EchoStream": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					close(h.echoStreamStarted)
					defer close(h.echoStreamDone)
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								return nil, nil
							}
							return nil, err
						}
						req.Seq++
						if err := ss.SendMsg(&req); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
			// Collect consumes client messages until half-close,
			// reports the count, waits for release, then returns the
			// count as the final payload.
			"Collect": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var n int64
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								break
							}
							return nil, err
						}
						n++
					}
					h.collectCount <- n
					<-h.collectRelease
					return &internal.EchoPayload{Seq: n}, nil
				},
				StreamingClient: true,
				StreamingServer: false,
			},
			// ServerStream waits for the start signal, sends two
			// messages, then returns a final payload.
			"ServerStream": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					<-h.s2cStart
					for i := int64(1); i <= 2; i++ {
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
							return nil, err
						}
					}
					return &internal.EchoPayload{Seq: 99}, nil
				},
				StreamingClient: false,
				StreamingServer: true,
			},
			// Hang never consumes and blocks until its context is
			// canceled, reporting the resulting error.
			"Hang": {
				Handler: func(ctx context.Context, _ StreamServer) (any, error) {
					close(h.hangStarted)
					<-ctx.Done()
					err := ctx.Err()
					h.hangErr <- err
					return nil, err
				},
				StreamingClient: true,
				StreamingServer: false,
			},
		},
	})
}

// clientStreamCount reports how many streams the client currently tracks.
func (h *contractHarness) clientStreamCount() int {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	return len(h.client.streams)
}

func (h *contractHarness) waitClientStreamCount(n int) {
	contractWait(h.t, fmt.Sprintf("client stream count %d", n), func() bool {
		return h.clientStreamCount() == n
	})
}

// clientStreamQueued reports how many messages are buffered in the
// client-side receive queue of the given stream.
func (h *contractHarness) clientStreamQueued(sid streamID) int {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	s := h.client.streams[sid]
	if s == nil {
		return -1
	}
	return len(s.recv)
}

func (h *contractHarness) connState() connState {
	st, _ := h.sconn.getState()
	return st
}

func (h *contractHarness) waitConnState(want connState) {
	contractWait(h.t, "connection state "+want.String(), func() bool {
		return h.connState() == want
	})
}

func (h *contractHarness) waitConnCount(n int) {
	contractWait(h.t, fmt.Sprintf("server connection count %d", n), func() bool {
		return h.server.countConnection() == n
	})
}

// disconnectClient hard-closes the client side of the transport without
// any protocol-level teardown.
func (h *contractHarness) disconnectClient() {
	h.clientConn.Close()
}

// injectC2S delivers a raw frame to the server as if the client had
// written it, bypassing the client state machine entirely.
func (h *contractHarness) injectC2S(hdr messageHeader, payload []byte) {
	frame := append(contractHeaderBytes(hdr), payload...)
	h.serverConn.readCh <- contractReadItem{frame: frame}
}

// injectS2C delivers a raw frame to the client as if the server had
// written it, bypassing the server state machine entirely.
func (h *contractHarness) injectS2C(hdr messageHeader, payload []byte) {
	frame := append(contractHeaderBytes(hdr), payload...)
	h.clientConn.readCh <- contractReadItem{frame: frame}
}
