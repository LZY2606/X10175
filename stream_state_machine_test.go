package ttrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const scriptServiceName = "scriptService"

var errDeterministicTransport = errors.New("deterministic transport error")

type scriptFrame struct {
	header  messageHeader
	payload []byte
}

type scriptAddr string

func (a scriptAddr) Network() string { return "script" }
func (a scriptAddr) String() string  { return string(a) }

type scriptConn struct {
	name      string
	peer      *scriptConn
	log       *scriptFrameLog
	frames    chan scriptFrame
	mu        sync.Mutex
	leftover  []byte
	paused    bool
	started   chan struct{}
	release   chan struct{}
	nextErr   error
	closed    bool
	closeCh   chan struct{}
	closeOnce sync.Once
	localAddr scriptAddr
	peerAddr  scriptAddr
}

func newScriptPair(name string) (*scriptConn, *scriptConn, *scriptFrameLog) {
	frameLog := newScriptFrameLog()
	client := &scriptConn{
		name:      name + "-client",
		frames:    make(chan scriptFrame),
		closeCh:   make(chan struct{}),
		localAddr: scriptAddr(name + "-client"),
		peerAddr:  scriptAddr(name + "-server"),
		log:       frameLog,
	}
	server := &scriptConn{
		name:      name + "-server",
		frames:    make(chan scriptFrame),
		closeCh:   make(chan struct{}),
		localAddr: scriptAddr(name + "-server"),
		peerAddr:  scriptAddr(name + "-client"),
		log:       frameLog,
	}
	client.peer = server
	server.peer = client
	return client, server, frameLog
}

func (c *scriptConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.leftover) > 0 {
		n := copy(p, c.leftover)
		c.leftover = c.leftover[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()

	select {
	case frame := <-c.frames:
		data := marshalScriptFrame(frame)
		n := copy(p, data)
		if n < len(data) {
			c.mu.Lock()
			c.leftover = append(c.leftover, data[n:]...)
			c.mu.Unlock()
		}
		return n, nil
	case <-c.closeCh:
		return 0, io.EOF
	}
}

func (c *scriptConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.paused {
		started := c.started
		release := c.release
		c.mu.Unlock()
		started <- struct{}{}
		select {
		case <-release:
		case <-c.closeCh:
			return 0, io.ErrClosedPipe
		}
		c.mu.Lock()
	}
	if c.nextErr != nil {
		err := c.nextErr
		c.nextErr = nil
		c.mu.Unlock()
		return 0, err
	}
	c.mu.Unlock()

	frames, err := parseScriptFrames(append([]byte(nil), p...))
	if err != nil {
		return 0, err
	}
	for _, frame := range frames {
		c.log.record(c.name, frame)
		c.peer.mu.Lock()
		peerClosed := c.peer.closed
		c.peer.mu.Unlock()
		if peerClosed {
			return 0, io.ErrClosedPipe
		}
		c.peer.frames <- frame
	}
	return len(p), nil
}

func (c *scriptConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.closeCh)
	})
	return nil
}

func (c *scriptConn) LocalAddr() net.Addr              { return c.localAddr }
func (c *scriptConn) RemoteAddr() net.Addr             { return c.peerAddr }
func (c *scriptConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptConn) SetWriteDeadline(time.Time) error { return nil }

func (c *scriptConn) pauseNextWrite() {
	c.mu.Lock()
	c.paused = true
	c.started = make(chan struct{})
	c.release = make(chan struct{})
	c.mu.Unlock()
}

func (c *scriptConn) failNextWrite(err error) {
	c.mu.Lock()
	c.paused = false
	c.nextErr = err
	c.mu.Unlock()
}

func (c *scriptConn) waitWriteStarted() { <-c.started }

func (c *scriptConn) resumeWrite() {
	c.mu.Lock()
	c.paused = false
	release := c.release
	c.mu.Unlock()
	close(release)
}

type loggedScriptFrame struct {
	endpoint string
	frame    scriptFrame
}

type scriptFrameLog struct {
	mu     sync.Mutex
	cond   *sync.Cond
	frames []loggedScriptFrame
}

func newScriptFrameLog() *scriptFrameLog {
	frameLog := &scriptFrameLog{}
	frameLog.cond = sync.NewCond(&frameLog.mu)
	return frameLog
}

func (l *scriptFrameLog) record(endpoint string, frame scriptFrame) {
	l.mu.Lock()
	l.frames = append(l.frames, loggedScriptFrame{endpoint: endpoint, frame: frame})
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *scriptFrameLog) snapshot() []loggedScriptFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]loggedScriptFrame(nil), l.frames...)
}

func (l *scriptFrameLog) waitCount(ctx context.Context, n int) []loggedScriptFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.frames) < n {
		checked := make(chan struct{})
		go func() {
			l.cond.Wait()
			close(checked)
		}()
		select {
		case <-checked:
		case <-ctx.Done():
			return append([]loggedScriptFrame(nil), l.frames...)
		}
	}
	return append([]loggedScriptFrame(nil), l.frames[:n]...)
}

func marshalScriptFrame(frame scriptFrame) []byte {
	var buf bytes.Buffer
	if err := writeMessageHeader(&buf, make([]byte, messageHeaderLength), frame.header); err != nil {
		panic(err)
	}
	buf.Write(frame.payload)
	return buf.Bytes()
}

func parseScriptFrames(data []byte) ([]scriptFrame, error) {
	var frames []scriptFrame
	for len(data) > 0 {
		if len(data) < messageHeaderLength {
			return nil, io.ErrShortBuffer
		}
		header, err := readMessageHeader(make([]byte, messageHeaderLength), bytes.NewReader(data[:messageHeaderLength]))
		if err != nil {
			return nil, err
		}
		data = data[messageHeaderLength:]
		end := int(header.Length)
		if len(data) < end {
			return nil, io.ErrShortBuffer
		}
		frames = append(frames, scriptFrame{header: header, payload: append([]byte(nil), data[:end]...)})
		data = data[end:]
	}
	return frames, nil
}

func encodeScriptRequest(payload proto.Message, flags uint8, sid uint32) scriptFrame {
	var request Request
	if payload != nil {
		request.Payload = mustProtoMarshal(payload)
	}
	return scriptFrame{
		header: messageHeader{
			Length:   uint32(len(mustProtoMarshal(&request))),
			StreamID: sid,
			Type:     messageTypeRequest,
			Flags:    flags,
		},
		payload: mustProtoMarshal(&request),
	}
}

func encodeScriptData(payload proto.Message, flags uint8, sid uint32) scriptFrame {
	var data []byte
	if payload != nil {
		data = mustProtoMarshal(payload)
	}
	return scriptFrame{
		header:  messageHeader{Length: uint32(len(data)), StreamID: sid, Type: messageTypeData, Flags: flags},
		payload: data,
	}
}

func decodeScriptResponse(t *testing.T, frame scriptFrame) *Response {
	t.Helper()
	var response Response
	if err := proto.Unmarshal(frame.payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return &response
}

func decodeScriptData(t *testing.T, frame scriptFrame, message proto.Message) {
	t.Helper()
	if err := proto.Unmarshal(frame.payload[:frame.header.Length], message); err != nil {
		t.Fatalf("decode data: %v", err)
	}
}

func mustProtoMarshal(message proto.Message) []byte {
	data, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return data
}

type scriptControl struct {
	start      chan struct{}
	release    chan struct{}
	received   chan internal.EchoPayload
	halfClosed chan struct{}
	returned   chan struct{}
	statusErr  error
	releaseOne sync.Once
}

func newScriptControl() *scriptControl {
	return &scriptControl{
		start:      make(chan struct{}),
		release:    make(chan struct{}),
		received:   make(chan internal.EchoPayload, 16),
		halfClosed: make(chan struct{}, 1),
		returned:   make(chan struct{}),
	}
}

func (c *scriptControl) allow() {
	c.releaseOne.Do(func() { close(c.release) })
}

func registerScriptService(server *Server, control *scriptControl) {
	server.RegisterService(scriptServiceName, &ServiceDesc{
		Methods: map[string]Method{
			"Unary": func(ctx context.Context, unmarshal func(any) error) (response any, err error) {
				defer close(control.returned)
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				control.start <- struct{}{}
				<-control.release
				if control.statusErr != nil {
					return nil, control.statusErr
				}
				req.Seq++
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"Client": {
				StreamingClient: true,
				Handler: func(ctx context.Context, stream StreamServer) (response any, err error) {
					defer close(control.returned)
					control.start <- struct{}{}
					var last internal.EchoPayload
					for {
						var req internal.EchoPayload
						if recvErr := stream.RecvMsg(&req); recvErr != nil {
							if !errors.Is(recvErr, io.EOF) {
								return nil, recvErr
							}
							control.halfClosed <- struct{}{}
							break
						}
						last = req
						control.received <- req
					}
					<-control.release
					if control.statusErr != nil {
						return nil, control.statusErr
					}
					last.Seq++
					return &last, nil
				},
			},
			"Server": {
				StreamingServer: true,
				Handler: func(ctx context.Context, stream StreamServer) (response any, err error) {
					defer close(control.returned)
					control.start <- struct{}{}
					<-control.release
					if control.statusErr != nil {
						return nil, control.statusErr
					}
					if sendErr := stream.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "one"}); sendErr != nil {
						return nil, sendErr
					}
					if sendErr := stream.SendMsg(&internal.EchoPayload{Seq: 2, Msg: "two"}); sendErr != nil {
						return nil, sendErr
					}
					return nil, nil
				},
			},
			"Bidi": {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(ctx context.Context, stream StreamServer) (response any, err error) {
					defer close(control.returned)
					control.start <- struct{}{}
					for {
						var req internal.EchoPayload
						if recvErr := stream.RecvMsg(&req); recvErr != nil {
							if !errors.Is(recvErr, io.EOF) {
								return nil, recvErr
							}
							control.halfClosed <- struct{}{}
							break
						}
						control.received <- req
						echo := req
						echo.Seq++
						if sendErr := stream.SendMsg(&echo); sendErr != nil {
							return nil, sendErr
						}
					}
					<-control.release
					if control.statusErr != nil {
						return nil, control.statusErr
					}
					return nil, nil
				},
			},
			"Blocked": {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(ctx context.Context, stream StreamServer) (response any, err error) {
					defer close(control.returned)
					control.start <- struct{}{}
					<-control.release
					return nil, nil
				},
			},
		},
	})
}

type scriptHarness struct {
	t               *testing.T
	clientConn      *scriptConn
	serverConn      *scriptConn
	client          *Client
	serverConnState *serverConn
	clientHooks     *clientTestHooks
	hooks           *serverConnTestHooks
	log             *scriptFrameLog
	control         *scriptControl
	clientDone      chan struct{}
}

func newScriptHarness(t *testing.T) *scriptHarness {
	t.Helper()
	clientConn, serverConn, frameLog := newScriptPair(t.Name())
	control := newScriptControl()
	server := mustServer(t)(NewServer())
	registerScriptService(server, control)
	clientDone := make(chan struct{})
	clientHooks := &clientTestHooks{changed: make(chan struct{}, 1024)}
	client := NewClient(clientConn, withClientTestHooks(clientHooks), WithOnClose(func() { close(clientDone) }))
	serverConnection, err := server.newConn(serverConn, nil)
	if err != nil {
		t.Fatal(err)
	}
	hooks := &serverConnTestHooks{
		ready:    make(chan struct{}),
		messages: make(chan messageHeader, 1024),
		changed:  make(chan struct{}, 1024),
		runDone:  make(chan struct{}),
	}
	serverConnection.testHooks = hooks
	go serverConnection.run(context.Background())
	waitFor(t, context.Background(), hooks.ready, "server test hooks")
	h := &scriptHarness{
		t:               t,
		clientConn:      clientConn,
		serverConn:      serverConn,
		client:          client,
		serverConnState: serverConnection,
		clientHooks:     clientHooks,
		hooks:           hooks,
		log:             frameLog,
		control:         control,
		clientDone:      clientDone,
	}
	t.Cleanup(func() { control.allow() })
	t.Cleanup(func() {
		client.Close()
		waitFor(t, testContext(t), clientDone, "client run completion")
	})
	t.Cleanup(func() {
		server.Close()
		waitFor(t, testContext(t), hooks.runDone, "server connection completion")
	})
	return h
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitFor(t *testing.T, ctx context.Context, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
	}
}

func (h *scriptHarness) waitFrames(ctx context.Context, n int) []loggedScriptFrame {
	h.t.Helper()
	frames := h.log.waitCount(ctx, n)
	if len(frames) < n {
		h.t.Fatalf("timed out waiting for %d frames, got %d: %+v", n, len(frames), frames)
	}
	return frames
}

func (h *scriptHarness) waitServerChanged(ctx context.Context) {
	h.t.Helper()
	waitFor(h.t, ctx, h.hooks.changed, "server stream map change")
}

func (h *scriptHarness) clientStreamIDs() []uint32 {
	h.client.streamLock.RLock()
	defer h.client.streamLock.RUnlock()
	ids := make([]uint32, 0, len(h.client.streams))
	for id := range h.client.streams {
		ids = append(ids, uint32(id))
	}
	return ids
}

func (h *scriptHarness) serverStreamIDs() []uint32 {
	var ids []uint32
	h.hooks.streams.Range(func(key, _ any) bool {
		ids = append(ids, key.(uint32))
		return true
	})
	return ids
}

func (h *scriptHarness) activeStreams() int32 {
	return atomicLoadInt32(h.hooks.active)
}

func (h *scriptHarness) waitServerStreamCount(ctx context.Context, want int) []uint32 {
	h.t.Helper()
	for {
		ids := h.serverStreamIDs()
		if len(ids) == want {
			return ids
		}
		select {
		case <-h.hooks.changed:
		case <-ctx.Done():
			h.t.Fatalf("timed out waiting for %d server streams, got %d: %v", want, len(ids), ids)
		}
	}
}

func (h *scriptHarness) waitClientStreamCount(ctx context.Context, want int) []uint32 {
	h.t.Helper()
	for {
		ids := h.clientStreamIDs()
		if len(ids) == want {
			return ids
		}
		select {
		case <-h.clientHooks.changed:
		case <-ctx.Done():
			h.t.Fatalf("timed out waiting for %d client streams, got %d: %v", want, len(ids), ids)
		}
	}
}

func atomicLoadInt32(value *int32) int32 {
	if value == nil {
		return 0
	}
	return atomic.LoadInt32(value)
}

type asyncResult struct {
	response internal.EchoPayload
	err      error
}

func startUnary(ctx context.Context, client *Client) <-chan asyncResult {
	results := make(chan asyncResult, 1)
	go func() {
		var response internal.EchoPayload
		err := client.Call(ctx, scriptServiceName, "Unary", &internal.EchoPayload{Seq: 10, Msg: "request"}, &response)
		results <- asyncResult{response: response, err: err}
	}()
	return results
}

func startStreamRecv(stream ClientStream, target proto.Message) <-chan error {
	results := make(chan error, 1)
	go func() { results <- stream.RecvMsg(target) }()
	return results
}

func assertErrorIs(t *testing.T, err error, targets ...error) {
	t.Helper()
	for _, target := range targets {
		if !errors.Is(err, target) {
			t.Fatalf("error = %v, want %v", err, target)
		}
	}
}

func assertStatus(t *testing.T, err error, code codes.Code, contains string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a grpc status", err)
	}
	if st.Code() != code {
		t.Fatalf("status code = %s, want %s (err=%v)", st.Code(), code, err)
	}
	if contains != "" && !strings.Contains(st.Message(), contains) {
		t.Fatalf("status message = %q, want substring %q", st.Message(), contains)
	}
}

func assertFrame(t *testing.T, got loggedScriptFrame, endpoint string, typ messageType, flags uint8, sid uint32) {
	t.Helper()
	if got.endpoint != endpoint || got.frame.header.Type != typ ||
		got.frame.header.Flags != flags || got.frame.header.StreamID != sid {
		t.Fatalf("frame = endpoint %q, type %s, flags %#x, stream %d; want endpoint %q, type %s, flags %#x, stream %d",
			got.endpoint, got.frame.header.Type, got.frame.header.Flags, got.frame.header.StreamID,
			endpoint, typ, flags, sid)
	}
}

func assertUint32Set(t *testing.T, got []uint32, want ...uint32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	seen := make(map[uint32]bool, len(want))
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("ids = %v, want %v", got, want)
		}
	}
}

func assertChannelClosed(t *testing.T, ctx context.Context, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("%s not closed: %v", description, ctx.Err())
	}
}

func TestStateMachineUnaryContract(t *testing.T) {
	ctx := testContext(t)
	h := newScriptHarness(t)
	call := startUnary(ctx, h.client)
	waitFor(t, ctx, h.control.start, "unary handler start")
	frames := h.waitFrames(ctx, 1)
	assertFrame(t, frames[0], h.clientConn.name, messageTypeRequest, flagRemoteClosed, 1)
	assertUint32Set(t, h.clientStreamIDs(), 1)
	assertUint32Set(t, h.waitServerStreamCount(ctx, 0))
	if got := h.activeStreams(); got != 0 {
		t.Fatalf("active streams before unary registration observation = %d, want 0", got)
	}

	h.control.allow()
	frames = h.waitFrames(ctx, 2)
	assertFrame(t, frames[1], h.serverConn.name, messageTypeResponse, 0, 1)
	result := <-call
	if result.err != nil {
		t.Fatalf("unary call: %v", result.err)
	}
	if result.response.Seq != 11 || result.response.Msg != "request" {
		t.Fatalf("response = %+v, want seq=11 msg=request", result.response)
	}
	assertUint32Set(t, h.waitClientStreamCount(ctx, 0))
	assertUint32Set(t, h.waitServerStreamCount(ctx, 0))
}

func TestStateMachineUnaryRemoteStatusWinsWhenEnqueuedBeforeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	h := newScriptHarness(t)
	call := startUnary(ctx, h.client)
	waitFor(t, ctx, h.control.start, "unary handler start")
	h.control.allow()
	h.waitFrames(ctx, 2)
	cancel()
	result := <-call
	if result.err != nil {
		t.Fatalf("enqueued final response must be observable before context cancellation: %v", result.err)
	}
	assertUint32Set(t, h.waitClientStreamCount(ctx, 0))
}

func TestStateMachineUnaryCancelBeforeRemoteStatusWins(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	h := newScriptHarness(t)
	call := startUnary(ctx, h.client)
	waitFor(t, ctx, h.control.start, "unary handler start")
	cancel()
	result := <-call
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", result.err)
	}
	h.control.allow()
	assertChannelClosed(t, ctx, h.control.returned, "unary handler completion")
	h.waitServerStreamCount(ctx, 0)
}

func TestStateMachineUnaryDeterministicTransportError(t *testing.T) {
	ctx := testContext(t)
	h := newScriptHarness(t)
	call := startUnary(ctx, h.client)
	waitFor(t, ctx, h.control.start, "unary handler start")
	h.serverConn.failNextWrite(errDeterministicTransport)
	h.control.allow()
	result := <-call
	if !errors.Is(result.err, errDeterministicTransport) {
		t.Fatalf("error = %v, want deterministic transport error", result.err)
	}
	waitFor(t, ctx, h.clientDone, "client completion after transport error")
	assertUint32Set(t, h.waitClientStreamCount(ctx, 0))
	assertUint32Set(t, h.waitServerStreamCount(ctx, 0))
}
