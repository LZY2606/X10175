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
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const smService = "stateService"

// ---------------------------------------------------------------------------
// client-side harness
// ---------------------------------------------------------------------------

type clientSMHarness struct {
	t    *testing.T
	conn *scriptedConn
	c    *Client
}

func newClientSMHarness(t *testing.T) *clientSMHarness {
	t.Helper()
	conn := newScriptedConn()
	c := NewClient(conn)
	h := &clientSMHarness{t: t, conn: conn, c: c}
	t.Cleanup(func() {
		c.Close()
		conn.Close()
	})
	return h
}

// runDone is closed when the client receive loop has fully terminated.
func (h *clientSMHarness) runDone() <-chan struct{} { return h.c.userCloseWaitCh }

// streamIDs snapshots the client's active stream set.
func (h *clientSMHarness) streamIDs() []streamID {
	h.c.streamLock.RLock()
	defer h.c.streamLock.RUnlock()
	ids := make([]streamID, 0, len(h.c.streams))
	for id := range h.c.streams {
		ids = append(ids, id)
	}
	return ids
}

func (h *clientSMHarness) streamCount() int { return len(h.streamIDs()) }

func (h *clientSMHarness) getStream(id streamID) *stream { return h.c.getStream(id) }

// waitStreamSet blocks until the client stream set satisfies pred. The
// condition is woken by the production map-mutation broadcasts, so this is
// deterministic and sleep-free.
func (h *clientSMHarness) waitStreamSet(pred func(int) bool, what string) {
	h.t.Helper()
	waitFor(h.t, h.c.streamCond, func() bool { return pred(len(h.c.streams)) }, what)
}

// waitDeliver installs a one-shot delivery hook on s and returns a
// channel closed when the next message enters s.recv.
func waitDeliver(s *stream) <-chan *streamMessage {
	ch := make(chan *streamMessage, 1)
	var once sync.Once
	s.afterDeliver = func(m *streamMessage) {
		once.Do(func() { ch <- m })
	}
	return ch
}

// newStream issues NewStream and returns the stream plus its captured
// initial request frame.
func (h *clientSMHarness) newStream(ctx context.Context, desc *StreamDesc, method string, req any) (ClientStream, frame) {
	h.t.Helper()
	cs, err := h.c.NewStream(ctx, desc, smService, method, req)
	if err != nil {
		h.t.Fatalf("NewStream: %v", err)
	}
	f := h.conn.nextWrite(h.t)
	if f.typ() != messageTypeRequest {
		h.t.Fatalf("expected initial request frame, got %s", f.typ())
	}
	return cs, f
}

// callAsync runs Client.Call and reports completion.
func (h *clientSMHarness) callAsync(ctx context.Context, method string, req, resp any) <-chan error {
	h.t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- h.c.Call(ctx, smService, method, req, resp) }()
	return errCh
}

// recvAsync runs RecvMsg and reports completion.
func recvAsync(cs ClientStream, m any) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- cs.RecvMsg(m) }()
	return errCh
}

// sendAsync runs SendMsg and reports completion.
func sendAsync(cs ClientStream, m any) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- cs.SendMsg(m) }()
	return errCh
}

// closeSendAsync runs CloseSend and reports completion.
func closeSendAsync(cs ClientStream) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- cs.CloseSend() }()
	return errCh
}

// responseFrame builds a final Response frame.
func responseFrame(t *testing.T, sid uint32, code codes.Code, msg string, m proto.Message) frame {
	t.Helper()
	st := status.New(code, msg)
	resp := &Response{Status: st.Proto()}
	if m != nil {
		p, err := protoMarshal(m)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		resp.Payload = p
	}
	return marshalFrame(t, sid, messageTypeResponse, 0, resp)
}

// dataFrame builds a streaming Data frame. A nil m produces a payload-less
// frame (usually combined with flagNoData).
func dataFrame(t *testing.T, sid uint32, flags uint8, m any) frame {
	t.Helper()
	var p []byte
	if m != nil {
		var err error
		p, err = protoMarshal(m)
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
	}
	return frame{
		header: messageHeader{
			Length:   uint32(len(p)),
			StreamID: sid,
			Type:     messageTypeData,
			Flags:    flags,
		},
		payload: p,
	}
}

// waitRecvParked installs a stream's park hook and returns a channel
// closed once the receive loop parks on backpressure for that stream.
func waitRecvParked(s *stream) <-chan struct{} {
	ch := make(chan struct{})
	s.onPark = func() { close(ch) }
	return ch
}

// ---------------------------------------------------------------------------
// server-side harness
// ---------------------------------------------------------------------------

type serverSMHarness struct {
	t       *testing.T
	server  *Server
	conn    *scriptedConn
	sc      *serverConn
	started chan struct{}

	regMu  sync.Mutex
	regCh  map[uint32]chan *streamHandler
	parkCh map[uint32]chan struct{}
}

func newServerSMHarness(t *testing.T) *serverSMHarness {
	t.Helper()
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	conn := newScriptedConn()
	sc, err := server.newConn(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.recvDone = make(chan struct{})
	h := &serverSMHarness{
		t:       t,
		server:  server,
		conn:    conn,
		sc:      sc,
		started: make(chan struct{}),
		regCh:   make(map[uint32]chan *streamHandler),
		parkCh:  make(map[uint32]chan struct{}),
	}
	sc.onStreamRegistered = func(id uint32, sh *streamHandler) {
		h.regMu.Lock()
		if rc, ok := h.regCh[id]; ok {
			rc <- sh
		}
		if pc, ok := h.parkCh[id]; ok {
			sh.onPark = func() { close(pc) }
		}
		h.regMu.Unlock()
	}
	t.Cleanup(func() {
		sc.close()
		conn.Close()
		server.delConnection(sc)
	})
	return h
}

// start runs the production server connection loop with the given desc.
func (h *serverSMHarness) start(desc *ServiceDesc) {
	h.t.Helper()
	h.server.RegisterService(smService, desc)
	ctx := context.Background()
	go func() {
		close(h.started)
		h.sc.run(ctx)
	}()
	<-h.started
}

// recvDone is closed when the connection receive goroutine exits.
func (h *serverSMHarness) recvDone() <-chan struct{} { return h.sc.recvDone }

func (h *serverSMHarness) waitRunDone() { waitClosed(h.t, h.sc.done, "server conn run to exit") }

// expectRegistration arranges for the harness to report the streamHandler
// registered for sid. Call before enqueuing the request.
func (h *serverSMHarness) expectRegistration(sid uint32) <-chan *streamHandler {
	h.regMu.Lock()
	defer h.regMu.Unlock()
	ch := make(chan *streamHandler, 1)
	h.regCh[sid] = ch
	return ch
}

// expectPark arranges for the given sid's handler to signal when the
// receive goroutine parks on its full recv buffer. Call before enqueuing
// the request.
func (h *serverSMHarness) expectPark(sid uint32) <-chan struct{} {
	h.regMu.Lock()
	defer h.regMu.Unlock()
	ch := make(chan struct{})
	h.parkCh[sid] = ch
	return ch
}

// streamIDs snapshots the active server-side streams.
func (h *serverSMHarness) streamIDs() []uint32 {
	var ids []uint32
	h.sc.streams.Range(func(k, _ any) bool {
		ids = append(ids, k.(uint32))
		return true
	})
	return ids
}

func (h *serverSMHarness) streamCount() int { return len(h.streamIDs()) }

func (h *serverSMHarness) handlerFor(sid uint32) *streamHandler {
	v, ok := h.sc.streams.Load(sid)
	if !ok {
		return nil
	}
	return v.(*streamHandler)
}

// activeCount reports the connection's active stream counter.
func (h *serverSMHarness) activeCount() int32 { return h.sc.active.Load() }

func (h *serverSMHarness) connState() connState {
	st, _ := h.sc.getState()
	return st
}

// requestFrame builds a server-bound Request frame.
func requestFrame(t *testing.T, sid uint32, flags uint8, method string, m any) frame {
	t.Helper()
	req := &Request{Service: smService, Method: method}
	if m != nil {
		p, err := protoMarshal(m)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		req.Payload = p
	}
	return marshalFrame(t, sid, messageTypeRequest, flags, req)
}

// expectResponse waits for an outbound response frame and decodes it.
func (h *serverSMHarness) expectResponse() (frame, *Response) {
	h.t.Helper()
	f := h.conn.nextWrite(h.t)
	if f.typ() != messageTypeResponse {
		h.t.Fatalf("expected response frame, got %s (flags=%#x)", f.typ(), f.flags())
	}
	return f, decodeResponse(h.t, f)
}

// expectData waits for an outbound data frame.
func (h *serverSMHarness) expectData() frame {
	h.t.Helper()
	f := h.conn.nextWrite(h.t)
	if f.typ() != messageTypeData {
		h.t.Fatalf("expected data frame, got %s", f.typ())
	}
	return f
}

// unaryMethod is a scriptable unary method descriptor.
func unaryMethod(fn func(context.Context, *internal.TestPayload) (*internal.TestPayload, error)) Method {
	return func(ctx context.Context, unmarshal func(any) error) (any, error) {
		var req internal.TestPayload
		if err := unmarshal(&req); err != nil {
			return nil, err
		}
		return fn(ctx, &req)
	}
}

// streamDesc builds a ServiceDesc with one scriptable stream. first is
// non-nil when the initial request carries a payload.
func streamDesc(method string, streamingClient, streamingServer bool,
	fn func(ctx context.Context, ss StreamServer) (any, error)) *ServiceDesc {
	return &ServiceDesc{
		Streams: map[string]Stream{
			method: {
				Handler:         fn,
				StreamingClient: streamingClient,
				StreamingServer: streamingServer,
			},
		},
	}
}

// statusErr builds a grpc status error for scripted handlers.
func statusErr(code codes.Code, msg string) error {
	return status.Error(code, msg)
}

// unmarshalTest decodes a proto payload in tests.
func unmarshalTest(t *testing.T, p []byte, m proto.Message) {
	t.Helper()
	if err := proto.Unmarshal(p, m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

// waitHandler blocks until the registration channel reports the handler.
func waitHandler(t *testing.T, ch <-chan *streamHandler) *streamHandler {
	t.Helper()
	select {
	case sh := <-ch:
		return sh
	case <-time.After(smWait):
		t.Fatal("timed out waiting for stream handler registration")
		return nil
	}
}
