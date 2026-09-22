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
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file contains a deterministic, replayable state-machine driver for
// the ttrpc streaming implementation. It never listens on a real port,
// never relies on goroutine scheduling luck, and never sleeps on the
// success path.
//
// The driver consists of:
//
//   - scriptedConn: an in-memory net.Conn whose inbound frames are queued
//     by the test and whose outbound frames are captured into a channel.
//     Individual writes may be paused and later released or failed with a
//     chosen error.
//   - clientSMHarness: drives a real *Client against a scriptedConn and
//     exposes the client's stream set and receive-loop completion.
//   - serverSMHarness: drives a real *serverConn (run by the production
//     server loop) against a scriptedConn and exposes the connection's
//     stream set, handler completion and receive-goroutine completion.
//
// Every synchronization point is an explicit channel: tests enqueue a
// frame, pause a write, release one frame, close a side, or inject a
// transport error, then observe exact frame ordering, return values,
// status codes and the point at which a stream leaves the stream set.

const smWait = 5 * time.Second // watchdog only; never hit on the success path

// frame is one captured or scripted wire message.
type frame struct {
	header  messageHeader
	payload []byte
}

func (f frame) streamID() uint32 { return f.header.StreamID }
func (f frame) typ() messageType { return f.header.Type }
func (f frame) flags() uint8     { return f.header.Flags }

// marshalFrame renders a message into a complete wire frame.
func marshalFrame(t *testing.T, sid uint32, typ messageType, flags uint8, m proto.Message) frame {
	t.Helper()
	var p []byte
	if m != nil {
		var err error
		p, err = proto.Marshal(m)
		if err != nil {
			t.Fatalf("marshal frame: %v", err)
		}
	}
	return frame{
		header: messageHeader{
			Length:   uint32(len(p)),
			StreamID: sid,
			Type:     typ,
			Flags:    flags,
		},
		payload: p,
	}
}

// frameBytes renders the frame to the exact wire bytes.
func frameBytes(f frame) []byte {
	buf := make([]byte, messageHeaderLength+len(f.payload))
	buf[0] = byte(f.header.Length >> 24)
	buf[1] = byte(f.header.Length >> 16)
	buf[2] = byte(f.header.Length >> 8)
	buf[3] = byte(f.header.Length)
	buf[4] = byte(f.header.StreamID >> 24)
	buf[5] = byte(f.header.StreamID >> 16)
	buf[6] = byte(f.header.StreamID >> 8)
	buf[7] = byte(f.header.StreamID)
	buf[8] = byte(f.header.Type)
	buf[9] = f.header.Flags
	copy(buf[messageHeaderLength:], f.payload)
	return buf
}

// decodeFrameBytes parses one complete frame from the wire bytes.
func decodeFrameBytes(b []byte) (frame, error) {
	if len(b) < messageHeaderLength {
		return frame{}, fmt.Errorf("short frame: %d bytes", len(b))
	}
	var hbuf [messageHeaderLength]byte
	hdr, err := readMessageHeader(hbuf[:], bytes.NewReader(b[:messageHeaderLength]))
	if err != nil {
		return frame{}, fmt.Errorf("decode header: %w", err)
	}
	if len(b) != messageHeaderLength+int(hdr.Length) {
		return frame{}, fmt.Errorf("frame length mismatch: header says %d, have %d", hdr.Length, len(b)-messageHeaderLength)
	}
	if hdr.Length == 0 {
		return frame{header: hdr}, nil
	}
	payload := make([]byte, hdr.Length)
	copy(payload, b[messageHeaderLength:])
	return frame{header: hdr, payload: payload}, nil
}

// decodeFrame parses one complete frame from the wire bytes.
func decodeFrame(t *testing.T, b []byte) frame {
	t.Helper()
	f, err := decodeFrameBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// writeHold describes one paused write.
type writeHold struct {
	entered chan struct{}
	release chan error // sending nil releases the write; sending an error fails it
}

func newWriteHold() *writeHold {
	return &writeHold{
		entered: make(chan struct{}),
		release: make(chan error, 1),
	}
}

// scriptedConn is a deterministic net.Conn for the state-machine tests.
type scriptedConn struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending []byte // data enqueued but not yet Read
	readErr error  // terminal error returned once pending drains
	closed  bool

	holdsMu sync.Mutex
	holds   []*writeHold

	wmu    sync.Mutex
	wb     []byte // bytes of the frame(s) currently being assembled
	writes chan frame
}

func newScriptedConn() *scriptedConn {
	c := &scriptedConn{
		writes: make(chan frame, 256),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// enqueue appends a frame to the inbound queue.
func (c *scriptedConn) enqueue(f frame) {
	c.enqueueRaw(frameBytes(f))
}

// enqueueRaw appends raw wire bytes (used to script malformed frames).
func (c *scriptedConn) enqueueRaw(b []byte) {
	c.mu.Lock()
	c.pending = append(c.pending, b...)
	c.cond.Broadcast()
	c.mu.Unlock()
}

// failReads makes the next Read after pending data drains return err.
func (c *scriptedConn) failReads(err error) {
	c.mu.Lock()
	c.readErr = err
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	for len(c.pending) == 0 {
		if c.readErr != nil {
			err := c.readErr
			c.mu.Unlock()
			return 0, err
		}
		if c.closed {
			c.mu.Unlock()
			return 0, io.EOF
		}
		c.cond.Wait()
	}
	// Hand the buffered reader exactly one queued chunk per Read so the
	// test fully controls frame boundaries.
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	c.mu.Unlock()
	return n, nil
}

// pauseNextWrite arms a one-shot hold on the next Write call.
func (c *scriptedConn) pauseNextWrite() *writeHold {
	h := newWriteHold()
	c.holdsMu.Lock()
	c.holds = append(c.holds, h)
	c.holdsMu.Unlock()
	return h
}

// pauseWrites arms a persistent hold on every subsequent Write; the
// returned hold refers to the next parked write.
func (c *scriptedConn) pauseWrites() *writeHold {
	h := newWriteHold()
	c.holdsMu.Lock()
	c.holds = append(c.holds, h)
	c.holdsMu.Unlock()
	return h
}

// releaseWrite unblocks the held write successfully.
func (h *writeHold) releaseWrite() { h.release <- nil }

// failWrite unblocks the held write with the given transport error.
func (h *writeHold) failWrite(err error) { h.release <- err }

func (c *scriptedConn) takeHold() *writeHold {
	c.holdsMu.Lock()
	defer c.holdsMu.Unlock()
	if len(c.holds) == 0 {
		return nil
	}
	h := c.holds[0]
	c.holds = c.holds[1:]
	return h
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	if h := c.takeHold(); h != nil {
		close(h.entered)
		if err := <-h.release; err != nil {
			return 0, err
		}
	}

	c.wmu.Lock()
	c.wb = append(c.wb, p...)
	for len(c.wb) >= messageHeaderLength {
		length := int(uint32(c.wb[0])<<24 | uint32(c.wb[1])<<16 |
			uint32(c.wb[2])<<8 | uint32(c.wb[3]))
		total := messageHeaderLength + length
		if len(c.wb) < total {
			break
		}
		f, err := decodeFrameBytes(c.wb[:total])
		if err != nil {
			// Production always emits well-formed frames.
			panic(err)
		}
		c.wb = c.wb[total:]
		c.writes <- f
	}
	c.wmu.Unlock()
	return len(p), nil
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr  { return dummyAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr { return dummyAddr{} }

func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "scripted" }
func (dummyAddr) String() string  { return "scripted" }

// nextWrite blocks until the next complete outbound frame is captured.
func (c *scriptedConn) nextWrite(t *testing.T) frame {
	t.Helper()
	select {
	case f := <-c.writes:
		return f
	case <-time.After(smWait):
		t.Fatalf("timed out waiting for an outbound frame")
		return frame{}
	}
}

// assertNoWrite asserts no frame is currently buffered outbound.
func (c *scriptedConn) assertNoWrite(t *testing.T) {
	t.Helper()
	select {
	case f := <-c.writes:
		t.Fatalf("unexpected outbound frame: type=%s sid=%d", f.header.Type, f.header.StreamID)
	case <-time.After(20 * time.Millisecond):
	}
}

// drainWrites returns all currently buffered outbound frames.
func (c *scriptedConn) drainWrites() []frame {
	var out []frame
	for {
		select {
		case f := <-c.writes:
			out = append(out, f)
		default:
			return out
		}
	}
}

// waitClosed blocks until ch is closed or the watchdog fires.
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(smWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitSignal blocks until ch receives or the watchdog fires.
func waitSignal(t *testing.T, ch <-chan struct{}, what string) { waitClosed(t, ch, what) }

// waitErr blocks until errCh receives or the watchdog fires.
func waitErr(t *testing.T, errCh <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(smWait):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// waitFor waits until pred is observed true, rechecking each time cond
// fires, and fails on the watchdog. Callers are responsible for signaling
// cond from the exact state transition under test (no sleeps).
func waitFor(t *testing.T, cond *sync.Cond, pred func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(smWait)
	cond.L.Lock()
	defer cond.L.Unlock()
	for !pred() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		cond.Wait()
	}
}

// decodeResponse decodes a Response from a frame payload.
func decodeResponse(t *testing.T, f frame) *Response {
	t.Helper()
	if f.typ() != messageTypeResponse {
		t.Fatalf("expected response frame, got %s", f.typ())
	}
	resp := &Response{}
	if err := proto.Unmarshal(f.payload, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

// decodeRequest decodes a Request from a frame payload.
func decodeRequest(t *testing.T, f frame) *Request {
	t.Helper()
	if f.typ() != messageTypeRequest {
		t.Fatalf("expected request frame, got %s", f.typ())
	}
	req := &Request{}
	if err := proto.Unmarshal(f.payload, req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	return req
}

// assertStatusCode checks the grpc status code carried by err.
func assertStatusCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected status code %s, got nil error", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error with code %s, got non-status error: %v", want, err)
	}
	if st.Code() != want {
		t.Fatalf("expected status code %s, got %s (%v)", want, st.Code(), err)
	}
}

// assertExactError asserts errors.Is(err, want).
func assertExactError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}
}

var _ context.Context
