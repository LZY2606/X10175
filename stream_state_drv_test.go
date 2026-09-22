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
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// testTimeout bounds waits in deterministic tests. Reaching it means the
// state machine deadlocked or dropped an event; tests never poll or sleep.
const testTimeout = 5 * time.Second

var errScripted = errors.New("ttrpc test: scripted transport failure")
var errScriptedConnClosed = errors.New("scripted conn: closed")

// wireFrame is a decoded view of one ttrpc wire frame.
type wireFrame struct {
	Length   uint32
	StreamID uint32
	Type     messageType
	Flags    uint8
	Payload  []byte
}

func (f *wireFrame) String() string {
	return fmt.Sprintf("frame{sid=%d type=%s flags=0x%x len=%d}", f.StreamID, f.Type, f.Flags, f.Length)
}

func decodeFrame(b []byte) *wireFrame {
	if len(b) < messageHeaderLength {
		return nil
	}
	f := &wireFrame{
		Length:   binary.BigEndian.Uint32(b[0:4]),
		StreamID: binary.BigEndian.Uint32(b[4:8]),
		Type:     messageType(b[8]),
		Flags:    b[9],
	}
	if len(b) >= messageHeaderLength+int(f.Length) {
		f.Payload = append([]byte(nil), b[messageHeaderLength:messageHeaderLength+int(f.Length)]...)
	}
	return f
}

// scriptedConn is an in-memory net.Conn whose reads are driven by the test
// and whose writes are fully observable. It uses no goroutines, ports or
// deadlines: producers append complete frames to the read queue, consumers
// (the production receive loops) block on a condition variable until a
// frame, a scripted read error, or close appears. Two ends may be linked so
// that frames written by one become readable by the other.
type scriptedConn struct {
	name   string
	remote *scriptedConn // peer end, when linked; nil for a lone end

	mu      sync.Mutex
	cond    *sync.Cond
	rdq     [][]byte // queued complete wire frames, FIFO
	rdPos   int
	readErr error // sticky: next Read returns this once
	closed  bool
	closeCh chan struct{}
	wrBuf   []byte // accumulation of the frame currently being written

	paused   bool
	permitCh chan struct{}
	failNext bool

	writtenMu sync.Mutex
	written   [][]byte // complete frames written by this end
	framesCh  chan *wireFrame
}

func newScriptedConn(name string) *scriptedConn {
	c := &scriptedConn{
		name:     name,
		permitCh: make(chan struct{}, 64),
		framesCh: make(chan *wireFrame, 256),
		closeCh:  make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// link pairs two ends: every frame successfully written on one end becomes
// readable on the other.
func linkScriptedConns(a, b *scriptedConn) {
	a.remote = b
	b.remote = a
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			return 0, err
		}
		if len(c.rdq) > 0 {
			n := copy(b, c.rdq[0][c.rdPos:])
			c.rdPos += n
			if c.rdPos == len(c.rdq[0]) {
				c.rdq = c.rdq[1:]
				c.rdPos = 0
			}
			return n, nil
		}
		if c.closed {
			return 0, errScriptedConnClosed
		}
		c.cond.Wait()
	}
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	c.wrBuf = append(c.wrBuf, b...)
	if len(c.wrBuf) < messageHeaderLength {
		return len(b), nil
	}
	flen := int(binary.BigEndian.Uint32(c.wrBuf[0:4])) + messageHeaderLength
	if len(c.wrBuf) < flen {
		return len(b), nil
	}
	frame := append([]byte(nil), c.wrBuf...)
	c.wrBuf = c.wrBuf[:0]

	if c.paused {
		select {
		case <-c.permitCh:
		case <-c.closeCh:
			return len(b), errScriptedConnClosed
		}
	}
	if c.failNext {
		c.failNext = false
		return len(b), errScripted
	}

	c.recordFrame(frame[:flen])
	return len(b), nil
}

func (c *scriptedConn) recordFrame(frame []byte) {
	f := decodeFrame(frame)
	c.writtenMu.Lock()
	c.written = append(c.written, append([]byte(nil), frame...))
	c.writtenMu.Unlock()
	if f != nil {
		select {
		case c.framesCh <- f:
		default:
		}
	}
	if c.remote != nil {
		c.remote.pushRead(frame)
	}
}

// pauseWrites makes every subsequent complete frame block at the gate until
// AllowOneFrame or ResumeWrites.
func (c *scriptedConn) pauseWrites() {
	c.paused = true
}

// resumeWrites releases every frame currently (and subsequently) blocked.
func (c *scriptedConn) resumeWrites() {
	c.paused = false
	for {
		select {
		case <-c.permitCh:
		default:
			return
		}
	}
}

// allowOneFrame releases exactly one frame blocked at the gate.
func (c *scriptedConn) allowOneFrame() {
	c.permitCh <- struct{}{}
}

// failNextWrite makes the next completed write fail deterministically.
func (c *scriptedConn) failNextWrite() {
	c.failNext = true
}

// pushRead appends a complete wire frame to this end's read queue.
func (c *scriptedConn) pushRead(frame []byte) {
	c.mu.Lock()
	if !c.closed {
		c.rdq = append(c.rdq, append([]byte(nil), frame...))
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

// enqueueRaw appends an already encoded frame to this end's read queue.
func (c *scriptedConn) enqueueRaw(frame []byte) {
	c.pushRead(frame)
}

// scriptReadError arranges the next Read to return err exactly once.
func (c *scriptedConn) scriptReadError(err error) {
	c.mu.Lock()
	c.readErr = err
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.closeCh)
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	c.resumeWrites()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr(c.name) }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr(c.name + "-remote") }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type scriptedAddr string

func (a scriptedAddr) Network() string { return "script" }
func (a scriptedAddr) String() string  { return string(a) }

// ---------------------------------------------------------------------------
// Frame builders and assertions
// ---------------------------------------------------------------------------

func encodeFrame(streamID uint32, mt messageType, flags uint8, payload []byte) []byte {
	b := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(b[4:8], streamID)
	b[8] = byte(mt)
	b[9] = flags
	copy(b[messageHeaderLength:], payload)
	return b
}

func marshalRequest(t testing.TB, service, method string, payload []byte) []byte {
	t.Helper()
	b, err := proto.Marshal(&Request{Service: service, Method: method, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return encodeFrame(1, messageTypeRequest, 0, b)
}

func responseFrame(t testing.TB, streamID uint32, st *status.Status, payload []byte) []byte {
	t.Helper()
	rsp := &Response{Payload: payload}
	if st != nil {
		rsp.Status = st.Proto()
	}
	b, err := proto.Marshal(rsp)
	if err != nil {
		t.Fatal(err)
	}
	return encodeFrame(streamID, messageTypeResponse, 0, b)
}

func dataFrame(streamID uint32, flags uint8, payload []byte) []byte {
	return encodeFrame(streamID, messageTypeData, flags, payload)
}

func decodeResponse(t testing.TB, f *wireFrame) *Response {
	t.Helper()
	rsp := &Response{}
	if err := proto.Unmarshal(f.Payload, rsp); err != nil {
		t.Fatal(err)
	}
	return rsp
}

func mustWaitFrame(t testing.TB, ch <-chan *wireFrame) *wireFrame {
	t.Helper()
	select {
	case f := <-ch:
		return f
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for wire frame")
		return nil
	}
}

func expectNoFrame(t testing.TB, ch <-chan *wireFrame, note string) {
	t.Helper()
	select {
	case f := <-ch:
		t.Fatalf("%s: unexpected frame %s", note, f)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitSignal(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertNoSignal(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected signal for %s", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitErrSignal(t testing.TB, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func statusCodeOf(err error) codes.Code {
	st, ok := status.FromError(err)
	if !ok {
		return codes.Unknown
	}
	return st.Code()
}

// eventRecorder serializes arbitrary lifecycle observations into one total
// order so tests can assert exact event sequences across goroutines.
type eventRecorder struct {
	mu     sync.Mutex
	events []string
	sig    chan struct{}
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{sig: make(chan struct{}, 4096)}
}

func (r *eventRecorder) emit(ev string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.sig <- struct{}{}:
	default:
	}
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// expectSequence waits until at least len(want) events have been recorded,
// then asserts the recorded prefix equals want.
func (r *eventRecorder) expectSequence(t testing.TB, want []string) {
	t.Helper()
	deadline := time.After(testTimeout)
	for range len(want) {
		select {
		case <-r.sig:
		case <-deadline:
			t.Fatalf("timed out waiting for events; got %v want %v", r.snapshot(), want)
		}
	}
	got := r.snapshot()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d mismatch: got %q want %q (all got %v want %v)", i, got[i], want[i], got, want)
		}
	}
}

func (r *eventRecorder) expectExact(t testing.TB, want []string) {
	t.Helper()
	r.expectSequence(t, want)
	got := r.snapshot()
	if len(got) != len(want) {
		t.Fatalf("extra events: got %v want %v", got, want)
	}
}
