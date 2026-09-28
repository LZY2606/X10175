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

// This file holds the deterministic, replayable state-machine test driver
// used by the streaming contract tests.
//
// The driver replaces every OS dependency with an in-memory, fully
// scripted transport (see scriptedConn). There are no real ports, no
// goroutine-scheduling assumptions and no time.Sleep: progress is always
// observed through explicit frame gates and completion channels. The only
// timers used are upper-bound test deadlines that turn an unexpected
// deadlock into a failure.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// errFSMConnClosed mirrors the error string the standard library produces
// when a closed connection is used, so filterCloseErr treats it exactly as
// it treats the real thing.
var errFSMConnClosed = errors.New("use of closed network connection")

type fsmAddr struct{}

func (fsmAddr) Network() string { return "fsm-pipe" }
func (fsmAddr) String() string  { return "fsm-pipe" }

// wireFrame is one framed protocol message.
type wireFrame struct {
	hdr messageHeader
	p   []byte
}

func (f wireFrame) encodedLen() int { return messageHeaderLength + int(f.hdr.Length) }

func (f wireFrame) encode() []byte {
	b := make([]byte, f.encodedLen())
	binary.BigEndian.PutUint32(b[0:4], f.hdr.Length)
	binary.BigEndian.PutUint32(b[4:8], f.hdr.StreamID)
	b[8] = byte(f.hdr.Type)
	b[9] = f.hdr.Flags
	copy(b[messageHeaderLength:], f.p)
	return b
}

func decodeFSMFrame(b []byte) wireFrame {
	return wireFrame{
		hdr: messageHeader{
			Length:   binary.BigEndian.Uint32(b[0:4]),
			StreamID: binary.BigEndian.Uint32(b[4:8]),
			Type:     messageType(b[8]),
			Flags:    b[9],
		},
		p: append([]byte(nil), b[messageHeaderLength:messageHeaderLength+int(binary.BigEndian.Uint32(b[0:4]))]...),
	}
}

// gateOutcome decides how a paused write resumes.
type gateOutcome struct {
	err error
}

// writeGate parks one matching Write until the test resolves it.
type writeGate struct {
	pred    func(wireFrame) bool
	hit     chan struct{}
	resolve chan gateOutcome

	once   sync.Once
	closed bool
	mu     sync.Mutex
}

func newWriteGate(pred func(wireFrame) bool) *writeGate {
	return &writeGate{
		pred:    pred,
		hit:     make(chan struct{}),
		resolve: make(chan gateOutcome, 1),
	}
}

// hitCh returns a channel closed once the matching writer is parked.
func (g *writeGate) hitCh() <-chan struct{} { return g.hit }

func (g *writeGate) resume() {
	g.resolve <- gateOutcome{}
}

func (g *writeGate) fail(err error) {
	g.resolve <- gateOutcome{err: err}
}

// scriptedConn is one endpoint of an in-memory, frame-oriented connection.
// A pair (see newFSMPipe) plays the role of a client and server net.Conn.
//
// Writes are parsed into complete ttrpc frames. Each completed frame is
// handed to the peer's inbound queue and recorded on "sent". A writeGate
// allows a test to park a specific frame before it is delivered and then
// either release it or fail the write (optionally severing the connection
// as a real transport error would).
type scriptedConn struct {
	name string

	writeMu sync.Mutex
	writeBuf []byte
	gateMu  sync.Mutex
	gate    *writeGate
	writeFailed bool
	writeErr    error

	sent chan wireFrame

	readMu  sync.Mutex
	cur     *bytes.Reader
	in      chan wireFrame
	readErr chan error

	reads      atomic.Uint64
	readPulse  chan uint64

	closeMu sync.Mutex
	closed  bool
	peer    *scriptedConn
}

func newFSMPipe(clientName, serverName string) (*scriptedConn, *scriptedConn) {
	client := &scriptedConn{
		name:      clientName,
		sent:      make(chan wireFrame, 4096),
		in:        make(chan wireFrame, 4096),
		readErr:   make(chan error, 1),
		readPulse: make(chan uint64, 4096),
	}
	server := &scriptedConn{
		name:      serverName,
		sent:      make(chan wireFrame, 4096),
		in:        make(chan wireFrame, 4096),
		readErr:   make(chan error, 1),
		readPulse: make(chan uint64, 4096),
	}
	client.peer = server
	server.peer = client
	return client, server
}

// Read serves the next inbound frame as a contiguous byte stream, matching
// how a bufio.Reader consumes a real connection.
func (c *scriptedConn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		if c.cur != nil && c.cur.Len() > 0 {
			return c.cur.Read(b)
		}
		c.cur = nil

		select {
		case f := <-c.in:
			n := c.reads.Add(1)
			c.cur = bytes.NewReader(f.encode())
			select {
			case c.readPulse <- n:
			default:
			}
		case err := <-c.readErr:
			return 0, err
		}
	}
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	n := len(b)
	if c.writeFailed {
		return 0, c.writeErr
	}
	c.closeMu.Lock()
	closed := c.closed
	c.closeMu.Unlock()
	if closed {
		return 0, errFSMConnClosed
	}

	c.writeBuf = append(c.writeBuf, b...)

	for len(c.writeBuf) >= messageHeaderLength {
		hdr := messageHeader{
			Length:   binary.BigEndian.Uint32(c.writeBuf[0:4]),
			StreamID: binary.BigEndian.Uint32(c.writeBuf[4:8]),
			Type:     messageType(c.writeBuf[8]),
			Flags:    c.writeBuf[9],
		}
		total := messageHeaderLength + int(hdr.Length)
		if len(c.writeBuf) < total {
			break
		}
		f := wireFrame{hdr: hdr, p: append([]byte(nil), c.writeBuf[messageHeaderLength:total]...)}
		c.writeBuf = c.writeBuf[total:]

		c.gateMu.Lock()
		gate := c.gate
		if gate != nil && !gate.consumed() && gate.pred(f) {
			c.gate = nil
			gate.markConsumed()
		} else {
			gate = nil
		}
		c.gateMu.Unlock()

		if gate != nil {
			close(gate.hit)
			outcome := <-gate.resolve
			if outcome.err != nil {
				c.writeFailed = true
				c.writeErr = outcome.err
				if errors.Is(outcome.err, errFSMConnClosed) {
					c.severBoth()
				}
				return 0, outcome.err
			}
		}

		c.deliver(f)
		select {
		case c.sent <- f:
		default:
		}
	}

	return n, nil
}

func (g *writeGate) consumed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

func (g *writeGate) markConsumed() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (c *scriptedConn) deliver(f wireFrame) {
	c.peer.in <- f
}

// injectInbound queues a frame as if the peer had sent it (including frames
// the production client would never legitimately emit).
func (c *scriptedConn) injectInbound(f wireFrame) {
	c.in <- f
}

// injectReadError makes the next Read return err once queued frames drain.
func (c *scriptedConn) injectReadError(err error) {
	select {
	case c.readErr <- err:
	default:
	}
}

// pauseNext arms a one-shot gate for the next matching outbound frame.
func (c *scriptedConn) pauseNext(g *writeGate) {
	c.gateMu.Lock()
	c.gate = g
	c.gateMu.Unlock()
}

func (c *scriptedConn) waitReads(ctx context.Context, n uint64, what string) {
	for c.reads.Load() < n {
		select {
		case <-c.readPulse:
		case <-ctx.Done():
			panicAt(c.name+": timed out waiting for "+what, ctx.Err())
		}
	}
}

func panicAt(msg string, err error) {
	panic(fmt.Sprintf("%s: %v", msg, err))
}

// severBoth closes both endpoints as a fatal transport error would.
func (c *scriptedConn) severBoth() {
	c.markClosed()
	c.peer.markClosed()
	c.peer.injectReadError(io.EOF)
}

func (c *scriptedConn) markClosed() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if !c.closed {
		c.closed = true
		c.injectReadError(errFSMConnClosed)
	}
}

// Close closes this endpoint; the peer observes io.EOF on its next Read.
func (c *scriptedConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.readErr <- errFSMConnClosed
	c.closeMu.Unlock()

	c.peer.closeMu.Lock()
	if !c.peer.closed {
		c.peer.closed = true
		c.peer.readErr <- io.EOF
	}
	c.peer.closeMu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr  { return fsmAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr { return fsmAddr{} }

func (*scriptedConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// ---- frame constructors -------------------------------------------------

func fsmRequestFrame(t *testing.T, sid uint32, flags uint8, service, method string, req proto.Message) wireFrame {
	t.Helper()
	var payload []byte
	if req != nil {
		var err error
		payload, err = proto.Marshal(req)
		if err != nil {
			t.Fatalf("marshal request payload: %v", err)
		}
	}
	r := &Request{Service: service, Method: method, Payload: payload}
	p, err := proto.Marshal(r)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return wireFrame{hdr: messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeRequest, Flags: flags}, p: p}
}

func fsmDataFrame(t *testing.T, sid uint32, flags uint8, m proto.Message) wireFrame {
	t.Helper()
	var p []byte
	if m != nil {
		var err error
		p, err = proto.Marshal(m)
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
	}
	return wireFrame{hdr: messageHeader{Length: uint32(len(p)), StreamID: sid, Type: messageTypeData, Flags: flags}, p: p}
}

func fsmDecodeResponse(t *testing.T, f wireFrame) *Response {
	t.Helper()
	if f.hdr.Type != messageTypeResponse {
		t.Fatalf("expected response frame, got %s", f.hdr.Type)
	}
	resp := &Response{}
	if err := proto.Unmarshal(f.p, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}
