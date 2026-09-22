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
	"io"
	"net"
	"sync"
	"time"
)

// scriptedPair is a deterministic, in-memory net.Conn pair used by the
// streaming state machine tests. It never uses real ports, goroutine
// scheduling timing, or wall-clock sleeps to make progress:
//
//   - writes are framed (header + payload) and may be individually held
//     back (holdNextWrites), explicitly released (releaseWrite), or made
//     to fail with a chosen error (failNextWrite);
//   - either side can be closed (Close) or have a terminal read error
//     injected (injectReadError), emulating a peer reset;
//   - complete frames can be injected directly from a side
//     (injectFrame), bypassing the local ttrpc stack;
//   - every frame that actually reaches the peer's inbound queue is
//     recorded with its originating side (recordedFrames).
type scriptedPair struct {
	client *scriptedConn // held by the ttrpc Client
	server *scriptedConn // held by serverConn
}

type scriptedConn struct {
	name string
	peer *scriptedConn

	writeMu sync.Mutex
	acc     []byte // bytes from Write calls not yet parsed into frames

	gateMu    sync.Mutex
	holdN     int
	failN     int
	failErr   error
	pending   []*heldWrite
	closed    bool
	closeCh   chan struct{}
	closeOnce sync.Once
	onHold    func(side string)

	// reader state
	mu           sync.Mutex
	cond         *sync.Cond
	inbound      [][]byte
	rbuf         []byte
	readErr      error
	writerClosed bool

	recMu    sync.Mutex
	recorded []wireFrame
	onFrame  func(from string, f wireFrame)
}

type heldWrite struct {
	frame []byte
	rel   chan struct{}
}

type wireFrame struct {
	header  messageHeader
	payload []byte
}

type scriptedAddr string

func (a scriptedAddr) Network() string { return "scripted" }
func (a scriptedAddr) String() string  { return string(a) }

func newScriptedPair(onFrame func(from string, f wireFrame), onHold func(side string)) *scriptedPair {
	c := &scriptedConn{name: "client", closeCh: make(chan struct{})}
	s := &scriptedConn{name: "server", closeCh: make(chan struct{})}
	c.peer = s
	s.peer = c
	c.onFrame = onFrame
	s.onFrame = onFrame
	c.onHold = onHold
	s.onHold = onHold
	c.cond = sync.NewCond(&c.mu)
	s.cond = sync.NewCond(&s.mu)
	return &scriptedPair{client: c, server: s}
}

func (c *scriptedConn) LocalAddr() net.Addr  { return scriptedAddr(c.name + "-local") }
func (c *scriptedConn) RemoteAddr() net.Addr { return scriptedAddr(c.peer.name + "-remote") }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// holdNextWrites parks the next n complete-frame writes in this side's gate.
func (c *scriptedConn) holdNextWrites(n int) {
	c.gateMu.Lock()
	c.holdN += n
	c.gateMu.Unlock()
}

// failNextWrite makes the next complete-frame write fail synchronously
// with err; the frame is never delivered.
func (c *scriptedConn) failNextWrite(err error) {
	c.gateMu.Lock()
	c.failN++
	c.failErr = err
	c.gateMu.Unlock()
}

// releaseWrite explicitly allows one previously held frame to reach the peer.
func (c *scriptedConn) releaseWrite() bool {
	c.gateMu.Lock()
	if len(c.pending) == 0 {
		c.gateMu.Unlock()
		return false
	}
	h := c.pending[0]
	c.pending = c.pending[1:]
	c.gateMu.Unlock()

	c.deliver(h.frame)
	close(h.rel)
	return true
}

// releaseWrites releases all currently held frames in order.
func (c *scriptedConn) releaseWrites() int {
	n := 0
	for c.releaseWrite() {
		n++
	}
	return n
}

// heldWrites reports how many writes are currently parked in the gate.
func (c *scriptedConn) heldWrites() int {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	return len(c.pending)
}

// injectReadError makes subsequent reads return err after queued frames drain.
func (c *scriptedConn) injectReadError(err error) {
	c.mu.Lock()
	c.readErr = err
	c.cond.Broadcast()
	c.mu.Unlock()
}

// injectFrame delivers a fully formed ttrpc frame as if this side sent it:
// the frame appears in the peer's inbound queue without touching local code.
func (c *scriptedConn) injectFrame(frame []byte) {
	c.deliver(frame)
}

func (c *scriptedConn) deliver(frame []byte) {
	hdr, payload := parseFrame(frame)
	cp := make([]byte, len(payload))
	copy(cp, payload)

	c.peer.mu.Lock()
	c.peer.inbound = append(c.peer.inbound, append([]byte(nil), frame...))
	c.peer.cond.Broadcast()
	c.peer.mu.Unlock()

	c.recMu.Lock()
	c.recorded = append(c.recorded, wireFrame{header: hdr, payload: cp})
	cb := c.onFrame
	c.recMu.Unlock()
	if cb != nil {
		cb(c.name, wireFrame{header: hdr, payload: cp})
	}
}

func parseFrame(b []byte) (messageHeader, []byte) {
	hdr := messageHeader{
		Length:   binary.BigEndian.Uint32(b[0:4]),
		StreamID: binary.BigEndian.Uint32(b[4:8]),
		Type:     messageType(b[8]),
		Flags:    b[9],
	}
	return hdr, b[messageHeaderLength : messageHeaderLength+int(hdr.Length)]
}

// encodeFrame builds a complete frame for injectFrame.
func encodeFrame(streamID uint32, t messageType, flags uint8, payload []byte) []byte {
	b := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(b[4:8], streamID)
	b[8] = byte(t)
	b[9] = flags
	copy(b[messageHeaderLength:], payload)
	return b
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if len(c.rbuf) > 0 {
			n := copy(b, c.rbuf)
			c.rbuf = c.rbuf[n:]
			return n, nil
		}
		if len(c.inbound) > 0 {
			frame := c.inbound[0]
			c.inbound = c.inbound[1:]
			n := copy(b, frame)
			if n < len(frame) {
				c.rbuf = frame[n:]
			}
			return n, nil
		}
		if c.readErr != nil {
			return 0, c.readErr
		}
		if c.writerClosed {
			return 0, io.EOF
		}
		c.cond.Wait()
	}
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	total := len(b)
	c.acc = append(c.acc, b...)

	for len(c.acc) >= messageHeaderLength {
		hdr, _ := parseFrame(c.acc)
		need := messageHeaderLength + int(hdr.Length)
		if len(c.acc) < need {
			break
		}
		frame := make([]byte, need)
		copy(frame, c.acc[:need])
		c.acc = c.acc[need:]

		c.gateMu.Lock()
		if c.closed {
			c.gateMu.Unlock()
			return total, io.ErrClosedPipe
		}
		if c.failN > 0 {
			c.failN--
			err := c.failErr
			c.gateMu.Unlock()
			return total, err
		}
		if c.holdN > 0 {
			c.holdN--
			h := &heldWrite{frame: frame, rel: make(chan struct{})}
			c.pending = append(c.pending, h)
			cb := c.onHold
			c.gateMu.Unlock()
			if cb != nil {
				cb(c.name)
			}
			select {
			case <-h.rel:
			case <-c.closeCh:
				return total, io.ErrClosedPipe
			}
			continue
		}
		c.gateMu.Unlock()
		c.deliver(frame)
	}
	return total, nil
}

func (c *scriptedConn) Close() error {
	c.closeOnce.Do(func() {
		c.gateMu.Lock()
		c.closed = true
		pending := c.pending
		c.pending = nil
		close(c.closeCh)
		c.gateMu.Unlock()

		// Parked writers are aborted.
		for _, h := range pending {
			close(h.rel)
		}

		// Local reads fail immediately once our side is closed.
		c.mu.Lock()
		c.readErr = io.ErrClosedPipe
		c.cond.Broadcast()
		c.mu.Unlock()

		// The peer observes a clean EOF after its queued frames drain.
		c.peer.mu.Lock()
		c.peer.writerClosed = true
		c.peer.cond.Broadcast()
		c.peer.mu.Unlock()
	})
	return nil
}

// recordedFrames returns a snapshot of frames that left this side.
func (c *scriptedConn) recordedFrames() []wireFrame {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	out := make([]wireFrame, len(c.recorded))
	copy(out, c.recorded)
	return out
}

// errScriptedTransport is a sentinel transport error that filterCloseErr
// intentionally does not rewrite, so tests can assert it verbatim.
var errScriptedTransport = errors.New("scripted transport failure")
