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
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// wireWatchdog bounds how long a test waits for an event that is
// guaranteed to happen. It is never used to "give the system time"; every
// wait in this file is for a condition that correct code always reaches.
const wireWatchdog = 10 * time.Second

// wireDir is one direction of a scripted connection. Writes are queued
// without being delivered; the test releases frames one at a time (or in
// batches) to make them readable by the peer. This makes the exact
// interleaving of frames between client and server fully deterministic.
type wireDir struct {
	name string

	mu      sync.Mutex
	cond    *sync.Cond
	changed chan struct{} // test-side notification, buffered 1

	queued   [][]byte // written frames awaiting release
	readable []byte   // released bytes available to Read
	written  int      // total frames ever queued (cumulative)

	readErr       error // terminal error for Read once readable is drained
	writeErr      error // sticky error for Write
	failNextWrite error // one-shot error for the next Write
	writeClosed   bool

	onWrite func(raw []byte)
}

func newWireDir(name string) *wireDir {
	d := &wireDir{name: name, changed: make(chan struct{}, 1)}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (d *wireDir) write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failNextWrite != nil {
		err := d.failNextWrite
		d.failNextWrite = nil
		return 0, err
	}
	if d.writeErr != nil {
		return 0, d.writeErr
	}
	if d.writeClosed {
		return 0, io.ErrClosedPipe
	}
	frame := append([]byte(nil), p...)
	d.queued = append(d.queued, frame)
	d.written++
	if d.onWrite != nil {
		d.onWrite(frame)
	}
	select {
	case d.changed <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (d *wireDir) read(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.readable) == 0 && d.readErr == nil {
		d.cond.Wait()
	}
	if len(d.readable) > 0 {
		n := copy(p, d.readable)
		d.readable = d.readable[n:]
		return n, nil
	}
	return 0, d.readErr
}

// release moves up to n queued frames into the readable buffer, waking any
// blocked reader. It returns the number of frames actually released.
func (d *wireDir) release(n int) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n > len(d.queued) {
		n = len(d.queued)
	}
	for _, f := range d.queued[:n] {
		d.readable = append(d.readable, f...)
	}
	d.queued = append([][]byte(nil), d.queued[n:]...)
	if n > 0 {
		d.cond.Broadcast()
	}
	return n
}

// closeRead makes Read return err once the readable buffer is drained.
func (d *wireDir) closeRead(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.readErr == nil {
		d.readErr = err
	}
	d.cond.Broadcast()
}

func (d *wireDir) closeWrite() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writeClosed = true
}

func (d *wireDir) injectReadErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.readErr = err
	d.cond.Broadcast()
}

func (d *wireDir) setFailNextWrite(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failNextWrite = err
}

func (d *wireDir) writtenCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.written
}

func (d *wireDir) queuedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.queued)
}

// waitWritten blocks until at least n frames have been written in total on
// this direction. Correct code always reaches n; the watchdog only guards
// against a broken implementation hanging the test forever.
func (d *wireDir) waitWritten(t *testing.T, n int) {
	t.Helper()
	timer := time.NewTimer(wireWatchdog)
	defer timer.Stop()
	for {
		if d.writtenCount() >= n {
			return
		}
		select {
		case <-d.changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for %d frames written on %s (have %d)", n, d.name, d.writtenCount())
		}
	}
}

// enqueueRaw queues a crafted frame as if the local endpoint had written
// it. Used to inject arbitrary (possibly invalid) frames into the peer.
func (d *wireDir) enqueueRaw(raw []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queued = append(d.queued, raw)
	d.written++
	if d.onWrite != nil {
		d.onWrite(raw)
	}
	select {
	case d.changed <- struct{}{}:
	default:
	}
}

// wireConn is a net.Conn whose reads and writes go through wireDirs.
type wireConn struct {
	rd, wr    *wireDir
	closeOnce sync.Once
}

func (c *wireConn) Read(p []byte) (int, error)  { return c.rd.read(p) }
func (c *wireConn) Write(p []byte) (int, error) { return c.wr.write(p) }

// Close mirrors net.Conn semantics: the peer observes EOF after draining
// what was already released, while local reads and writes fail.
func (c *wireConn) Close() error {
	c.closeOnce.Do(func() {
		c.wr.closeRead(io.EOF)
		c.wr.closeWrite()
		c.rd.closeRead(io.ErrClosedPipe)
	})
	return nil
}

type wireAddr string

func (a wireAddr) Network() string { return "wire" }
func (a wireAddr) String() string  { return string(a) }

func (c *wireConn) LocalAddr() net.Addr              { return wireAddr("local") }
func (c *wireConn) RemoteAddr() net.Addr             { return wireAddr("remote") }
func (c *wireConn) SetDeadline(time.Time) error      { return nil }
func (c *wireConn) SetReadDeadline(time.Time) error  { return nil }
func (c *wireConn) SetWriteDeadline(time.Time) error { return nil }

// wireFrame is a parsed ttrpc frame.
type wireFrame struct {
	header  messageHeader
	payload []byte
}

func parseWireFrame(raw []byte) (wireFrame, error) {
	if len(raw) < messageHeaderLength {
		return wireFrame{}, fmt.Errorf("short frame: %d bytes", len(raw))
	}
	mh := messageHeader{
		Length:   binary.BigEndian.Uint32(raw[:4]),
		StreamID: binary.BigEndian.Uint32(raw[4:8]),
		Type:     messageType(raw[8]),
		Flags:    raw[9],
	}
	if int(mh.Length) != len(raw)-messageHeaderLength {
		return wireFrame{}, fmt.Errorf("write contains more than one message: header length %d, frame length %d", mh.Length, len(raw)-messageHeaderLength)
	}
	return wireFrame{header: mh, payload: raw[messageHeaderLength:]}, nil
}

// wireEvent records one frame write on the scripted connection, in the
// global order the writes happened.
type wireEvent struct {
	dir     *wireDir
	header  messageHeader
	payload []byte
	err     error // set if the written bytes were not exactly one frame
}

// wireCtl drives a scripted client<->server connection. It lets a test
// pause any write, release frames explicitly, close either endpoint,
// inject transport errors, and inspect the exact frame sequence.
type wireCtl struct {
	t *testing.T

	c2s *wireDir // client -> server
	s2c *wireDir // server -> client

	client net.Conn
	server net.Conn

	mu     sync.Mutex
	events []wireEvent
}

func newWireCtl(t *testing.T) *wireCtl {
	t.Helper()
	c2s := newWireDir("c2s")
	s2c := newWireDir("s2c")
	ctl := &wireCtl{t: t, c2s: c2s, s2c: s2c}
	c2s.onWrite = func(raw []byte) { ctl.record(c2s, raw) }
	s2c.onWrite = func(raw []byte) { ctl.record(s2c, raw) }
	ctl.client = &wireConn{rd: s2c, wr: c2s}
	ctl.server = &wireConn{rd: c2s, wr: s2c}
	return ctl
}

func (c *wireCtl) record(dir *wireDir, raw []byte) {
	f, err := parseWireFrame(raw)
	e := wireEvent{dir: dir, err: err}
	if err == nil {
		e.header = f.header
		e.payload = f.payload
	}
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

// takeEvents returns the events recorded so far and resets the log.
func (c *wireCtl) takeEvents() []wireEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev := c.events
	c.events = nil
	return ev
}

// queuedFrames parses the frames currently queued (written but not yet
// released) on dir.
func (c *wireCtl) queuedFrames(t *testing.T, dir *wireDir) []wireFrame {
	t.Helper()
	dir.mu.Lock()
	raw := append([][]byte(nil), dir.queued...)
	dir.mu.Unlock()
	frames := make([]wireFrame, 0, len(raw))
	for i, r := range raw {
		f, err := parseWireFrame(r)
		if err != nil {
			t.Fatalf("queued frame %d on %s: %v", i, dir.name, err)
		}
		frames = append(frames, f)
	}
	return frames
}

// writeRaw crafts a frame with arbitrary header fields and queues it on
// dir, as if the local endpoint had sent it.
func (c *wireCtl) writeRaw(dir *wireDir, sid uint32, mt messageType, flags uint8, payload []byte) {
	raw := make([]byte, messageHeaderLength+len(payload))
	binary.BigEndian.PutUint32(raw[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(raw[4:8], sid)
	raw[8] = byte(mt)
	raw[9] = flags
	copy(raw[messageHeaderLength:], payload)
	dir.enqueueRaw(raw)
}

func (c *wireCtl) closeClient() { c.client.Close() }
func (c *wireCtl) closeServer() { c.server.Close() }

// wantEvent describes an expected frame in the event log.
type wantEvent struct {
	dir   *wireDir
	mt    messageType
	flags uint8
	sid   uint32
}

// checkEvents asserts the exact sequence of frames written since the last
// takeEvents call.
func checkEvents(t *testing.T, ctl *wireCtl, want []wantEvent) {
	t.Helper()
	got := ctl.takeEvents()
	if len(got) != len(want) {
		t.Fatalf("expected %d events, got %d: %v", len(want), len(got), describeEvents(got))
	}
	for i, w := range want {
		e := got[i]
		if e.err != nil {
			t.Fatalf("event %d: malformed frame: %v", i, e.err)
		}
		if e.dir != w.dir || e.header.Type != w.mt || e.header.Flags != w.flags || e.header.StreamID != w.sid {
			t.Fatalf("event %d: got %s %s flags=%#x sid=%d, want %s %s flags=%#x sid=%d",
				i, e.dir.name, e.header.Type, e.header.Flags, e.header.StreamID,
				w.dir.name, w.mt, w.flags, w.sid)
		}
	}
}

func describeEvents(events []wireEvent) string {
	s := ""
	for _, e := range events {
		if e.err != nil {
			s += fmt.Sprintf("[%s malformed: %v] ", e.dir.name, e.err)
			continue
		}
		s += fmt.Sprintf("[%s %s flags=%#x sid=%d] ", e.dir.name, e.header.Type, e.header.Flags, e.header.StreamID)
	}
	return s
}
