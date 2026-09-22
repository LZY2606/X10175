package ttrpc

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// This file implements a fully scriptable in-memory transport used by the
// stream state machine tests. It never touches real ports, never sleeps, and
// every frame crossing the wire is held until the test explicitly releases
// it (unless a direction is switched to automatic mode). This makes every
// interleaving of the client and server state machines replayable.

// wireWrite is a single Write call that is parked until the test releases it.
type wireWrite struct {
	data     []byte
	resolved bool
	err      error
}

// wireDir is one direction of a scripted connection.
type wireDir struct {
	mu      sync.Mutex
	cond    sync.Cond
	changed chan struct{} // cap 1, signaled on every state change

	pending  []*wireWrite // writes parked until released by the test
	released [][]byte     // frames visible to the reader, in order
	readBuf  []byte       // remainder of the frame currently being read

	auto     bool  // when true, writes are released immediately
	closed   bool  // reads drain remaining frames then return io.EOF
	readErr  error // injected deterministic read error
	writeErr error // injected deterministic write error
}

func newWireDir() *wireDir {
	d := &wireDir{changed: make(chan struct{}, 1)}
	d.cond.L = &d.mu
	return d
}

func (d *wireDir) signal() {
	select {
	case d.changed <- struct{}{}:
	default:
	}
}

// write implements the conn side. In manual mode the call blocks until the
// test releases the frame; in auto mode it is queued for the reader at once.
func (d *wireDir) write(p []byte) (int, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if d.writeErr != nil {
		err := d.writeErr
		d.mu.Unlock()
		return 0, err
	}
	if d.auto {
		d.released = append(d.released, append([]byte(nil), p...))
		d.mu.Unlock()
		d.signal()
		return len(p), nil
	}
	w := &wireWrite{data: append([]byte(nil), p...)}
	d.pending = append(d.pending, w)
	d.signal()
	for !w.resolved {
		d.cond.Wait()
	}
	err := w.err
	d.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// read implements the conn side. Released frames are drained in order before
// any injected error or EOF is observed, mirroring a real transport.
func (d *wireDir) read(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.readBuf) == 0 {
		if len(d.released) > 0 {
			d.readBuf = d.released[0]
			d.released = d.released[1:]
			continue
		}
		if d.readErr != nil {
			return 0, d.readErr
		}
		if d.closed {
			return 0, io.EOF
		}
		d.cond.Wait()
	}
	n := copy(p, d.readBuf)
	d.readBuf = d.readBuf[n:]
	return n, nil
}

// releaseOne moves the oldest parked write into the readable queue and
// unblocks its writer. It reports whether a write was released.
func (d *wireDir) releaseOne() (*wireWrite, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return nil, false
	}
	w := d.pending[0]
	d.pending = d.pending[1:]
	d.released = append(d.released, w.data)
	w.resolved = true
	d.cond.Broadcast()
	d.signal()
	return w, true
}

// peekPending returns the oldest parked write without releasing it.
func (d *wireDir) peekPending() ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return nil, false
	}
	return append([]byte(nil), d.pending[0].data...), true
}

// inject appends a frame directly to the readable queue, bypassing any
// parked writer. Used to replay late or duplicate frames from a peer.
func (d *wireDir) inject(frame []byte) {
	d.mu.Lock()
	d.released = append(d.released, append([]byte(nil), frame...))
	d.mu.Unlock()
	d.cond.Broadcast()
	d.signal()
}

func (d *wireDir) setAuto(auto bool) {
	d.mu.Lock()
	d.auto = auto
	if auto {
		for _, w := range d.pending {
			d.released = append(d.released, w.data)
			w.resolved = true
		}
		d.pending = nil
	}
	d.mu.Unlock()
	d.cond.Broadcast()
	d.signal()
}

func (d *wireDir) close() {
	d.mu.Lock()
	d.closed = true
	for _, w := range d.pending {
		w.resolved = true
		w.err = io.ErrClosedPipe
	}
	d.pending = nil
	d.mu.Unlock()
	d.cond.Broadcast()
	d.signal()
}

func (d *wireDir) failReads(err error) {
	d.mu.Lock()
	d.readErr = err
	d.mu.Unlock()
	d.cond.Broadcast()
	d.signal()
}

func (d *wireDir) failWrites(err error) {
	d.mu.Lock()
	d.writeErr = err
	for _, w := range d.pending {
		w.resolved = true
		w.err = err
	}
	d.pending = nil
	d.mu.Unlock()
	d.cond.Broadcast()
	d.signal()
}

// waitFor blocks until pred holds (evaluated under the dir lock) or the
// deadlock guard fires. It never polls and never sleeps.
func (d *wireDir) waitFor(t *testing.T, what string, pred func(*wireDir) bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		d.mu.Lock()
		ok := pred(d)
		d.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deadlock guard: timed out waiting for %s", what)
		}
		select {
		case <-d.changed:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("deadlock guard: timed out waiting for %s", what)
		}
	}
}

// wireConn is a net.Conn whose read and write directions are scripted.
type wireConn struct {
	in  *wireDir
	out *wireDir
}

func (c *wireConn) Read(p []byte) (int, error) { return c.in.read(p) }
func (c *wireConn) Write(p []byte) (int, error) { return c.out.write(p) }
func (c *wireConn) Close() error {
	c.in.close()
	c.out.close()
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

// wireEvent records one frame that became visible on the wire, in the exact
// order the peer could observe it.
type wireEvent struct {
	dir     string // "c2s" or "s2c"
	header  messageHeader
	payload []byte
}

// testWire is the test driver: it owns both connection ends and replays
// frames between them under explicit test control.
type testWire struct {
	c2s *wireDir // client writes, server reads
	s2c *wireDir // server writes, client reads

	client *wireConn
	server *wireConn

	mu     sync.Mutex
	events []wireEvent
}

func newTestWire() *testWire {
	c2s, s2c := newWireDir(), newWireDir()
	return &testWire{
		c2s:    c2s,
		s2c:    s2c,
		client: &wireConn{in: s2c, out: c2s},
		server: &wireConn{in: c2s, out: s2c},
	}
}

func (w *testWire) record(dir string, frame []byte) {
	mh, payload, err := parseFrame(frame)
	if err != nil {
		panic("wire: released frame is not a complete ttrpc message: " + err.Error())
	}
	w.mu.Lock()
	w.events = append(w.events, wireEvent{dir: dir, header: mh, payload: payload})
	w.mu.Unlock()
}

// frameEvents returns the ordered log of frames that became visible on the
// wire, in observation order.
func (w *testWire) frameEvents() []wireEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]wireEvent(nil), w.events...)
}

// expectClientFrame waits for the client to park a write, releases it to the
// server, and returns the parsed frame.
func (w *testWire) expectClientFrame(t *testing.T) (messageHeader, []byte) {
	t.Helper()
	return w.expectFrame(t, w.c2s, "c2s")
}

// expectServerFrame waits for the server to park a write, releases it to the
// client, and returns the parsed frame.
func (w *testWire) expectServerFrame(t *testing.T) (messageHeader, []byte) {
	t.Helper()
	return w.expectFrame(t, w.s2c, "s2c")
}

func (w *testWire) expectFrame(t *testing.T, d *wireDir, dir string) (messageHeader, []byte) {
	t.Helper()
	d.waitFor(t, dir+" frame to be written", func(d *wireDir) bool {
		return len(d.pending) > 0 || d.closed
	})
	raw, ok := d.peekPending()
	if !ok {
		t.Fatalf("expected a parked %s frame, but the direction was closed", dir)
	}
	mh, payload, err := parseFrame(raw)
	if err != nil {
		t.Fatalf("parked %s write is not a complete ttrpc frame: %v", dir, err)
	}
	if _, ok := d.releaseOne(); !ok {
		t.Fatalf("expected a parked %s frame to release", dir)
	}
	w.record(dir, raw)
	return mh, payload
}

// injectServerFrame queues a raw frame as if the server had written it,
// bypassing the server entirely. Used to replay late or duplicate frames.
func (w *testWire) injectServerFrame(sid uint32, mt messageType, flags uint8, payload []byte) {
	frame := buildFrame(sid, mt, flags, payload)
	w.s2c.inject(frame)
	w.record("s2c", frame)
}

// failClientWrites makes subsequent client writes fail with err.
func (w *testWire) failClientWrites(err error) { w.c2s.failWrites(err) }

// failClientReads makes subsequent client reads fail with err.
func (w *testWire) failClientReads(err error) { w.s2c.failReads(err) }

// setAutoClientToServer switches client writes to automatic delivery.
func (w *testWire) setAutoClientToServer() { w.c2s.setAuto(true) }

// setAutoServerToClient switches server writes to automatic delivery.
func (w *testWire) setAutoServerToClient() { w.s2c.setAuto(true) }

// close shuts down both directions and unblocks every parked writer.
func (w *testWire) close() {
	w.c2s.close()
	w.s2c.close()
}

func parseFrame(b []byte) (messageHeader, []byte, error) {
	mh, err := readMessageHeader(make([]byte, messageHeaderLength), bytes.NewReader(b))
	if err != nil {
		return messageHeader{}, nil, err
	}
	if int(mh.Length) != len(b)-messageHeaderLength {
		return messageHeader{}, nil, io.ErrUnexpectedEOF
	}
	return mh, b[messageHeaderLength:], nil
}

func buildFrame(sid uint32, mt messageType, flags uint8, payload []byte) []byte {
	var buf bytes.Buffer
	if err := writeMessageHeader(&buf, make([]byte, messageHeaderLength), messageHeader{
		Length:   uint32(len(payload)),
		StreamID: sid,
		Type:     mt,
		Flags:    flags,
	}); err != nil {
		panic(err)
	}
	buf.Write(payload)
	return buf.Bytes()
}
