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
	"net"
	"sync"
	"time"
)

// memPipe returns two in-memory connected net.Conn values backed by a
// growable byte buffer per direction. Unlike net.Pipe, a released Write
// completes without rendez-vousing with a reader; withheld (gated) writes
// never reach the buffer and stay invisible to the peer.
func memPipe() (a, b net.Conn) {
	a2b := newMemPipeQueue()
	b2a := newMemPipeQueue()
	ca := &memConn{out: a2b, in: b2a, done: make(chan struct{})}
	cb := &memConn{out: b2a, in: a2b, done: make(chan struct{})}
	ca.peerDone = cb.done
	cb.peerDone = ca.done
	return ca, cb
}

type memPipeQueue struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte // pending unread bytes
	shut bool
}

func newMemPipeQueue() *memPipeQueue {
	q := &memPipeQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *memPipeQueue) write(b []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shut {
		return 0, errMemConnClosed
	}
	q.buf = append(q.buf, b...)
	n := len(b)
	q.cond.Broadcast()
	return n, nil
}

func (q *memPipeQueue) read(p []byte, done <-chan struct{}) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.buf) == 0 {
		if q.shut {
			return 0, errMemConnClosed
		}
		q.cond.Wait()
	}
	n := copy(p, q.buf)
	q.buf = q.buf[n:]
	if len(q.buf) == 0 {
		q.buf = q.buf[:0]
	}
	q.cond.Broadcast()
	return n, nil
}

func (q *memPipeQueue) shutdown() {
	q.mu.Lock()
	q.shut = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

var errMemConnClosed = &net.OpError{Op: "read/write", Err: memClosed{}}

type memClosed struct{}

func (memClosed) Error() string   { return "memconn: closed" }
func (memClosed) Timeout() bool   { return false }
func (memClosed) Temporary() bool { return false }

type memConn struct {
	out, in  *memPipeQueue
	done     chan struct{}
	peerDone <-chan struct{}
	once     sync.Once
}

func (c *memConn) Read(b []byte) (int, error)  { return c.in.read(b, c.done) }
func (c *memConn) Write(b []byte) (int, error) { return c.out.write(b) }

func (c *memConn) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.out.shutdown()
		c.in.shutdown()
	})
	return nil
}

func (c *memConn) LocalAddr() net.Addr              { return memAddr{} }
func (c *memConn) RemoteAddr() net.Addr             { return memAddr{} }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

type memAddr struct{}

func (memAddr) Network() string { return "mempipe" }
func (memAddr) String() string  { return "mempipe" }
