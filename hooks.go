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
	"net"
)

// This file contains observation-only hooks used by the replayable state
// machine tests (stream_state_test.go). The hooks never affect control
// flow, synchronization, or error handling: every call site behaves
// identically whether or not a hook is installed. Methods are safe to
// call on a nil pointer so production code does not have to guard them.

// clientHooks observes internal client state transitions.
type clientHooks struct {
	streamCreated    func(id streamID)
	streamDeleted    func(id streamID)
	messageDelivered func(id streamID, t messageType, flags uint8, delivered bool)
	orphanMessage    func(id streamID, t messageType)
	receiveBlocked   func(id streamID)
	receiveLoopDone  func()
}

func (h *clientHooks) created(id streamID) {
	if h != nil && h.streamCreated != nil {
		h.streamCreated(id)
	}
}

func (h *clientHooks) deleted(id streamID) {
	if h != nil && h.streamDeleted != nil {
		h.streamDeleted(id)
	}
}

func (h *clientHooks) delivered(id streamID, t messageType, flags uint8, ok bool) {
	if h != nil && h.messageDelivered != nil {
		h.messageDelivered(id, t, flags, ok)
	}
}

func (h *clientHooks) orphan(id streamID, t messageType) {
	if h != nil && h.orphanMessage != nil {
		h.orphanMessage(id, t)
	}
}

func (h *clientHooks) blocked(id streamID) {
	if h != nil && h.receiveBlocked != nil {
		h.receiveBlocked(id)
	}
}

func (h *clientHooks) loopDone() {
	if h != nil && h.receiveLoopDone != nil {
		h.receiveLoopDone()
	}
}

// withTestClientHooks installs observation hooks on a client. It is
// package-private and intended only for tests within this package.
func withTestClientHooks(h *clientHooks) ClientOpts {
	return func(c *Client) {
		c.hooks = h
	}
}

// serverHooks observes internal server connection state transitions.
type serverHooks struct {
	connStart        func(c *serverConn)
	connDone         func(c *serverConn)
	connState        func(c *serverConn, state connState)
	streamRegistered func(id uint32)
	streamDeleted    func(id uint32)
	handlerDone      func(id uint32)
	orphanFrame      func(id uint32, t messageType)
	dataDropped      func(id uint32, err error)
	dataBlocked      func(id uint32)
	recvLoopDone     func()
}

func (h *serverHooks) started(c *serverConn) {
	if h != nil && h.connStart != nil {
		h.connStart(c)
	}
}

func (h *serverHooks) finished(c *serverConn) {
	if h != nil && h.connDone != nil {
		h.connDone(c)
	}
}

func (h *serverHooks) state(c *serverConn, state connState) {
	if h != nil && h.connState != nil {
		h.connState(c, state)
	}
}

func (h *serverHooks) registered(id uint32) {
	if h != nil && h.streamRegistered != nil {
		h.streamRegistered(id)
	}
}

func (h *serverHooks) deleted(id uint32) {
	if h != nil && h.streamDeleted != nil {
		h.streamDeleted(id)
	}
}

func (h *serverHooks) finishedHandler(id uint32) {
	if h != nil && h.handlerDone != nil {
		h.handlerDone(id)
	}
}

func (h *serverHooks) orphan(id uint32, t messageType) {
	if h != nil && h.orphanFrame != nil {
		h.orphanFrame(id, t)
	}
}

func (h *serverHooks) droppedData(id uint32, err error) {
	if h != nil && h.dataDropped != nil {
		h.dataDropped(id, err)
	}
}

func (h *serverHooks) blockedData(id uint32) {
	if h != nil && h.dataBlocked != nil {
		h.dataBlocked(id)
	}
}

func (h *serverHooks) recvLoopFinished() {
	if h != nil && h.recvLoopDone != nil {
		h.recvLoopDone()
	}
}

// withTestServerHooks installs observation hooks on a server. It is
// package-private and intended only for tests within this package.
func withTestServerHooks(h *serverHooks) ServerOpt {
	return func(c *serverConfig) error {
		c.hooks = h
		return nil
	}
}

// serveConnTest runs the server side of a scripted connection without a
// listener. Package-private test helper only.
func (s *Server) serveConnTest(conn net.Conn) *serverConn {
	sc, err := s.newConn(conn, nil)
	if err != nil {
		return nil
	}
	go sc.run(context.Background())
	return sc
}
