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

func (c *Client) testStreamIDs() map[streamID]struct{} {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()

	ids := make(map[streamID]struct{}, len(c.streams))
	for id := range c.streams {
		ids[id] = struct{}{}
	}
	return ids
}

func (s *Server) testWakeShutdown() {
	select {
	case s.testWake <- struct{}{}:
	default:
	}
}

func (c *serverConn) testStreamIDs() map[uint32]*streamHandler {
	ids := make(map[uint32]*streamHandler)
	c.streams.Range(func(key, value any) bool {
		ids[key.(uint32)] = value.(*streamHandler)
		return true
	})
	return ids
}

func (c *serverConn) testActiveStreams() int {
	return int(c.active.Load())
}

type clientTestHooks interface {
	streamDelivered(uint32, messageHeader)
	streamInactive(uint32, messageHeader)
}

type serverTestHooks interface {
	streamClosing(uint32)
	streamHalfClosed(uint32)
	streamDataRejected(uint32)
	streamClosed(uint32)
	connStateChanged(connState)
	shutdownLoop()
}
