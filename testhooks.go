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

// serverTestHooks are optional package-private observation points used by
// the state-machine tests. They are never set by production code paths and
// every call site is nil-guarded, so production behavior is unchanged.
// They do not alter synchronization or error handling; they only observe
// events that already happened.
type serverTestHooks struct {
	// streamRegistered fires after a stream id is stored in the
	// connection's stream map, with the wire id of the stream.
	streamRegistered func(uint32)
	// streamDeleted fires after a finished stream id is removed from the
	// connection's stream map.
	streamDeleted func(uint32)
	// connDone fires when a server connection's run loop has exited.
	connDone func()
}

// clientTestHooks are optional package-private observation points used by
// the state-machine tests. See serverTestHooks for the guarantees.
type clientTestHooks struct {
	// messageDelivered fires from the client receive loop after a frame
	// has been offered to the destination stream's receive buffer (or the
	// stream was closed with a terminal delivery error).
	messageDelivered func(uint32, error)
}
