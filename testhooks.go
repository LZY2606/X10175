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

// This file contains small, package-private test hooks. They are nil in
// production use (NewClient/NewServer never set them), have no behavior of
// their own, and exist only so the streaming state-machine tests can observe
// deterministic delivery, stream-set and goroutine-lifecycle events without
// real sockets, timers or scheduling assumptions.

// streamDispatchOutcome describes what the client receive loop did with a
// frame once it came off the wire.
type streamDispatchOutcome int

const (
	// dispatchDelivered: the frame was handed to the stream's recv channel.
	dispatchDelivered streamDispatchOutcome = iota
	// dispatchInactive: no stream with that id is registered; the frame was
	// rejected.
	dispatchInactive
	// dispatchRecvError: reading the frame produced a grpc status error and
	// the stream was closed with that error.
	dispatchRecvError
)

// clientTestHooks observes internal client events. Every callback is optional
// and must never block.
type clientTestHooks struct {
	// onStreamRegistered fires after a stream is inserted into the client
	// stream map.
	onStreamRegistered func(id streamID)
	// onStreamDeleted fires after a stream is removed from the client stream
	// map.
	onStreamDeleted func(id streamID)
	// onDispatched fires after the receive loop finished handling one frame
	// for the given stream id.
	onDispatched func(id streamID, hdr messageHeader, outcome streamDispatchOutcome)
	// onRunDone fires from the client receive goroutine after all streams
	// have been cleaned up, immediately before it terminates.
	onRunDone func()
}

// serverConnTestHooks observes internal server-connection events. Every
// callback is optional and must never block.
type serverConnTestHooks struct {
	// onStreamRegistered fires after a request started a new server stream.
	onStreamRegistered func(id uint32)
	// onStreamDeleted fires after the terminal frame for a server stream was
	// sent and the stream was removed from the connection stream set.
	onStreamDeleted func(id uint32)
	// onInactiveStream fires when a data frame arrived for a stream id that
	// is not (or no longer) registered on the server.
	onInactiveStream func(id uint32)
	// onRunDone fires from the server connection goroutine after it removed
	// itself from the server and closed the connection, right before
	// termination.
	onRunDone func()
}
