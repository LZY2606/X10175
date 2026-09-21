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

import "sync"

// streamTestHooks is a collection of package-private observation points used
// exclusively by the deterministic state-machine tests. Every field is
// optional and defaults to a no-op; production synchronization, ordering and
// error handling remain unchanged.
//
// The hooks must never block indefinitely: a blocked hook blocks the
// production goroutine that invoked it.
type streamTestHooks struct {
	// clientStreamRegistered fires after a stream is inserted into the
	// client connection's stream table.
	clientStreamRegistered func(sid streamID)
	// clientStreamDeleted fires after a stream is removed from the client
	// connection's stream table.
	clientStreamDeleted func(sid streamID)
	// clientReceiveBlocked fires when the client receive loop is about to
	// wait on a stream whose receive buffer is already full.
	clientReceiveBlocked func(sid streamID)

	// serverStreamDeleted fires after a finished stream is removed from a
	// server connection's stream table, while the response is still being
	// written to the wire (for unary responses) or has just been written
	// (for streaming close frames).
	serverStreamDeleted func(sid uint32, streams *sync.Map)
	// serverStreamTable is handed to tests once a connection's stream table
	// exists, together with the stream id of the first stored stream.
	serverStreamTable func(streams *sync.Map)
	// serverDataBlocked fires when the server receive goroutine has to wait
	// because a streaming handler is not consuming its receive channel.
	serverDataBlocked func(sid uint32)
	// serverConnExited fires after a server connection's run loop returns.
	serverConnExited func()

	// channelSendBlocked is invoked by a sender when another goroutine
	// holds the client's send lock.
	channelSendBlocked func()
}

// streamHooks is replaced as a whole by tests; callers must read the field
// once and nil-check it. The default value leaves all production paths as
// no-ops.
var streamHooks = streamTestHooks{}
