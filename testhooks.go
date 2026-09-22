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

// This file contains small package-private observation hooks used by the
// deterministic streaming state-machine tests. They are the only seam the
// tests need beyond the exported API: they never block, never alter control
// flow, and default to no-ops. Production synchronization and error
// handling remain entirely in the production files.

var (
	// testHookStreamDelivered is invoked after the client receive loop has
	// successfully enqueued a frame into a live stream's receive buffer.
	// The argument is the stream id.
	testHookStreamDelivered = func(streamID uint32) {}

	// testHookInactiveStream is invoked when the client receive loop gets a
	// frame for a stream id that is not present in the connection's stream
	// map (late frames, reused ids, frames after terminal status).
	testHookInactiveStream = func(streamID uint32) {}
)
