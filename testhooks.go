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

// This file contains small package-private hooks used exclusively by the
// state-machine tests in this package to observe stream lifecycle events
// without changing any externally visible behavior. They must remain nil in
// production. Hooks are invoked synchronously at the point of the state
// transition, possibly while locks are held, so they must be fast and must
// not call back into the client or server.

// clientStreamTableHook, when non-nil, is invoked after a stream is added to
// or removed from the client's stream table.
var clientStreamTableHook func(id uint32, added bool)

// serverStreamTableHook, when non-nil, is invoked after a stream is added to
// or removed from a server connection's stream table.
var serverStreamTableHook func(id uint32, added bool)

// streamDeliverHook, when non-nil, is invoked after a message has been
// queued to a stream's receive buffer by the connection receive loop.
var streamDeliverHook func(id uint32)
