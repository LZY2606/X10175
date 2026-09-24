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

// testHooks are optional, package-private observation points used solely by
// the in-package state-machine tests. Every field is optional and all call
// sites are nil-guarded, so production behavior and synchronization are
// unchanged when no hooks are installed.
//
// Hook implementations must not block on the paths that invoked them and
// must not call back into the component being observed; tests use channels
// and recorders that satisfy these constraints.
type testHooks struct {
	// clientInactiveStream is called by the client receive loop when a
	// frame arrives for a stream id that is no longer (or never was) in
	// the connection's stream map.
	clientInactiveStream func(uint32)

	// clientStreamsCleaned is called once after the client receive loop
	// exits and every registered stream has been torn down.
	clientStreamsCleaned func()

	// serverStreamRegistered is called from a connection's run goroutine
	// after the handler for a new request has been registered.
	serverStreamRegistered func(uint32)

	// serverStreamDeleted is called from a connection's run goroutine
	// after a terminal response removed the stream from the stream map
	// and decremented the active counter.
	serverStreamDeleted func(uint32)
}

// withTestHooks installs observation hooks used by the state-machine tests.
// It is a package-private test option and must not be used outside tests.
func withTestHooks(h *testHooks) ServerOpt {
	return func(c *serverConfig) error {
		c.hooks = h
		return nil
	}
}
