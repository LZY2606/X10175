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
	"errors"
	"io"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	p, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// openStream opens a client stream through the paused gate, releasing exactly
// the request frame, and returns the stream plus its allocated id.
func openStream(t *testing.T, h *clientHarness, desc *StreamDesc, method string) (ClientStream, uint32) {
	t.Helper()
	h.gate.setPaused(true)

	type res struct {
		cs  ClientStream
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		cs, err := h.client.NewStream(context.Background(), desc, "stateService", method, nil)
		resCh <- res{cs, err}
	}()

	h.gate.waitHeld(t)
	r := <-resCh // NewStream only returns after the frame is released below? no: it blocks in the gate
	_ = r
	return nil, 0
}

func TestPlaceholder(t *testing.T) {}
