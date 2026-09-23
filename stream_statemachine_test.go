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
	"strings"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The tests in this file pin the observable contract of the ttrpc stream
// state machine: concrete return values, status codes, frame ordering, and
// the exact points at which streams enter and leave the client's stream
// table. Every scenario is driven through the wireGate test driver, so event
// sequences are replayed deterministically with no real ports, no scheduling
// luck, and no time.Sleep.

const smService = "stateMachineService"

// echoSMMethod is a unary method that increments Seq.
func echoSMMethod() Method {
	return func(_ context.Context, unmarshal func(any) error) (any, error) {
		var req internal.EchoPayload
		if err := unmarshal(&req); err != nil {
			return nil, err
		}
		req.Seq++
		return &req, nil
	}
}

// callEcho performs a unary Echo call, failing instead of hanging.
func callEcho(t *testing.T, c *Client, ctx context.Context, seq int64) (*internal.EchoPayload, error) {
	t.Helper()
	req := &internal.EchoPayload{Seq: seq}
	resp := &internal.EchoPayload{}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Call(ctx, smService, "Echo", req, resp) }()
	select {
	case err := <-errCh:
		return resp, err
	case <-timeout():
		t.Fatal("Call blocked unexpectedly")
		return nil, nil
	}
}

func timeout() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		select {
		case <-ch:
		case <-after10s():
		}
	}()
	return after10s()
}

func after10s() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		<-after10sReal()
	}()
	return ch
}
