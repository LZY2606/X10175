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

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceDesc builds the deterministic test service. The implementation
// deliberately does not sleep: synchronization is entirely channel
// driven through the harness.
func (h *stateHarness) serviceDesc() *ServiceDesc {
	return &ServiceDesc{
		Methods: map[string]Method{
			mUnary: func(_ context.Context, unmarshal func(any) error) (any, error) {
				var p internal.EchoPayload
				if err := unmarshal(&p); err != nil {
					return nil, err
				}
				p.Seq++
				return &p, nil
			},
			mHoldUnary: func(_ context.Context, unmarshal func(any) error) (any, error) {
				var p internal.EchoPayload
				if err := unmarshal(&p); err != nil {
					return nil, err
				}
				<-h.blockRelease
				p.Seq++
				return &p, nil
			},
		},
		Streams: map[string]Stream{
			mUpload: {
				StreamingClient: true,
				StreamingServer: false,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var sum int64
					for {
						var p internal.EchoPayload
						if err := ss.RecvMsg(&p); err != nil {
							if errors.Is(err, io.EOF) {
								return &internal.EchoPayload{Seq: sum}, nil
							}
							return nil, err
						}
						sum += p.Seq
					}
				},
			},
			mDownload: {
				StreamingClient: false,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var p internal.EchoPayload
					if err := ss.RecvMsg(&p); err != nil {
						return nil, err
					}
					for i := int64(1); i <= p.Seq; i++ {
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i, Msg: p.Msg}); err != nil {
							return nil, err
						}
					}
					return nil, nil
				},
			},
			mChat: {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for {
						var p internal.EchoPayload
						if err := ss.RecvMsg(&p); err != nil {
							if errors.Is(err, io.EOF) {
								return nil, nil
							}
							return nil, err
						}
						p.Seq++
						if err := ss.SendMsg(&p); err != nil {
							return nil, err
						}
					}
				},
			},
			mErrorChat: {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var p internal.EchoPayload
					if err := ss.RecvMsg(&p); err != nil {
						return nil, err
					}
					return nil, status.Errorf(codes.ResourceExhausted, "quota exhausted: %d", p.Seq)
				},
			},
			mBlockChat: {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					// Signal deterministically that the handler is parked,
					// then wait for the test to release it.
					h.blockEntered <- struct{}{}
					<-h.blockRelease
					var p internal.EchoPayload
					if err := ss.RecvMsg(&p); err != nil {
						return nil, err
					}
					p.Seq++
					return &p, nil
				},
			},
			mSlowUp: {
				StreamingClient: true,
				StreamingServer: false,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					// Consume exactly one buffered message, then park so
					// the server receive goroutine deterministically fills
					// the stream's recv buffer and hits its slow path.
					var p internal.EchoPayload
					if err := ss.RecvMsg(&p); err != nil {
						return nil, err
					}
					h.blockEntered <- struct{}{}
					<-h.blockRelease
					return &internal.EchoPayload{Seq: p.Seq}, nil
				},
			},
		},
	}
}

// client-streaming / server-streaming descriptor helpers.
var (
	descCS = &StreamDesc{StreamingClient: true}
	descSS = &StreamDesc{StreamingServer: true}
	descBidi = &StreamDesc{StreamingClient: true, StreamingServer: true}
	descUnary = &StreamDesc{}
)
