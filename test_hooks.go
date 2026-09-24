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

type serverTestEvent struct {
	kind string
	id   uint32
	sh   *streamHandler
}

type serverTestHooks struct {
	mu           sync.Mutex
	cond         *sync.Cond
	events       []serverTestEvent
	shutdownWake chan struct{}
}

func newServerTestHooks() *serverTestHooks {
	hooks := &serverTestHooks{
		shutdownWake: make(chan struct{}, 1),
	}
	hooks.cond = sync.NewCond(&hooks.mu)
	return hooks
}

func (h *serverTestHooks) emit(kind string, id uint32, sh *streamHandler) {
	if h == nil {
		return
	}

	h.mu.Lock()
	h.events = append(h.events, serverTestEvent{kind: kind, id: id, sh: sh})
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *serverTestHooks) waitEvent(kind string, id uint32) serverTestEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	for {
		for _, event := range h.events {
			if event.kind == kind && (id == 0 || event.id == id) {
				return event
			}
		}
		h.cond.Wait()
	}
}

func (h *serverTestHooks) eventOrder(kind string, id uint32) []serverTestEvent {
	event := h.waitEvent(kind, id)

	h.mu.Lock()
	defer h.mu.Unlock()

	for i, candidate := range h.events {
		if candidate.kind == event.kind && candidate.id == event.id {
			result := make([]serverTestEvent, i+1)
			copy(result, h.events[:i+1])
			return result
		}
	}
	return nil
}

func (h *serverTestHooks) wakeShutdown() {
	if h == nil {
		return
	}

	select {
	case h.shutdownWake <- struct{}{}:
	default:
	}
}
