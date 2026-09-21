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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/containerd/log"
	"github.com/sirupsen/logrus"
)

// This file contains replayable state-machine tests for the streaming
// implementation. They run over an in-memory, scripted transport (net.Pipe
// plus a blocking write gate) so they neither allocate ports nor depend on
// scheduler timing or time.Sleep. All cross-goroutine synchronization is
// expressed through explicit channels and observable hooks.

const (
	evClientRegistered = "client-registered"
	evClientDeleted    = "client-deleted"
	evClientBlocked    = "client-recv-blocked"

	evServerDeleted = "server-deleted"
	evServerBlocked = "server-data-blocked"
	evServerExited  = "server-conn-exited"

	evWireSend  = "wire-send"
	evWireRecv  = "wire-recv"
	evClientRun = "client-run-exited"
)

var testWaitTimeout = 5 * time.Second

// orderedEventLog is a goroutine-safe append-only record of named events with
// signals keyed by (name, occurrence index), allowing deterministic waits
// without sleeps.
type orderedEventLog struct {
	mu     sync.Mutex
	events []string
	counts map[string]int
	wait   map[string]map[int]chan struct{}
}

func newOrderedEventLog() *orderedEventLog {
	return &orderedEventLog{
		counts: make(map[string]int),
		wait:   make(map[string]map[int]chan struct{}),
	}
}

func (l *orderedEventLog) signalLocked(name string, n int) chan struct{} {
	m, ok := l.wait[name]
	if !ok {
		m = make(map[int]chan struct{})
		l.wait[name] = m
	}
	ch, ok := m[n]
	if !ok {
		ch = make(chan struct{})
		m[n] = ch
	}
	return ch
}

func (l *orderedEventLog) record(name string) {
	l.mu.Lock()
	n := l.counts[name]
	l.counts[name] = n + 1
	l.events = append(l.events, name)
	ch := l.signalLocked(name, n)
	l.mu.Unlock()
	close(ch)
}

// waitN blocks until the n-th (0-based) occurrence of name is recorded.
func (l *orderedEventLog) waitN(t testing.TB, name string, n int) {
	t.Helper()
	l.mu.Lock()
	if l.counts[name] > n {
		l.mu.Unlock()
		return
	}
	ch := l.signalLocked(name, n)
	l.mu.Unlock()
	select {
	case <-ch:
	case <-time.After(testWaitTimeout):
		t.Fatalf("timed out waiting for event %q #%d; observed %v", name, n, l.snapshot())
	}
}

func (l *orderedEventLog) waitEvent(t testing.TB, name string) {
	t.Helper()
	l.waitN(t, name, 0)
}

func (l *orderedEventLog) count(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[name]
}

func (l *orderedEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	copy(out, l.events)
	return out
}

func (l *orderedEventLog) assertExact(t testing.TB, want []string) {
	t.Helper()
	got := l.snapshot()
	if len(got) != len(want) {
		t.Fatalf("event order mismatch:\n got %v\nwant %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("event order mismatch at %d:\n got %v\nwant %v", i, got, want)
		}
	}
}

func (l *orderedEventLog) assertPrefix(t testing.TB, want []string) {
	t.Helper()
	got := l.snapshot()
	if len(got) < len(want) {
		t.Fatalf("event prefix mismatch:\n got %v\nwant prefix %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event prefix mismatch at %d:\n got %v\nwant prefix %v", i, got, want)
		}
	}
}

func (l *orderedEventLog) assertNotRecorded(t testing.TB, name string) {
	t.Helper()
	if c := l.count(name); c > 0 {
		t.Fatalf("event %q was unexpectedly recorded %d times; observed %v", name, c, l.snapshot())
	}
}

// silenceStandardLogger redirects the global logrus logger (used by the
// receive loops for benign protocol rejects) to io.Discard for the duration
// of the test and restores it afterwards.
func silenceStandardLogger(t testing.TB) {
	t.Helper()
	old := logrus.StandardLogger().Out
	logrus.StandardLogger().SetOutput(io.Discard)
	t.Cleanup(func() { logrus.StandardLogger().SetOutput(old) })
	_ = log.L // keep the log import referenced for future ctx wiring
}

// withTestHooks installs the given hooks for the duration of the test and
// resets every hook to a no-op afterwards so no state leaks to later tests.
// Tests using hooks must not call t.Parallel().
func withTestHooks(t testing.TB, h streamTestHooks) {
	t.Helper()
	prev := streamHooks
	streamHooks = h
	t.Cleanup(func() { streamHooks = streamTestHooks{} })
	_ = prev
}
