// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package faultopcua_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// TestFault_RefusesAReadWithTheStatusItWasGiven is the fault that separates a
// retry from a dead letter. The handshake reads a status code off the wire
// and decides from it alone, so the instrument has to be able to produce any
// of them rather than one stand-in refusal.
func TestFault_RefusesAReadWithTheStatusItWasGiven(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status ua.StatusCode
	}{
		{"the machine may answer next time", ua.StatusBadCommunicationError},
		{"it never will", ua.StatusBadNodeIDUnknown},
	} {
		status := tc.status
		t.Run(tc.name, func(t *testing.T) {
			srv, client, nodeID := startServer(t, "Flag", true)
			srv.Inject(faultopcua.Fault{Node: "Flag", Kind: faultopcua.RefuseRead, Status: status})

			if got := readStatus(t, client, nodeID); got != status {
				t.Errorf("the read answered %v, want %v", got, status)
			}
			if !srv.Fired(faultopcua.RefuseRead, "Flag") {
				t.Errorf("the fault never fired, so this proved nothing; firings: %v", srv.Firings())
			}
		})
	}
}

// TestFault_AnswersLate is the fault a deadline exists for. The machine is
// answering correctly the whole time; it is only slow, which is why a client
// that waits on the socket rather than on its own clock never recovers.
func TestFault_AnswersLate(t *testing.T) {
	srv, client, nodeID := startServer(t, "Flag", true)
	srv.Inject(faultopcua.Fault{Node: "Flag", Kind: faultopcua.AnswerLate, Delay: 600 * time.Millisecond})

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := readWithContext(ctx, client, nodeID)

	if err == nil {
		t.Errorf("the read returned in %v, but the machine was %v late and the caller allowed 150ms",
			time.Since(start), 600*time.Millisecond)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the read failed with %v, want the caller's own deadline", err)
	}
	if !srv.Fired(faultopcua.AnswerLate, "Flag") {
		t.Errorf("the fault never fired, so this proved nothing; firings: %v", srv.Firings())
	}
}

// TestFault_NeverAnswers is the half-state: the request reached the machine
// and nothing came back, so the caller cannot know whether it landed. Only
// the caller's own clock ends this.
func TestFault_NeverAnswers(t *testing.T) {
	srv, client, nodeID := startServer(t, "Flag", true)
	srv.Inject(faultopcua.Fault{Node: "Flag", Kind: faultopcua.NeverAnswer})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := readWithContext(ctx, client, nodeID); done <- err }()

	select {
	case err := <-done:
		if err == nil {
			t.Error("the read returned a value from a machine that never answered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read did not come back at all: the caller is waiting on the socket, not on its own clock")
	}
	if !srv.Fired(faultopcua.NeverAnswer, "Flag") {
		t.Errorf("the fault never fired, so this proved nothing; firings: %v", srv.Firings())
	}
}

// TestFault_RaisesTheFlagLate is the ordinary case wait_until exists for: the
// machine confirms, just not yet. A test whose flag is already up when it
// looks never exercises the waiting at all.
func TestFault_RaisesTheFlagLate(t *testing.T) {
	srv, client, nodeID := startServer(t, "Loaded", true)
	srv.Inject(faultopcua.Fault{
		Node:  "Loaded",
		Kind:  faultopcua.RaiseFlagLate,
		After: 400 * time.Millisecond,
		Until: false,
	})

	if got := readValue(t, client, nodeID); got != false {
		t.Fatalf("the flag read %v straight away, want false: it was supposed to be held down", got)
	}
	if !srv.Fired(faultopcua.RaiseFlagLate, "Loaded") {
		t.Fatalf("the fault never fired, so this proved nothing; firings: %v", srv.Firings())
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if readValue(t, client, nodeID) == true {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("the flag never came up, so this fault is a permanent refusal rather than a late answer")
}
