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
)

// TestFault_RefusesAReadWithTheStatusItWasGiven is the fault that separates a
// retry from a dead letter. The handshake reads a status code off the wire
// and decides from it alone, so the instrument has to be able to produce any
// of them rather than one stand-in refusal.
func TestFault_RefusesAReadWithTheStatusItWasGiven(t *testing.T) {
	t.Run("control: the honest server answers the read with the value", func(t *testing.T) {
		_, client, v := startServer(t, "Flag", true)
		id := nodeID(t, v)

		if got := readStatus(t, client, id); got != ua.StatusOK {
			t.Fatalf("the honest server answered %v, want OK", got)
		}
		if got := readValue(t, client, id); got != true {
			t.Errorf("read back %v, want true", got)
		}
	})

	t.Run("the machine answers with the status it was given", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status ua.StatusCode
		}{
			{"the machine may answer next time", ua.StatusBadCommunicationError},
			{"it never will", ua.StatusBadNodeIDUnknown},
		} {
			status := tc.status
			t.Run(tc.name, func(t *testing.T) {
				srv, client, v := startServer(t, "Flag", true)
				f := srv.RefuseRead(v, status)
				id := nodeID(t, v)

				if got := readStatus(t, client, id); got != status {
					t.Errorf("the read answered %v, want %v", got, status)
				}
				if n := v.Reads(); n == 0 {
					t.Errorf("the read never reached the node, so this test proved nothing")
				}
				if !f.Fired() {
					t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
				}
			})
		}
	})
}

// TestFault_FlapsTheValue is what a single-read check mistakes for a
// confirmation: the machine with two controllers, where the flag is up when
// the script looks and down when it looks again. Until the rewrite this fault
// had no test at all.
func TestFault_FlapsTheValue(t *testing.T) {
	t.Run("control: the honest server answers the same value every time", func(t *testing.T) {
		_, client, v := startServer(t, "Loaded", true)
		id := nodeID(t, v)

		for i := 0; i < 4; i++ {
			if got := readValue(t, client, id); got != true {
				t.Fatalf("read %v on look %d, want true every time", got, i+1)
			}
		}
	})

	t.Run("the machine alternates on every read", func(t *testing.T) {
		srv, client, v := startServer(t, "Loaded", true)
		f := srv.FlapValue(v, false)
		id := nodeID(t, v)

		// The real value is true; the fault's is false. Every faulted read
		// must differ from the one before it, and the first must be the
		// fault's, which is what makes a single read a lie.
		want := []any{false, true, false, true}
		for i, w := range want {
			if got := readValue(t, client, id); got != w {
				t.Fatalf("read %v on look %d, want %v: the node is not alternating", got, i+1, w)
			}
		}
		if n := v.Reads(); n < len(want) {
			t.Errorf("only %d reads reached the node, want at least %d", n, len(want))
		}
		if n := f.Count(); n < len(want) {
			t.Errorf("the fault took effect %d times, want at least %d: it is not firing on every read", n, len(want))
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this test proved nothing")
		}
	})
}

// TestFault_AnswersLate is the fault a deadline exists for. The machine is
// answering correctly the whole time; it is only slow, which is why a client
// that waits on the socket rather than on its own clock never recovers.
//
// Both halves are here, because until the rewrite the write path had no case
// for this fault at all: a write was applied and acknowledged on the spot
// while this machine was supposed to be slow.
func TestFault_AnswersLate(t *testing.T) {
	t.Run("control: the honest server answers promptly", func(t *testing.T) {
		_, client, v := startServer(t, "Flag", true)
		id := nodeID(t, v)

		start := time.Now()
		if got := readValue(t, client, id); got != true {
			t.Fatalf("read back %v, want true", got)
		}
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Errorf("the honest read took %v: the control must be prompt or the late fault proves nothing", elapsed)
		}
	})

	t.Run("a read answers after the caller has gone", func(t *testing.T) {
		srv, client, v := startServer(t, "Flag", true)
		f := srv.AnswerLate(v, 600*time.Millisecond)
		id := nodeID(t, v)

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		_, err := readWithContext(ctx, client, id)

		if err == nil {
			t.Errorf("the read returned in %v, but the machine was %v late and the caller allowed 150ms",
				time.Since(start), 600*time.Millisecond)
		} else if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the read failed with %v, want the caller's own deadline", err)
		}
		if n := v.Reads(); n == 0 {
			t.Errorf("the read never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}
	})

	t.Run("a write lands only after the delay", func(t *testing.T) {
		srv, client, v := startServer(t, "Setpoint", int32(7))
		f := srv.AnswerLate(v, 600*time.Millisecond)
		id := nodeID(t, v)

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		if _, err := writeWithContext(ctx, client, id, int32(4200)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the write answered with %v, want the caller's own deadline: the machine was 600ms late on the write too", err)
		}

		// The machine is working; it is only slow. The answer the caller
		// stopped waiting for still has to land, which is what separates
		// "late" from "never".
		time.Sleep(700 * time.Millisecond)
		if got := readValue(t, client, id); got != int32(4200) {
			t.Errorf("read back %v, want 4200: a late write is still supposed to be applied", got)
		}
		if n := v.Writes(); n == 0 {
			t.Errorf("the write never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}
	})
}

// TestFault_NeverAnswers is the half-state: the request reached the machine
// and nothing came back, so the caller cannot know whether it landed. Only
// the caller's own clock ends this.
//
// Both halves are here, because until the rewrite the write path had no case
// for this fault at all: a write was applied and acknowledged on the spot by
// a machine that was supposed to be silent.
func TestFault_NeverAnswers(t *testing.T) {
	t.Run("control: the honest server answers promptly", func(t *testing.T) {
		_, client, v := startServer(t, "Flag", true)
		id := nodeID(t, v)

		if got := readValue(t, client, id); got != true {
			t.Fatalf("read back %v, want true", got)
		}
	})

	t.Run("a read never comes back", func(t *testing.T) {
		srv, client, v := startServer(t, "Flag", true)
		f := srv.NeverAnswer(v)
		id := nodeID(t, v)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { _, err := readWithContext(ctx, client, id); done <- err }()

		select {
		case err := <-done:
			if err == nil {
				t.Error("the read returned a value from a machine that never answered")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the read did not come back at all: the caller is waiting on the socket, not on its own clock")
		}
		if n := v.Reads(); n == 0 {
			t.Errorf("the read never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}
	})

	t.Run("a write never comes back, and never lands", func(t *testing.T) {
		srv, client, v := startServer(t, "Setpoint", int32(7))
		f := srv.NeverAnswer(v)
		id := nodeID(t, v)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if _, err := writeWithContext(ctx, client, id, int32(4200)); err == nil {
			t.Fatal("the write was answered by a machine that never answers")
		}

		// The half-state an error handler exists for: the caller cannot find
		// out whether the write landed. The server-side truth can, and it
		// must say it did not.
		if got := v.Value(); got != int32(7) {
			t.Errorf("the node holds %v server-side, want the original 7: the write was applied by a machine that never answered", got)
		}
		if n := v.Writes(); n == 0 {
			t.Errorf("the write never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Errorf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}
	})
}

// TestFault_RaisesTheFlagLate is the ordinary case wait_until exists for: the
// machine confirms, just not yet. A test whose flag is already up when it
// looks never exercises the waiting at all.
func TestFault_RaisesTheFlagLate(t *testing.T) {
	t.Run("control: the honest server's flag is up straight away", func(t *testing.T) {
		_, client, v := startServer(t, "Loaded", true)
		id := nodeID(t, v)

		if got := readValue(t, client, id); got != true {
			t.Fatalf("the flag read %v straight away, want true", got)
		}
	})

	t.Run("the flag comes up only when it comes up", func(t *testing.T) {
		srv, client, v := startServer(t, "Loaded", true)
		f := srv.RaiseFlagLate(v, false, 400*time.Millisecond)
		id := nodeID(t, v)

		if got := readValue(t, client, id); got != false {
			t.Fatalf("the flag read %v straight away, want false: it was supposed to be held down", got)
		}
		if n := v.Reads(); n == 0 {
			t.Errorf("the read never reached the node, so this test proved nothing")
		}
		if !f.Fired() {
			t.Fatalf("the fault never fired, so this proved nothing; it took effect %d times", f.Count())
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if readValue(t, client, id) == true {
				return // it came up, late, which is the whole point
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Error("the flag never came up, so this fault is a permanent refusal rather than a late answer")
	})
}

// TestFault_FlapsIndependentlyPerNode pins the phase FlapValue alternates on
// as the fault's own. The phase used to live on the namespace, one bool for
// every fault in it, so two flapping nodes read alternately did not alternate:
// they toggled the same phase in lockstep, one node answering the fault's
// value every time and the other the real value every time. Neither node
// flapped, which reads as two machines, not one machine flapping twice.
func TestFault_FlapsIndependentlyPerNode(t *testing.T) {
	srv, client, left := startServer(t, "Left", int32(1))
	right := srv.AddVariable("Right", int32(2))
	fLeft := srv.FlapValue(left, int32(111))
	fRight := srv.FlapValue(right, int32(222))
	idLeft, idRight := nodeID(t, left), nodeID(t, right)

	// Look at Left, then Right, then Left again, the way a script watching
	// two controllers does. Each node must alternate on its own reads: the
	// first faulted read of a node answers the fault's value, the second
	// the real one.
	wantLeft := []any{int32(111), int32(1), int32(111)}
	wantRight := []any{int32(222), int32(2), int32(222)}
	for i := range wantLeft {
		if got := readValue(t, client, idLeft); got != wantLeft[i] {
			t.Fatalf("Left read %v on look %d, want %v: the node is not alternating on its own reads", got, i+1, wantLeft[i])
		}
		if got := readValue(t, client, idRight); got != wantRight[i] {
			t.Fatalf("Right read %v on look %d, want %v: reading another node took this one's phase, so this node is not alternating", got, i+1, wantRight[i])
		}
	}
	if !fLeft.Fired() || !fRight.Fired() {
		t.Errorf("Left fired %d times and Right %d: both faults must fire for this to prove anything", fLeft.Count(), fRight.Count())
	}
	if n := left.Reads(); n < len(wantLeft) {
		t.Errorf("only %d reads reached Left, want at least %d", n, len(wantLeft))
	}
	if n := right.Reads(); n < len(wantRight) {
		t.Errorf("only %d reads reached Right, want at least %d", n, len(wantRight))
	}
}
