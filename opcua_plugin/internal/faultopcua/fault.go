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

package faultopcua

import (
	"fmt"
	"sync"
	"time"

	"github.com/gopcua/opcua/ua"
)

// A Fault is one armed misbehavior. The method that armed it returns one, and the
// test asserts on it: a fault that silently failed to inject reads exactly like a
// fault the client handled, and a test that cannot tell those apart proves nothing.
type Fault struct {
	// kind is which way this fault makes the machine misbehave. The namespace
	// is the only place it is interpreted.
	kind faultKind

	// node is the node's identifier within the server's namespace, the form
	// a NodeID's StringID carries -- "Setpoint", not "ns=2;s=Setpoint".
	node string

	// status is the refusal RefuseWrite and RefuseRead answer with.
	status ua.StatusCode

	// delay is how much later than it should AnswerLate answer.
	delay time.Duration

	// after is how long RaiseFlagLate holds the node at until, measured from
	// the moment the fault was armed.
	after time.Duration

	// until is the value RaiseFlagLate serves while it is holding, and the
	// value FlapValue alternates the node with.
	until any

	// flap is this fault's own phase, which FlapValue flips on every read
	// that meets it. It lives on the fault and not on the namespace because
	// two flapping nodes are two machines misbehaving independently: a
	// shared phase would make them toggle in lockstep -- one answering the
	// fault's value every time and the other the real value every time --
	// and neither node would alternate.
	flap bool

	// armed is when the arm method took this fault, which is what after
	// counts from.
	armed time.Time

	// ns disarms the fault: it is the namespace the fault's node lives in.
	ns *faultNamespace

	mu   sync.Mutex
	hits int
}

// Fired reports whether the fault took effect at least once.
func (f *Fault) Fired() bool { return f.Count() > 0 }

// Count is how many times the fault took effect. A flap that fired once and a flap
// that fired on every read are different machines.
func (f *Fault) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

// Disarm stops the fault for every request that arrives afterwards, and frees
// the node for another fault. A request the fault is already holding -- a
// NeverAnswer or an AnswerLate in progress -- is not let go: it stays held
// until its delay ends or the server closes. The fault's record is kept, so a
// scenario can disarm and still prove the fault fired earlier. Disarming twice
// is safe.
func (f *Fault) Disarm() {
	if f.ns == nil {
		return // never armed: the arm method refused it
	}
	f.ns.mu.Lock()
	defer f.ns.mu.Unlock()
	// Only let go of the node if this fault is still the one holding it:
	// once it is disarmed another fault can be armed on the node, and a
	// second Disarm of this one must not disarm the newcomer.
	if f.ns.faults[f.node] == f {
		delete(f.ns.faults, f.node)
	}
}

// tookEffect is the evidence. Called once for every request the fault answered,
// from the namespace, at the moment the misbehavior happens: a fault whose
// branch ran without this is a fault the test cannot distinguish from one the
// client handled.
func (f *Fault) tookEffect() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
}

// toggle flips this fault's flap phase, under the fault's own lock, because a
// background read -- gopcua answers monitored items from its own goroutine --
// can race the service loop for it.
func (f *Fault) toggle() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flap = !f.flap
	return f.flap
}

// --- the eight faults, one per way a real machine goes wrong ---
//
// Each takes the node it is armed against and nothing but its own parameters. A
// fault cannot be armed against a node this server does not have, because there is
// no way to name one, and it cannot be armed without the value it needs, because
// there is no field to leave at its zero value.

// AcceptWriteNeverApply answers a write with Good and leaves the node holding its
// old value. Some machine controllers do this when they are in a state that does
// not take the parameter, without saying so, and it is the reason the handshake
// reads a value back after writing it.
func (s *Server) AcceptWriteNeverApply(v *Variable) *Fault {
	return s.arm(v, &Fault{kind: faultAcceptWriteNeverApply})
}

// RefuseWrite answers a write with status and changes nothing. It is the commonest
// persistent fault a plant produces, and the one a simulator never produces,
// because a simulator takes everything.
func (s *Server) RefuseWrite(v *Variable, status ua.StatusCode) *Fault {
	s.refuseASuccessStatus("RefuseWrite", status)
	return s.arm(v, &Fault{kind: faultRefuseWrite, status: status})
}

// RefuseRead answers a read with status instead of a value. The status decides what
// the client should do, so this is the fault that separates "try again later" from
// "park it".
func (s *Server) RefuseRead(v *Variable, status ua.StatusCode) *Fault {
	s.refuseASuccessStatus("RefuseRead", status)
	return s.arm(v, &Fault{kind: faultRefuseRead, status: status})
}

// FlapValue alternates the node between its real value and other on every read. A
// flag that is up when the script looks and down when it looks again is how a
// machine with two controllers behaves, and it is what a single-read check mistakes
// for a confirmation.
func (s *Server) FlapValue(v *Variable, other any) *Fault {
	return s.arm(v, &Fault{kind: faultFlapValue, until: other})
}

// AnswerLate answers correctly, delay too late, on reads and on writes alike. It
// tests a deadline: the client must stop waiting and its cleanup must still run,
// while the machine is answering normally the whole time. A delay of zero is
// refused: a fault that delays nothing answers the way an honest machine does
// while reporting that it fired.
func (s *Server) AnswerLate(v *Variable, delay time.Duration) *Fault {
	s.t.Helper()
	if delay <= 0 {
		s.t.Fatalf("faultopcua: AnswerLate: delay must be positive, got %v: "+
			"a fault that delays nothing answers the way an honest machine does while reporting that it fired", delay)
		return &Fault{kind: faultAnswerLate}
	}
	return s.arm(v, &Fault{kind: faultAnswerLate, delay: delay})
}

// NeverAnswer does not answer at all, on reads or writes, until the server closes.
// A client that waits on a socket rather than on its own clock hangs here forever,
// which is the failure this fault exists to find.
func (s *Server) NeverAnswer(v *Variable) *Fault {
	return s.arm(v, &Fault{kind: faultNeverAnswer})
}

// RaiseFlagLate holds the node at until for after, then lets the real value
// through. A machine that confirms late rather than never is the ordinary case a
// wait exists for, and the one a test using a flag that is already up never
// exercises. An after of zero is refused: a fault that is over before it is armed
// records nothing, and reads as a fault that never fired.
func (s *Server) RaiseFlagLate(v *Variable, until any, after time.Duration) *Fault {
	s.t.Helper()
	if after <= 0 {
		s.t.Fatalf("faultopcua: RaiseFlagLate: after must be positive, got %v: "+
			"a fault that is over before it is armed records nothing and reads as a fault that never fired", after)
		return &Fault{kind: faultRaiseFlagLate}
	}
	return s.arm(v, &Fault{kind: faultRaiseFlagLate, until: until, after: after})
}

// DropSession closes the connection underneath the client at the moment the node is
// touched. It is the half-state an error handler exists for: the write may have
// landed, and the read-back that would have said so can no longer be asked.
func (s *Server) DropSession(v *Variable) *Fault {
	return s.arm(v, &Fault{kind: faultDropSession})
}

// arm fills in where the fault lives, publishes it against v's node, and
// returns it. Registering on the node's own namespace, rather than on the
// server's, is what makes the handle the whole truth about where the fault
// lives: there is no name to typo and no node to miss.
//
// The fault must arrive complete, and every arm method builds it that way:
// the moment it is in the namespace's map the server's request handlers can
// find it, so a parameter set after arm returns is a parameter a request can
// miss. A RefuseWrite whose status was set late would answer Good to the
// requests in between, reaching around refuseASuccessStatus, and would be a
// data race on the field besides.
//
// A node holds one armed fault, and arming a second fails the test. Replacing
// the first silently would leave its *Fault in the caller's hand, never firing
// again, with Fired() answering false and no diagnostic. Disarm the first
// fault before arming another.
func (s *Server) arm(v *Variable, f *Fault) *Fault {
	s.t.Helper()
	if v == nil || v.ns == nil {
		s.t.Fatalf("faultopcua: cannot arm a fault against a node this server does not have")
		return f
	}
	f.node = v.id.StringID()
	f.ns = v.ns
	f.armed = time.Now()
	v.ns.mu.Lock()
	defer v.ns.mu.Unlock()
	if other := v.ns.faults[f.node]; other != nil {
		s.t.Fatalf("faultopcua: cannot arm %s against %q: %s is already armed there and would be silently replaced, so the first fault would stop firing with no diagnostic -- Disarm the first fault before arming another",
			f.kind, f.node, other.kind)
		return f
	}
	v.ns.faults[f.node] = f
	return f
}

// String names a fault the way it is armed, which is how the guards speak: a
// refusal that did not say which faults it was about would leave a scenario
// to find out by elimination.
func (k faultKind) String() string {
	switch k {
	case faultAcceptWriteNeverApply:
		return "AcceptWriteNeverApply"
	case faultRefuseWrite:
		return "RefuseWrite"
	case faultRefuseRead:
		return "RefuseRead"
	case faultFlapValue:
		return "FlapValue"
	case faultAnswerLate:
		return "AnswerLate"
	case faultNeverAnswer:
		return "NeverAnswer"
	case faultDropSession:
		return "DropSession"
	case faultRaiseFlagLate:
		return "RaiseFlagLate"
	default:
		return fmt.Sprintf("faultKind(%d)", uint8(k))
	}
}

// statusSeverity is the top two bits of an OPC UA status code, which carry the
// severity. Both clear means Good: the operation succeeded.
const statusSeverity ua.StatusCode = 0xC0000000

// refuseASuccessStatus fails the test rather than arming a refusal that answers
// with success. Such a refusal would answer Good, record a firing and report
// fired: the package's own evidence corroborating an honest answer on the fault
// a plant produces most.
func (s *Server) refuseASuccessStatus(method string, status ua.StatusCode) {
	s.t.Helper()
	if status&statusSeverity == 0 {
		s.t.Fatalf("faultopcua: %s: %v is a Good status, which means success; "+
			"a fault that refuses with it answers the way an honest machine does "+
			"while reporting that it misbehaved -- arm it with a status that means failure", method, status)
	}
}
