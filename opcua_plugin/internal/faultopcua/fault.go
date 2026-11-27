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

	// armed is when the arm method took this fault, which is what after
	// counts from.
	armed time.Time

	// ns disarms the fault: it is the namespace the fault's node lives in.
	ns *faultNamespace

	mu       sync.Mutex
	hits     int
	disarmed bool
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

// Disarm stops the fault, leaving an honest machine. Its record is kept, so a
// scenario can disarm and still prove the fault fired earlier.
func (f *Fault) Disarm() {
	f.mu.Lock()
	f.disarmed = true
	f.mu.Unlock()
	if f.ns == nil {
		return
	}
	f.ns.mu.Lock()
	defer f.ns.mu.Unlock()
	// Only let go of the node if this fault is still the one holding it:
	// arming another fault on the same node takes the node over, and a
	// disarm arriving late must not disarm the newcomer.
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

// --- the eight faults, one per way a real machine goes wrong ---
//
// Each takes the node it is armed against and nothing but its own parameters. A
// fault cannot be armed against a node this server does not have, because there is
// no way to name one, and it cannot be armed without the value it needs, because
// there is no field to leave at its zero value.

// AcceptWriteNeverApply answers a write with Good and leaves the node holding its
// old value. TRUMPF's press does this, and it is the reason the handshake reads a
// value back after writing it.
func (s *Server) AcceptWriteNeverApply(v *Variable) *Fault {
	return s.arm(v, faultAcceptWriteNeverApply)
}

// RefuseWrite answers a write with status and changes nothing. It is the commonest
// persistent fault a plant produces, and the one a simulator never produces,
// because a simulator takes everything.
func (s *Server) RefuseWrite(v *Variable, status ua.StatusCode) *Fault {
	s.refuseASuccessStatus("RefuseWrite", status)
	f := s.arm(v, faultRefuseWrite)
	f.status = status
	return f
}

// RefuseRead answers a read with status instead of a value. The status decides what
// the client should do, so this is the fault that separates "try again later" from
// "park it".
func (s *Server) RefuseRead(v *Variable, status ua.StatusCode) *Fault {
	s.refuseASuccessStatus("RefuseRead", status)
	f := s.arm(v, faultRefuseRead)
	f.status = status
	return f
}

// FlapValue alternates the node between its real value and other on every read. A
// flag that is up when the script looks and down when it looks again is how a
// machine with two controllers behaves, and it is what a single-read check mistakes
// for a confirmation.
func (s *Server) FlapValue(v *Variable, other any) *Fault {
	f := s.arm(v, faultFlapValue)
	f.until = other
	return f
}

// AnswerLate answers correctly, delay too late, on reads and on writes alike. It
// tests a deadline: the client must stop waiting and its cleanup must still run,
// while the machine is answering normally the whole time.
func (s *Server) AnswerLate(v *Variable, delay time.Duration) *Fault {
	f := s.arm(v, faultAnswerLate)
	f.delay = delay
	return f
}

// NeverAnswer does not answer at all, on reads or writes, until the server closes.
// A client that waits on a socket rather than on its own clock hangs here forever,
// which is the failure this fault exists to find.
func (s *Server) NeverAnswer(v *Variable) *Fault {
	return s.arm(v, faultNeverAnswer)
}

// RaiseFlagLate holds the node at until for after, then lets the real value
// through. A machine that confirms late rather than never is the ordinary case a
// wait exists for, and the one a test using a flag that is already up never
// exercises. An after of zero is refused: a fault that is over before it is armed
// records nothing, and reads as a fault that never fired.
func (s *Server) RaiseFlagLate(v *Variable, until any, after time.Duration) *Fault {
	if after <= 0 {
		s.t.Fatalf("faultopcua: RaiseFlagLate: after must be positive, got %v: "+
			"a fault that is over before it is armed records nothing and reads as a fault that never fired", after)
	}
	f := s.arm(v, faultRaiseFlagLate)
	f.until = until
	f.after = after
	return f
}

// DropSession closes the connection underneath the client at the moment the node is
// touched. It is the half-state an error handler exists for: the write may have
// landed, and the read-back that would have said so can no longer be asked.
func (s *Server) DropSession(v *Variable) *Fault {
	return s.arm(v, faultDropSession)
}

// arm builds the fault, registers it against v's node, and returns it. Registering
// on the node's own namespace, rather than on the server's, is what makes the
// handle the whole truth about where the fault lives: there is no name to typo and
// no node to miss.
func (s *Server) arm(v *Variable, kind faultKind) *Fault {
	if v == nil || v.ns == nil {
		s.t.Fatalf("faultopcua: cannot arm a fault against a node this server does not have")
	}
	f := &Fault{
		kind:  kind,
		node:  v.id.StringID(),
		ns:    v.ns,
		armed: time.Now(),
	}
	v.ns.mu.Lock()
	v.ns.faults[f.node] = f
	v.ns.mu.Unlock()
	return f
}

// statusSeverity is the top two bits of an OPC UA status code, which carry the
// severity. Both clear means Good: the operation succeeded.
const statusSeverity ua.StatusCode = 0xC0000000

// refuseASuccessStatus fails the test rather than arming a refusal that answers
// with success. RefuseWrite without this guard answered Good, recorded a firing,
// and reported fired -- the package's own evidence machinery corroborating the
// wrong behavior on the fault a plant produces most.
func (s *Server) refuseASuccessStatus(method string, status ua.StatusCode) {
	if status&statusSeverity == 0 {
		s.t.Fatalf("faultopcua: %s: %v is a Good status, which means success; "+
			"a fault that refuses with it answers the way an honest machine does "+
			"while reporting that it misbehaved -- arm it with a status that means failure", method, status)
	}
}
