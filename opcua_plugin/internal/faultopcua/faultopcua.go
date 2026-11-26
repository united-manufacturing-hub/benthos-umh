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
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
)

// Kind names one way a machine can misbehave. The names are the plant's words
// for what went wrong, not the protocol's.
type Kind string

const (
	// AcceptWriteNeverApply answers a write with Good and leaves the node
	// holding its old value. TRUMPF's press does this, and it is the reason
	// the handshake reads a value back after writing it.
	AcceptWriteNeverApply Kind = "accept_write_never_apply"

	// RefuseWrite answers a write with Fault.Status and changes nothing.
	// It is the commonest persistent fault a plant produces -- a value the
	// machine will not take -- and the one a simulator never produces,
	// because a simulator takes everything.
	RefuseWrite Kind = "refuse_write"

	// FlapValue alternates the node between its real value and Fault.Until
	// on every read. A flag that is up when the script looks and down when
	// it looks again is how a machine with two controllers behaves, and it
	// is what a single-read check mistakes for a confirmation.
	FlapValue Kind = "flap_value"

	// RefuseRead answers a read with Fault.Status instead of a value. The
	// status decides what the client should do, so this is the fault that
	// separates "try again later" from "park it": the same shape of answer
	// carries BadCommunicationError and BadNodeIdUnknown alike.
	RefuseRead Kind = "refuse_read"

	// AnswerLate answers correctly, Fault.Delay too late. It is the fault
	// that tests a deadline: the handshake must stop waiting and the cleanup
	// must still run, while the machine is answering normally the whole time.
	AnswerLate Kind = "answer_late"

	// NeverAnswer does not answer at all. A client that waits on a socket
	// rather than on its own clock hangs here forever, which is the failure
	// this fault exists to find.
	NeverAnswer Kind = "never_answer"

	// DropSession closes the connection underneath the client at the moment
	// the node is touched. It is the half-state the handshake's onError block
	// exists for: the write may have landed and the read-back that would have
	// said so can no longer be asked.
	DropSession Kind = "drop_session"

	// RaiseFlagLate holds the node at Fault.Until for Fault.After, then lets
	// the real value through. A machine that confirms late rather than never
	// is the ordinary case wait_until exists for, and the one a test using a
	// flag that is already up never exercises.
	RaiseFlagLate Kind = "raise_flag_late"
)

// knownKinds is every kind Inject will accept. A kind absent here arms a
// fault that nothing in the dispatch switch matches.
var knownKinds = map[Kind]bool{
	AcceptWriteNeverApply: true,
	RefuseWrite:           true,
	FlapValue:             true,
	RefuseRead:            true,
	AnswerLate:            true,
	NeverAnswer:           true,
	DropSession:           true,
	RaiseFlagLate:         true,
}

// Fault is one misbehavior, armed against one node. Which fields a fault
// reads depends on its Kind; each Kind's comment says so.
type Fault struct {
	// Node is the node's string identifier within this server's namespace,
	// as passed to AddNode -- "Setpoint", not "ns=2;s=Setpoint".
	Node string
	Kind Kind

	// Status is the refusal RefuseRead answers with.
	Status ua.StatusCode

	// Delay is how much later than it should AnswerLate answers.
	Delay time.Duration

	// After is how long RaiseFlagLate holds the node at Until, measured from
	// the moment the fault was armed.
	After time.Duration

	// Until is the value RaiseFlagLate serves while it is holding.
	Until any

	// armed is when Inject took this fault, which is what After counts from.
	armed time.Time
}

// Firing records that a fault actually took effect, which is the only
// evidence that a test exercised what it claims to exercise. A test asserting
// the client's reaction without asserting a Firing cannot tell a handled
// fault from a fault that never happened.
type Firing struct {
	Kind Kind
	Node string
	At   time.Time
}

func (f Firing) String() string {
	return fmt.Sprintf("%s on %s at %s", f.Kind, f.Node, f.At.Format(time.RFC3339Nano))
}

// Server is a running OPC UA server with a programmable set of faults.
// The zero value is not usable; call New.
type Server struct {
	srv   *server.Server
	ns    *faultNamespace
	proxy *proxy
	host  string
	port  int // where the OPC UA server listens, behind the proxy
}

// New builds a server with no faults armed: an honest machine, which is the
// control every fault test needs beside it. The port is taken from the
// operating system here rather than in Start, so several servers can run at
// once without a test picking a number and colliding with another test, and
// so Endpoint answers before the server is listening.
func New() (*Server, error) {
	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("faultopcua: no free port: %w", err)
	}
	s := &Server{host: "127.0.0.1", port: port}
	s.srv = server.New(
		server.EndPoint(s.host, s.port),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)
	s.ns = &faultNamespace{
		NodeNameSpace: server.NewNodeNameSpace(s.srv, "faultopcua"),
		faults:        map[string]Fault{},
		closed:        make(chan struct{}),
	}
	s.srv.AddNamespace(s.ns)

	// Every client reaches the server through the proxy, so a session drop
	// is a real socket close rather than a polite error, and there is one
	// code path whether or not a drop is armed.
	px, err := newProxy(fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		return nil, fmt.Errorf("faultopcua: proxy: %w", err)
	}
	s.proxy = px
	s.ns.onDrop = func() { px.cut() }
	return s, nil
}

// Cuts reports how many connections a session drop actually closed. A test
// that only watched the client fail could not tell a drop that reached a
// socket from one that never fired.
func (s *Server) Cuts() int { return s.proxy.cutCount() }

// AddNode gives the server a readable and writable variable node holding
// value. The node's identifier is its name, so AddNode("Setpoint", 0) is
// addressable as ns=<NamespaceID()>;s=Setpoint.
func (s *Server) AddNode(name string, value any) {
	s.ns.AddNewVariableStringNode(name, value)
}

// Inject arms a fault, and panics if the node does not exist.
//
// The panic is the point. A fault matches by node name, and the read side
// answers before the real namespace is consulted, so a fault armed against a
// name nothing added produces its whole observable behavior against a node
// that is not there: the ghost refuses, stalls, flaps and drops exactly as
// instructed, Fired reports true, and the scenario passes end to end. One
// typo between AddNode and Inject is enough. Failing here, loudly, is the
// only place the mistake is still visible.
func (s *Server) Inject(f Fault) {
	if s.ns.Node(ua.NewStringNodeID(s.ns.ID(), f.Node)) == nil {
		panic(fmt.Sprintf(
			"faultopcua: no node %q to arm %s against -- add it with AddNode first", f.Node, f.Kind))
	}
	// And the field next to it. A misspelled Kind arms a fault that matches
	// nothing in the switch: the node answers honestly, nothing is recorded,
	// and the scenario author is told "the fault never fired, so this test
	// proved nothing" -- true, and pointing at the wrong thing entirely. The
	// argument for panicking on a bad node is exactly as good here.
	if !knownKinds[f.Kind] {
		panic(fmt.Sprintf("faultopcua: %q is not a fault kind", f.Kind))
	}
	ns := s.ns
	f.armed = time.Now()
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.faults[f.Node] = f
}

// Clear disarms every fault, leaving an honest machine. Firings are kept, so
// a scenario can clear a fault and still prove it fired earlier.
func (s *Server) Clear() {
	ns := s.ns
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.faults = map[string]Fault{}
}

// Firings reports the faults that took effect, oldest first.
func (s *Server) Firings() []Firing {
	ns := s.ns
	ns.mu.Lock()
	defer ns.mu.Unlock()
	out := make([]Firing, len(ns.firings))
	copy(out, ns.firings)
	return out
}

// Fired reports whether a fault of this kind took effect on this node. It is
// the assertion a fault test makes beside the one about the client.
func (s *Server) Fired(kind Kind, node string) bool {
	for _, f := range s.Firings() {
		if f.Kind == kind && f.Node == node {
			return true
		}
	}
	return false
}

// Dials reports how many connections clients have opened to this server.
// A client that reconnects after a session drop shows up as another dial,
// which is the only way to see a reconnect from outside the client.
func (s *Server) Dials() int { return s.proxy.dialCount() }

// NamespaceID is the namespace the server assigned to our nodes. The server
// assigns it at AddNamespace, so a node identifier cannot be built until the
// namespace exists.
func (s *Server) NamespaceID() uint16 { return s.ns.ID() }

// NodeID renders a node's full identifier, the form a client config carries.
func (s *Server) NodeID(name string) string {
	return fmt.Sprintf("ns=%d;s=%s", s.NamespaceID(), name)
}

// Endpoint is the URL a client connects to: the proxy's address, not the
// server's. Valid as soon as New has returned.
func (s *Server) Endpoint() string {
	return fmt.Sprintf("opc.tcp://%s:%d", s.host, s.proxy.port())
}

// Start listens and serves until ctx is canceled or Close is called.
func (s *Server) Start(ctx context.Context) error {
	if err := s.srv.Start(ctx); err != nil {
		return fmt.Errorf("faultopcua: start: %w", err)
	}
	return nil
}

// Close stops the server.
// Close stops the server and releases any read a fault is holding.
func (s *Server) Close() error {
	s.ns.release()
	if s.proxy != nil {
		s.proxy.close()
	}
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
}

// release unblocks NeverAnswer and shortens AnswerLate. It tolerates being
// called twice, because a test that closes in a defer and again by hand
// should not panic in its teardown.
func (ns *faultNamespace) release() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	select {
	case <-ns.closed:
	default:
		close(ns.closed)
	}
}

// faultNamespace is an ordinary gopcua namespace with the faults wrapped
// around its two attribute entries. Everything a client does to a node
// arrives at Attribute or SetAttribute, so a fault armed here reaches every
// read and every write without the service layer knowing about it.
type faultNamespace struct {
	*server.NodeNameSpace

	mu      sync.Mutex
	faults  map[string]Fault
	firings []Firing

	// closed releases anything NeverAnswer is holding, so a stuck read ends
	// with the server rather than outliving the test that armed it.
	closed chan struct{}

	// onDrop cuts the connections underneath the client. The proxy keeps the
	// count, because only the proxy can publish it before the close it
	// records becomes visible to the client.
	onDrop func()

	// flap alternates FlapValue between the real value and the fault's.
	flap bool
}

// drop cuts the client's connection. The proxy counts what it closed.
func (ns *faultNamespace) drop() {
	if ns.onDrop == nil {
		return
	}
	ns.onDrop()
}

// Attribute is the read side.
//
// A read reaches here for every attribute, not only the value, so a fault
// that refused all of them would also refuse the browse and type reads a
// client makes while connecting, and the test would fail before reaching
// what it meant to test. Faults therefore apply to the value attribute only.
func (ns *faultNamespace) Attribute(id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	f, armed := ns.faultFor(id)
	if !armed || attr != ua.AttributeIDValue {
		return ns.NodeNameSpace.Attribute(id, attr)
	}
	switch f.Kind {
	case DropSession:
		ns.record(f)
		ns.drop()
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadConnectionClosed,
		}
	case RefuseRead:
		ns.record(f)
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          f.Status,
		}
	case AnswerLate:
		ns.record(f)
		select {
		case <-time.After(f.Delay):
		case <-ns.closed:
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	case NeverAnswer:
		ns.record(f)
		<-ns.closed
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadServerHalted,
		}
	case FlapValue:
		ns.record(f)
		ns.flap = !ns.flap
		if ns.flap {
			return server.DataValueFromValue(f.Until)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	case RaiseFlagLate:
		if time.Since(f.armed) < f.After {
			ns.record(f)
			return server.DataValueFromValue(f.Until)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	default:
		return ns.NodeNameSpace.Attribute(id, attr)
	}
}

// SetAttribute is the write side. A node with no fault armed is written
// through to the real namespace, which is what makes this server usable as
// its own control.
func (ns *faultNamespace) SetAttribute(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	f, armed := ns.faultFor(id)
	if !armed {
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
	}
	switch f.Kind {
	case DropSession:
		// Record and cut before the value is stored, because the question
		// this fault asks is what a caller does when it cannot find out
		// whether the write landed. Storing it first would answer that.
		ns.record(f)
		ns.drop()
		return ua.StatusBadConnectionClosed
	case RefuseWrite:
		ns.record(f)
		return f.Status
	case AcceptWriteNeverApply:
		// Tell the client it worked and change nothing. The client has no
		// way to know from this answer alone, which is the point: only a
		// read-back finds it.
		ns.record(f)
		return ua.StatusOK
	default:
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
	}
}

// faultFor finds the fault armed against a node, if any.
func (ns *faultNamespace) faultFor(id *ua.NodeID) (Fault, bool) {
	if id == nil {
		return Fault{}, false
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	f, ok := ns.faults[id.StringID()]
	return f, ok
}

// record keeps the evidence that a fault took effect.
func (ns *faultNamespace) record(f Fault) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.firings = append(ns.firings, Firing{Kind: f.Kind, Node: f.Node, At: time.Now()})
}

// freePort asks the operating system for a port and gives it straight back.
// Another process can take it in between; a test that cannot bind should say
// so rather than retry forever, so the window is left visible here.
func freePort() (int, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
