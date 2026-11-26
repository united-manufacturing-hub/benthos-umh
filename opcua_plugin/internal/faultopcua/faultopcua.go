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
)

// Fault is one misbehaviour, armed against one node.
type Fault struct {
	// Node is the node's string identifier within this server's namespace,
	// as passed to AddNode -- "Setpoint", not "ns=2;s=Setpoint".
	Node string
	Kind Kind
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
	srv  *server.Server
	ns   *faultNamespace
	host string
	port int
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
	s := &Server{host: "localhost", port: port}
	s.srv = server.New(
		server.EndPoint(s.host, s.port),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)
	s.ns = &faultNamespace{
		NodeNameSpace: server.NewNodeNameSpace(s.srv, "faultopcua"),
		faults:        map[string]Fault{},
	}
	s.srv.AddNamespace(s.ns)
	return s, nil
}

// AddNode gives the server a readable and writable variable node holding
// value. The node's identifier is its name, so AddNode("Setpoint", 0) is
// addressable as ns=<NamespaceID()>;s=Setpoint.
func (s *Server) AddNode(name string, value any) {
	s.ns.AddNewVariableStringNode(name, value)
}

// Inject arms a fault. Faults can be armed before or after Start, because a
// scenario often needs the machine to behave until a particular moment.
func (s *Server) Inject(f Fault) {
	ns := s.ns
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

// NamespaceID is the namespace the server assigned to our nodes. The server
// assigns it at AddNamespace, so a node identifier cannot be built until the
// namespace exists.
func (s *Server) NamespaceID() uint16 { return s.ns.ID() }

// NodeID renders a node's full identifier, the form a client config carries.
func (s *Server) NodeID(name string) string {
	return fmt.Sprintf("ns=%d;s=%s", s.NamespaceID(), name)
}

// Endpoint is the URL a client connects to. Valid once Start has returned.
func (s *Server) Endpoint() string {
	return fmt.Sprintf("opc.tcp://%s:%d", s.host, s.port)
}

// Start listens and serves until ctx is cancelled or Close is called.
func (s *Server) Start(ctx context.Context) error {
	if err := s.srv.Start(ctx); err != nil {
		return fmt.Errorf("faultopcua: start: %w", err)
	}
	return nil
}

// Close stops the server.
func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
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
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
