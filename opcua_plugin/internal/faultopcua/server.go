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

	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
)

// TB is the part of a test the server reports to. *testing.T satisfies it, and
// so does Ginkgo's GinkgoT(), so the server works in a plain Go test and in a
// Ginkgo spec alike.
//
// The server calls Fatalf on the caller's goroutine when it cannot be built or a
// fault cannot be armed, and expects it not to return, as both of the above do.
// It calls Errorf from its own goroutines when a fault catches itself reporting
// a firing it did not perform.
type TB interface {
	Helper()
	Cleanup(func())
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// Server is a running OPC UA server with a programmable set of faults.
type Server struct {
	t     TB
	srv   *server.Server
	ns    *faultNamespace
	proxy *proxy
	host  string
	port  int // where the OPC UA server listens, behind the proxy

	// mu guards cancel, which Start sets and Close calls.
	mu sync.Mutex

	// cancel ends the context gopcua's accept and service loops run under.
	// gopcua's own Close closes the listener and the secure channels but
	// leaves the service loop waiting on that context, so without the
	// cancel every server would leave its goroutines behind.
	cancel context.CancelFunc
}

// New builds a server with no faults armed: an honest machine, which is the
// control every fault test needs beside it. Clients can use it once Start has
// returned. New fails the test rather than returning an error, because a fixture
// that could not be built is not a condition a test handles, and it registers
// Close with t.Cleanup so a forgotten teardown cannot leak the listeners or the
// server's goroutines into the next test.
//
// The server's port is taken from the operating system here rather than on Start,
// so several servers can run at once without a test picking a number. The proxy
// in front of the server listens from New on, so Endpoint is valid before Start.
func New(t TB) *Server {
	t.Helper()
	port, err := freePort()
	if err != nil {
		t.Fatalf("faultopcua: no free port: %v", err)
		return nil
	}
	s := &Server{t: t, host: "127.0.0.1", port: port}
	s.srv = server.New(
		server.EndPoint(s.host, s.port),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)
	s.ns = &faultNamespace{
		NodeNameSpace: server.NewNodeNameSpace(s.srv, "faultopcua"),
		t:             t,
		faults:        map[string]*Fault{},
		vars:          map[string]*Variable{},
		notifying:     map[*ua.NodeID]struct{}{},
		closed:        make(chan struct{}),
	}

	// The namespace the server dispatches to must be the wrapper, whose
	// Attribute and SetAttribute are where the faults live, and not the
	// NodeNameSpace inside it -- the constructor registers that one on its
	// own, at the index before this one, and a request routed there meets
	// an honest machine no matter what is armed. The wrapper takes the next
	// index, and the nodes added to it are named with it.
	_ = s.srv.AddNamespace(s.ns)

	// The read and write services are ours rather than gopcua's, registered
	// before Start so the server keeps them: gopcua's own registration will
	// not overwrite a handler that is already there. Everything they do is
	// what gopcua's do, plus one thing gopcua cannot offer any other way --
	// telling the namespace which connection is touching a node, which is
	// how a session drop cuts the client that touched the node rather than
	// every client there is.
	s.srv.RegisterHandler(idReadRequest, s.read)
	s.srv.RegisterHandler(idWriteRequest, s.write)

	// Every client reaches the server through the proxy, so a session drop
	// is a real socket close rather than a polite error, and there is one
	// code path whether or not a drop is armed.
	px, err := newProxy(fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		t.Fatalf("faultopcua: proxy: %v", err)
		return nil
	}
	s.proxy = px
	s.ns.onDrop = func(port int) int { return px.cutPort(port) }
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// AddVariable gives the server a readable and writable node holding value, and
// returns the handle every fault is armed against.
func (s *Server) AddVariable(name string, value any) *Variable {
	n := s.ns.AddNewVariableStringNode(name, value)
	v := &Variable{ns: s.ns, id: n.ID(), name: name}
	s.ns.mu.Lock()
	s.ns.vars[n.ID().StringID()] = v
	s.ns.mu.Unlock()
	return v
}

// Endpoint is the URL a client connects to: the proxy's address, not the server's.
// Valid as soon as New has returned. It is the string an operator would paste into
// the plugin's config, so a test exercises the real address path.
func (s *Server) Endpoint() string {
	return fmt.Sprintf("opc.tcp://%s:%d", s.host, s.proxy.port())
}

// Dials reports how many connections clients have opened. It is evidence about the
// client, not about a fault: a reconnect is invisible from inside a client that
// hides it, and visible here.
func (s *Server) Dials() int { return s.proxy.dialCount() }

// Cuts reports how many connections a DropSession fault actually closed. It is the
// independent witness beside Fault.Count: Count says the fault's branch ran, Cuts
// says a socket died. Shutting the server down does not change it.
func (s *Server) Cuts() int { return s.proxy.cutCount() }

// Start listens and serves until Close is called. Cancelling ctx stops gopcua's
// accept and service loops too, but only Close releases the requests a fault is
// holding and shuts the proxy down, so a test closes the server either way.
func (s *Server) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	if err := s.srv.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("faultopcua: start: %w", err)
	}
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	return nil
}

// Close stops the server and releases any request a fault is holding. New
// registers it with t.Cleanup; calling it by hand as well is safe.
//
// It cancels the context Start handed to gopcua, which is what ends gopcua's
// accept and service loops; gopcua's own Close does not. The loops do not
// report when they have finished, so Close cannot wait for them, but neither
// can outlive the cancel: the accept loop's listener is closed by gopcua's
// Close, and the service loop is either waiting on the context or inside one
// of this package's handlers, which releasing the faults lets go.
func (s *Server) Close() error {
	s.ns.release()
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Not cutPort. The proxy's connections are torn down here without being
	// counted, because a shutdown is not a session drop and must not read as
	// one: Cuts is evidence about a fault, and closing the server is not a
	// fault.
	s.proxy.shutdown()
	return s.srv.Close()
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
