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
	"testing"

	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
)

// Server is a running OPC UA server with a programmable set of faults.
type Server struct {
	t     *testing.T
	srv   *server.Server
	ns    *faultNamespace
	proxy *proxy
	host  string
	port  int // where the OPC UA server listens, behind the proxy
}

// New starts a server with no faults armed: an honest machine, which is the control
// every fault test needs beside it. It fails the test rather than returning an error,
// because a fixture that could not start is not a condition a test handles, and it
// registers Close with t.Cleanup so a forgotten teardown cannot leak a listener into
// the next test.
//
// The port is taken from the operating system here rather than on Start, so several
// servers can run at once without a test picking a number, and so Endpoint answers
// before the server is listening.
func New(t *testing.T) *Server {
	port, err := freePort()
	if err != nil {
		t.Fatalf("faultopcua: no free port: %v", err)
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

// Start listens and serves until ctx is canceled or Close is called.
func (s *Server) Start(ctx context.Context) error {
	if err := s.srv.Start(ctx); err != nil {
		return fmt.Errorf("faultopcua: start: %w", err)
	}
	return nil
}

// Close stops the server and releases any read a fault is holding. New registers it
// with t.Cleanup; calling it by hand as well is safe.
func (s *Server) Close() error {
	s.ns.release()
	if s.proxy != nil {
		// Not cut(). The proxy's connections are torn down here without
		// being counted, because a shutdown is not a session drop and
		// must not read as one: Cuts is evidence about a fault, and
		// closing the server is not a fault.
		s.proxy.shutdown()
	}
	if s.srv == nil {
		return nil
	}
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
