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
	"io"
	"net"
	"sync"
)

// proxy forwards bytes between a client and the OPC UA server and can cut
// the connection underneath them.
//
// A session drop is the one fault the namespace cannot produce alone, because
// it happens below the services: the question is what a client does when the
// socket dies between a write and the read-back that checks it, and by then
// the namespace has already been asked and answered. Every client connects
// through here, so the drop is a real TCP close on a real connection rather
// than an error value handed back politely.
type proxy struct {
	ln      net.Listener
	backend string

	mu    sync.Mutex
	pairs map[int]*connPair // keyed by the backend's local port
	dials int
	done  bool

	// cutMu is held for the whole of a cut -- taking the connection, every
	// Close, and the count that records them -- and by cutCount, so nobody
	// can read the count while a close is in flight. The connection mutex
	// is not enough: a client sees EOF as soon as its socket closes, and a
	// test that reacts to that error and then asks for the count must find
	// the close already counted.
	cutMu  sync.Mutex
	closed int
}

// connPair is one client's hop through the proxy. The key is the backend's
// local port, because that is how the server's side names the connection:
// the remote address a secure channel reports, seen from the server, is the
// port the proxy dialed the server on.
type connPair struct {
	client  net.Conn // the socket underneath the client
	backend net.Conn // the proxy's end of the hop to the server
	port    int
}

// dialCount is how many connections have been accepted since the server
// started. It is how a test sees a client RECONNECT: a client told not to
// reconnect that comes back after a drop shows up here as a second dial,
// with nothing else to give it away.
func (p *proxy) dialCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dials
}

func newProxy(backend string) (*proxy, error) {
	// 127.0.0.1 and not "localhost": the name resolves to both ::1 and
	// 127.0.0.1, the listener binds one of them and the client dials the
	// other, which fails as "connection refused" in perhaps one run in ten.
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{ln: ln, backend: backend, pairs: map[int]*connPair{}}
	go p.accept()
	return p, nil
}

func (p *proxy) port() int { return p.ln.Addr().(*net.TCPAddr).Port }

func (p *proxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // the listener is closed; nothing left to accept
		}
		backend, err := net.Dial("tcp", p.backend)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.dials++
		p.mu.Unlock()
		p.track(client, backend)
		go pipe(client, backend)
		go pipe(backend, client)
	}
}

func (p *proxy) track(client net.Conn, backend net.Conn) {
	pair := &connPair{client: client, backend: backend}
	if tcp, ok := backend.LocalAddr().(*net.TCPAddr); ok {
		pair.port = tcp.Port
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		_ = client.Close()
		_ = backend.Close()
		return
	}
	p.pairs[pair.port] = pair
}

// cutPort closes the one connection that arrived on this port, which is the
// connection that touched the faulted node, and reports how many sockets
// underneath a client it actually closed. The count is what a test asserts,
// so it counts closes that succeeded rather than connections it intended to
// close: a count of connections would still read as evidence if the close
// were removed.
func (p *proxy) cutPort(port int) int {
	p.cutMu.Lock()
	defer p.cutMu.Unlock()
	p.mu.Lock()
	pair := p.pairs[port]
	delete(p.pairs, port)
	p.mu.Unlock()
	if pair == nil {
		return 0
	}
	closed := 0
	if err := pair.client.Close(); err == nil {
		closed++
	}
	_ = pair.backend.Close()
	p.closed += closed
	return closed
}

// cutCount is how many connections every cut so far has closed. It waits for
// a cut in flight, so a caller that got here by noticing a dead socket is
// told about the close that killed it.
func (p *proxy) cutCount() int {
	p.cutMu.Lock()
	defer p.cutMu.Unlock()
	return p.closed
}

// shutdown stops the proxy and tears its connections down without counting
// them, because a server closing is not a session drop. Counting here would
// let a plain shutdown read as drop evidence in Cuts, which is exactly the
// confusion the count exists to settle.
func (p *proxy) shutdown() {
	p.mu.Lock()
	p.done = true
	pairs := p.pairs
	p.pairs = map[int]*connPair{}
	p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
	}
	for _, pair := range pairs {
		_ = pair.client.Close()
		_ = pair.backend.Close()
	}
}

func pipe(dst net.Conn, src net.Conn) {
	_, _ = io.Copy(dst, src)
	// One direction ending means the conversation is over, so close the
	// other rather than leaving a half-open socket that neither side reads.
	_ = dst.Close()
	_ = src.Close()
}
