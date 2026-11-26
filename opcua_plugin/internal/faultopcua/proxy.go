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
// A session drop is the one fault the namespace cannot produce, because it
// happens below the services: the question is what a client does when the
// socket dies between a write and the read-back that checks it, and by then
// the namespace has already been asked and answered. Every client connects
// through here, so the drop is a real TCP close on a real connection rather
// than an error value handed back politely.
type proxy struct {
	ln      net.Listener
	backend string

	mu    sync.Mutex
	conns []net.Conn
	dials int
	done  bool
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
	p := &proxy{ln: ln, backend: backend}
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
		server, err := net.Dial("tcp", p.backend)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.dials++
		p.mu.Unlock()
		p.track(client, server)
		go pipe(client, server)
		go pipe(server, client)
	}
}

func (p *proxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		for _, c := range conns {
			_ = c.Close()
		}
		return
	}
	p.conns = append(p.conns, conns...)
}

// cut closes every connection now open and reports how many it actually
// closed. The count is what a test asserts, so it counts closes that
// succeeded rather than connections it intended to close: a count of
// connections would still read as evidence if the close were removed, and a
// mutation proved that nothing noticed.
func (p *proxy) cut() int {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	closed := 0
	for _, c := range conns {
		if err := c.Close(); err == nil {
			closed++
		}
	}
	return closed
}

func (p *proxy) close() {
	p.mu.Lock()
	p.done = true
	p.mu.Unlock()
	_ = p.ln.Close()
	p.cut()
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	// One direction ending means the conversation is over, so close the
	// other rather than leaving a half-open socket that neither side reads.
	_ = dst.Close()
	_ = src.Close()
}
