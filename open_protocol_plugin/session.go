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

package open_protocol_plugin

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Logger is the minimal logging surface the session needs. *service.Logger from
// Benthos satisfies it; tests pass nil for a no-op.
type Logger interface {
	Debugf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
}

// nopLogger is used when a nil Logger is supplied.
type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

const (
	defaultKeepAlive      = 10 * time.Second
	defaultRequestTimeout = 5 * time.Second
	defaultReadTimeout    = 30 * time.Second
	dialTimeout           = 10 * time.Second
)

// SessionConfig configures an Open Protocol client session.
type SessionConfig struct {
	Endpoint          string        // controller host:port (e.g. "10.0.0.42:4545")
	Subscriptions     []string      // friendly subscription names: "last_tightening", "alarms"
	GenericMIDs       []int         // additional MIDs to subscribe via the generic mechanism
	Revision          int           // requested MID revision (pinned to 1)
	KeepAliveInterval time.Duration // MID 9999 cadence
	RequestTimeout    time.Duration // timeout awaiting login / handshake replies
	ReadTimeout       time.Duration // deadline for each Read call (0 → defaultReadTimeout)
}

// Result couples a forwardable telegram with its idempotent, generation-bound
// 0062 ack closure.
type Result struct {
	Telegram Telegram
	Ack      func()
}

var errNotConnected = fmt.Errorf("open protocol: not connected")

// errSessionClosed is returned by Read when the session was deliberately
// closed via Close(), as opposed to an unexpected connection loss.
var errSessionClosed = fmt.Errorf("open protocol: session closed")

// Session manages an Open Protocol connection following the Benthos-native
// lifecycle: Connect establishes the session (login + confirmed subscriptions +
// a connection-scoped keep-alive), Read delivers the next forwardable telegram,
// and Close tears everything down. On connection loss Read returns an error and
// the caller (Benthos) re-invokes Connect.
type Session struct {
	cfg SessionConfig
	log Logger

	mu          sync.Mutex
	conn        net.Conn
	fr          *FrameReader
	reassembler *Reassembler
	pending     []Telegram // telegrams read during handshake, to drain first
	generation  uint64
	kaCancel    context.CancelFunc
	closed      bool // true when Close() was called deliberately; distinct from a transient connection loss

	writeMu sync.Mutex
}

// NewSession creates a Session from cfg. A nil logger is replaced with a no-op.
func NewSession(cfg SessionConfig, log Logger) *Session {
	if log == nil {
		log = nopLogger{}
	}
	if cfg.KeepAliveInterval <= 0 {
		cfg.KeepAliveInterval = defaultKeepAlive
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultReadTimeout
	}
	return &Session{cfg: cfg, log: log}
}

// Generation returns the current connection generation (monotonically increasing
// with each successful Connect).
func (s *Session) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

// Connect establishes (or re-establishes) the session: login + confirmed
// subscriptions + a connection-scoped keep-alive. Re-runnable; tears down any
// prior connection first.
func (s *Session) Connect(ctx context.Context) error {
	s.teardown()

	conn, fr, pending, err := s.connectAndHandshake(ctx)
	if err != nil {
		return err
	}

	kaCtx, kaCancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.conn = conn
	s.fr = fr
	s.reassembler = NewReassembler()
	s.pending = pending
	s.generation++
	s.kaCancel = kaCancel
	s.closed = false
	s.mu.Unlock()

	go s.keepAlive(kaCtx, conn)
	return nil
}

func (s *Session) teardown() {
	s.mu.Lock()
	if s.kaCancel != nil {
		s.kaCancel()
		s.kaCancel = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.fr = nil
	s.pending = nil
	s.mu.Unlock()
}

// Close idempotently stops the keep-alive and closes the connection.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.kaCancel != nil {
		s.kaCancel()
		s.kaCancel = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.mu.Unlock()
	return nil
}

// Read returns the next forwardable telegram + its ack closure, or a connection
// error (the input surfaces it as service.ErrNotConnected).
func (s *Session) Read(ctx context.Context) (Result, error) {
	s.mu.Lock()
	conn, gen, fr, reasm := s.conn, s.generation, s.fr, s.reassembler
	// Pop any pending telegram buffered during handshake.
	var pending *Telegram
	if len(s.pending) > 0 {
		t := s.pending[0]
		s.pending = s.pending[1:]
		pending = &t
	}
	s.mu.Unlock()
	if conn == nil || reasm == nil {
		return Result{}, errNotConnected
	}

	for {
		var tel Telegram
		if pending != nil {
			tel = *pending
			pending = nil
		} else {
			if s.cfg.ReadTimeout > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
			}
			frame, err := fr.ReadFrame()
			if err != nil {
				s.mu.Lock()
				closed := s.closed
				s.mu.Unlock()
				if closed {
					return Result{}, errSessionClosed
				}
				return Result{}, err
			}
			var perr error
			tel, perr = ParseTelegram(frame)
			if perr != nil {
				s.log.Warnf("open protocol: dropping malformed telegram: %v", perr)
				continue
			}
		}

		switch tel.Header.MID {
		case MIDKeepAlive, MIDCommandAccepted:
			continue
		case MIDCommandError:
			ce, _ := ParseCommandError(tel)
			s.log.Warnf("open protocol: controller reported %v", ce)
			continue
		}

		complete, ok, rerr := reasm.Push(tel)
		if rerr != nil {
			s.log.Warnf("open protocol: reassembly error: %v", rerr)
			continue
		}
		if !ok {
			continue
		}
		ackMID, needsAck := ackFor(complete.Header.MID)
		return Result{Telegram: complete, Ack: s.makeAck(gen, conn, ackMID, needsAck)}, nil
	}
}

// makeAck builds the idempotent, generation-bound MID 0062 sender. The guard is
// keyed on having sent a 0062 and latches only on a SUCCESSFUL write, so a
// transient write failure does not permanently suppress a later retry. Lock
// order: s.mu (released) -> local mu -> writeMu (inside s.write). Stale
// generation => benign no-op; write failure => benign no-op without latching.
func (s *Session) makeAck(gen uint64, conn net.Conn, ackMID int, needsAck bool) func() {
	var (
		mu   sync.Mutex
		sent bool
	)
	return func() {
		if !needsAck {
			return
		}
		s.mu.Lock()
		current := s.generation
		s.mu.Unlock()
		if current != gen {
			return // stale generation: controller re-pushes on the new session (Edge #22)
		}
		mu.Lock()
		defer mu.Unlock()
		if sent {
			return // a 0062 was already sent for this result
		}
		if err := s.write(conn, ackMID, 1, nil); err != nil {
			s.log.Warnf("open protocol: 0062 ack write failed (benign, will retry on re-ack): %v", err)
			return // do NOT latch on failure
		}
		sent = true
	}
}

func (s *Session) keepAlive(ctx context.Context, conn net.Conn) {
	t := time.NewTicker(s.cfg.KeepAliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.write(conn, MIDKeepAlive, 1, nil); err != nil {
				_ = conn.Close() // next Read errors -> Benthos reconnects
				return
			}
		}
	}
}

// connectAndHandshake dials, logs in, and confirms each subscription. It returns
// the conn, its FrameReader, and any forwardable telegrams read during the
// handshake window (to be drained by Read first).
func (s *Session) connectAndHandshake(ctx context.Context) (net.Conn, *FrameReader, []Telegram, error) {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", s.cfg.Endpoint)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dialing %s: %w", s.cfg.Endpoint, err)
	}
	fr := NewFrameReader(conn)

	// Login (MID 0001) and await MID 0002 / 0004.
	if err := s.write(conn, MIDCommunicationStart, s.cfg.Revision, nil); err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("sending login: %w", err)
	}
	tel, err := s.readWithTimeout(conn, fr, s.cfg.RequestTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("awaiting login reply: %w", err)
	}
	switch tel.Header.MID {
	case MIDCommunicationStartAck:
		// accepted
	case MIDCommandError:
		ce, _ := ParseCommandError(tel)
		_ = conn.Close()
		return nil, nil, nil, ce
	default:
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("unexpected MID %04d during login", tel.Header.MID)
	}

	var pending []Telegram

	// Subscribe + confirm each.
	confirmSub := func(subMID int) error {
		if err := s.write(conn, subMID, 1, nil); err != nil {
			return fmt.Errorf("subscribing (MID %04d): %w", subMID, err)
		}
		// Await confirmation; skip keep-alives; buffer a stray result as confirmation.
		for {
			rep, err := s.readWithTimeout(conn, fr, s.cfg.RequestTimeout)
			if err != nil {
				return fmt.Errorf("awaiting subscribe reply (MID %04d): %w", subMID, err)
			}
			switch rep.Header.MID {
			case MIDKeepAlive:
				continue
			case MIDCommandAccepted:
				return nil
			case MIDCommandError:
				ce, _ := ParseCommandError(rep)
				return ce
			default:
				// A pushed result during the window also confirms; keep it for Read.
				pending = append(pending, rep)
				return nil
			}
		}
	}
	for _, mid := range s.subscriptionMIDs() {
		if err := confirmSub(mid); err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
	}
	// Generic subscribes remain fire-and-forget (experimental; not confirmed).
	for _, gmid := range s.cfg.GenericMIDs {
		data := fmt.Sprintf("%04d", gmid)
		if err := s.write(conn, MIDGenericSubscribe, 1, []byte(data)); err != nil {
			_ = conn.Close()
			return nil, nil, nil, fmt.Errorf("generic-subscribing MID %04d: %w", gmid, err)
		}
	}

	return conn, fr, pending, nil
}

// readWithTimeout reads a single telegram, bounded by timeout. The read
// deadline is reset to zero on return so it does not leak into subsequent reads.
func (s *Session) readWithTimeout(conn net.Conn, fr *FrameReader, timeout time.Duration) (Telegram, error) {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	frame, err := fr.ReadFrame()
	if err != nil {
		return Telegram{}, err
	}
	return ParseTelegram(frame)
}

// write serialises and sends one telegram, holding the write mutex so acks and
// keep-alives never interleave on the wire.
func (s *Session) write(conn net.Conn, mid, rev int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := conn.Write(BuildMessage(mid, rev, data))
	return err
}

// subscriptionMIDs maps the configured friendly subscription names to the MID
// each one opens.
func (s *Session) subscriptionMIDs() []int {
	var mids []int
	for _, name := range s.cfg.Subscriptions {
		switch name {
		case "last_tightening":
			mids = append(mids, MIDLastTighteningSub)
		case "alarms":
			mids = append(mids, MIDAlarmSub)
		default:
			s.log.Warnf("open protocol: unknown subscription %q, ignoring", name)
		}
	}
	return mids
}

// ackFor returns the acknowledgement MID for a pushed result MID, and whether
// an acknowledgement is required.
func ackFor(mid int) (int, bool) {
	switch mid {
	case MIDLastTightening:
		return MIDLastTighteningAck, true
	case MIDAlarm:
		return MIDAlarmAck, true
	default:
		return 0, false
	}
}
