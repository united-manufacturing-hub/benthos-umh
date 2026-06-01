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
	defaultMaxBackoff     = 30 * time.Second
	initialBackoff        = 250 * time.Millisecond
	telegramBufferSize    = 1024
	dialTimeout           = 10 * time.Second
)

// SessionConfig configures an Open Protocol client session.
type SessionConfig struct {
	Endpoint          string        // controller host:port (e.g. "10.0.0.42:4545")
	Subscriptions     []string      // friendly subscription names: "last_tightening", "alarms"
	GenericMIDs       []int         // additional MIDs to subscribe via the generic mechanism
	Revision          int           // requested MID revision (0 = controller default / 1)
	KeepAliveInterval time.Duration // MID 9999 cadence
	RequestTimeout    time.Duration // timeout awaiting login / handshake replies
	MaxBackoff        time.Duration // reconnect backoff ceiling
}

// Session manages the lifecycle of a single Open Protocol connection: it dials
// the controller, logs in (MID 0001 -> 0002), subscribes to the configured
// event streams, acknowledges pushed results, keeps the link alive (MID 9999),
// and transparently reconnects (replaying login + subscriptions) when the
// connection drops. Received result telegrams are delivered on the channel
// returned by Telegrams.
type Session struct {
	cfg SessionConfig
	log Logger

	out         chan Telegram
	reassembler *Reassembler

	mu       sync.Mutex
	conn     net.Conn // current connection (for Stop to interrupt blocking reads)
	stopped  bool
	writeMu  sync.Mutex // serialises writes (acks vs keep-alives) on a connection
	wg       sync.WaitGroup
	cancelFn context.CancelFunc
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
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	return &Session{
		cfg:         cfg,
		log:         log,
		out:         make(chan Telegram, telegramBufferSize),
		reassembler: NewReassembler(),
	}
}

// Telegrams returns the channel on which fully-received (and reassembled)
// result telegrams are delivered.
func (s *Session) Telegrams() <-chan Telegram { return s.out }

// Start performs the initial connect + login + subscribe synchronously (so
// configuration and connectivity errors surface immediately) and then spawns a
// background goroutine that serves the connection and reconnects as needed.
//
// connectCtx bounds only the initial handshake; the long-lived serve/reconnect
// loop runs under the session's own context (cancelled by Stop), so it survives
// after the caller's connect-scoped context is cancelled.
func (s *Session) Start(connectCtx context.Context) error {
	conn, fr, err := s.connectAndHandshake(connectCtx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	s.mu.Lock()
	s.conn = conn
	s.cancelFn = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go s.serveLoop(ctx, conn, fr)
	return nil
}

// Stop cancels the session and closes the active connection. It blocks until
// the background goroutine has exited.
func (s *Session) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	if s.cancelFn != nil {
		s.cancelFn()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}

// serveLoop serves the current connection and, on disconnect, reconnects with
// exponential backoff until the context is cancelled.
func (s *Session) serveLoop(ctx context.Context, conn net.Conn, fr *FrameReader) {
	defer s.wg.Done()

	backoff := initialBackoff
	for {
		err := s.serve(ctx, conn, fr)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.log.Warnf("open protocol: connection to %s lost: %v; reconnecting", s.cfg.Endpoint, err)
		}

		// Reconnect with backoff.
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			newConn, newFr, derr := s.connectAndHandshake(ctx)
			if derr == nil {
				s.mu.Lock()
				s.conn = newConn
				s.mu.Unlock()
				conn, fr = newConn, newFr
				backoff = initialBackoff
				s.log.Infof("open protocol: reconnected to %s", s.cfg.Endpoint)
				break
			}

			s.log.Warnf("open protocol: reconnect to %s failed: %v", s.cfg.Endpoint, derr)
			backoff = nextBackoff(backoff, s.cfg.MaxBackoff)
		}
	}
}

// serve reads telegrams from conn until an error occurs, while a concurrent
// goroutine emits keep-alives. It returns the error that ended the connection.
func (s *Session) serve(ctx context.Context, conn net.Conn, fr *FrameReader) error {
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Keep-alive emitter.
	go func() {
		t := time.NewTicker(s.cfg.KeepAliveInterval)
		defer t.Stop()
		for {
			select {
			case <-serveCtx.Done():
				return
			case <-t.C:
				if err := s.write(conn, MIDKeepAlive, 1, nil); err != nil {
					cancel() // wake the reader so serve returns
					return
				}
			}
		}
	}()

	for {
		frame, err := fr.ReadFrame()
		if err != nil {
			return err
		}
		tel, err := ParseTelegram(frame)
		if err != nil {
			s.log.Warnf("open protocol: dropping malformed telegram: %v", err)
			continue
		}
		if err := s.handle(serveCtx, conn, tel); err != nil {
			return err
		}
	}
}

// handle processes one received telegram: it reassembles multi-part messages,
// acknowledges pushed results, and forwards completed telegrams downstream.
func (s *Session) handle(ctx context.Context, conn net.Conn, tel Telegram) error {
	switch tel.Header.MID {
	case MIDKeepAlive, MIDCommandAccepted:
		return nil // link management / generic ack: nothing to forward
	case MIDCommandError:
		ce, _ := ParseCommandError(tel)
		s.log.Warnf("open protocol: controller reported %v", ce)
		return nil
	}

	complete, ok, err := s.reassembler.Push(tel)
	if err != nil {
		s.log.Warnf("open protocol: reassembly error: %v", err)
		return nil
	}
	if !ok {
		return nil // waiting for more parts
	}

	// Acknowledge pushed results before forwarding so a slow consumer cannot
	// stall the controller's view of the link.
	if ackMID, needsAck := ackFor(complete.Header.MID); needsAck {
		if err := s.write(conn, ackMID, 1, nil); err != nil {
			return fmt.Errorf("sending ack MID %04d: %w", ackMID, err)
		}
	}

	select {
	case s.out <- complete:
	case <-ctx.Done():
		return ctx.Err()
	default:
		// At-most-once: never block the read loop on a stalled consumer.
		s.log.Warnf("open protocol: telegram buffer full, dropping MID %04d", complete.Header.MID)
	}
	return nil
}

// connectAndHandshake dials the controller, performs the login handshake and
// sends the configured subscriptions. It returns the connection and the
// FrameReader that buffers it (the same reader must be reused for serving, so
// no buffered bytes are lost).
func (s *Session) connectAndHandshake(ctx context.Context) (net.Conn, *FrameReader, error) {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", s.cfg.Endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("dialing %s: %w", s.cfg.Endpoint, err)
	}

	fr := NewFrameReader(conn)

	// Login (MID 0001) and await MID 0002 / 0004.
	if err := s.write(conn, MIDCommunicationStart, s.cfg.Revision, nil); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("sending login: %w", err)
	}

	tel, err := s.readWithTimeout(conn, fr, s.cfg.RequestTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("awaiting login reply: %w", err)
	}
	switch tel.Header.MID {
	case MIDCommunicationStartAck:
		// accepted
	case MIDCommandError:
		ce, _ := ParseCommandError(tel)
		_ = conn.Close()
		return nil, nil, ce
	default:
		_ = conn.Close()
		return nil, nil, fmt.Errorf("unexpected MID %04d during login", tel.Header.MID)
	}

	// Subscribe.
	for _, mid := range s.subscriptionMIDs() {
		if err := s.write(conn, mid, 1, nil); err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("subscribing (MID %04d): %w", mid, err)
		}
	}
	for _, gmid := range s.cfg.GenericMIDs {
		// Generic data subscription (MID 0008). The data field carries the MID
		// to subscribe to. Generic mode is experimental and intended to be
		// validated against a real controller.
		data := fmt.Sprintf("%04d", gmid)
		if err := s.write(conn, MIDGenericSubscribe, 1, []byte(data)); err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("generic-subscribing MID %04d: %w", gmid, err)
		}
	}

	return conn, fr, nil
}

// readWithTimeout reads a single telegram, bounded by timeout.
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

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}
