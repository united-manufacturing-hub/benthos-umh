// Copyright 2026 UMH Systems GmbH
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

// This file is the contract between the write engine (output.go, config.go,
// values.go) and a protocol. It is generic: copy it unchanged.
//
// A protocol plugs in with two parts:
//
//   - a Codec, which turns an address and a message value into a Point. It
//     knows the protocol's address grammar and encoding and does no I/O.
//   - a Dialer and its Session, which sit on top of the plugin's own Client
//     interface (one per plugin, shared by its input and output). The Client
//     is the only place a device library is imported, so swapping the library
//     means writing one new Client adapter.
//
// The engine does not retry. A failed write returns an error and Benthos nacks
// the message. Retry and backoff belong to the write flow in umh-core
// (ENG-5759). Errors are WriteErrors with a stable reason, and Reason.Retryable
// tells the write flow whether another attempt can help.

package template_write_plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Point is one prepared write: where to write and the already-encoded value.
type Point[P any] interface {
	// Address returns the address as the user wrote it, for errors.
	Address() string
	// SameValue reports whether other, read back from the device, holds the
	// value this point wrote.
	SameValue(other P) bool
}

// Codec turns one mapping into a point. It must not do I/O.
type Codec[P any] interface {
	Prepare(address string, value any) (P, error)
}

// Resolved is what a device reports about an address it resolved, for
// example an ADS symbol or an S7+ tag. Library-agnostic, like the ADS
// plugin's SymbolInfo.
type Resolved struct {
	DataType string
	Size     int
}

// Resolver is implemented by sessions of protocols that address by name and
// must ask the device for type and size first (ADS symbols, S7+ tags). The
// engine calls it once per address per connection and caches the result.
type Resolver interface {
	Resolve(ctx context.Context, addresses []string) (map[string]Resolved, error)
}

// ResolvingCodec is implemented by codecs that need the resolved type to
// encode. When the session is a Resolver, the engine calls PrepareResolved
// instead of Prepare.
type ResolvingCodec[P any] interface {
	PrepareResolved(address string, resolved Resolved, value any) (P, error)
}

// Dialer opens a session to the device.
type Dialer[P any] interface {
	Dial(ctx context.Context) (Session[P], error)
}

// Session is one open connection, used for writing.
//
// Write must wrap transport failures with ErrConnectionLost and device-side
// refusals with ErrRejected. Any other error counts as a lost connection, so
// an unclassified library error never leaves a broken session in use.
type Session[P any] interface {
	Write(ctx context.Context, points []P) error
	Close() error
}

// ReadBacker is implemented by sessions that can read written points back.
// ReadBack returns one point per input point, in order, holding the device's
// current value.
type ReadBacker[P any] interface {
	ReadBack(ctx context.Context, points []P) ([]P, error)
}

// Batcher is implemented by sessions that must split a write into several
// requests (item count, PDU size). Without it, all points of a message go to
// Write at once.
type Batcher[P any] interface {
	Batch(points []P) [][]P
}

// Protocol is what a plugin hands to Register.
type Protocol[P Point[P]] struct {
	Codec  Codec[P]
	Dialer Dialer[P]
}

var (
	// ErrConnectionLost marks a transport failure. The engine drops the
	// session and returns service.ErrNotConnected.
	ErrConnectionLost = errors.New("connection to device lost")
	// ErrRejected marks a write the device refused.
	ErrRejected = errors.New("device rejected the write")
	// ErrReadbackMismatch means the read-back value differs from the written one.
	ErrReadbackMismatch = errors.New("read-back does not match the written value")
	// ErrInvalidValue marks a value a codec cannot encode for its address.
	ErrInvalidValue = errors.New("invalid value")
	// ErrNotWritable marks an address a codec refuses to write.
	ErrNotWritable = errors.New("address is not writable")
	// ErrUnresolved marks an address the device could not resolve.
	ErrUnresolved = errors.New("address not found on device")
)

// Reason is a stable, machine-readable error category. It is the first word
// of WriteError.Error(), so a fallback output can route on it from the
// fallback_error metadata without parsing prose.
//
// Reason, Retryable and ParseReason are the retry signal. The plugin never
// retries itself. The write flow in umh-core owns retry and backoff (ENG-5759)
// and decides from the reason in the error text whether another attempt can
// help. The error text format is part of that contract:
//
//	<reason>: <address>: <detail>
//
// Adding a reason is compatible. Renaming or removing one breaks the write
// flow, so a change here needs a matching change in umh-core.
type Reason string

const (
	ReasonInvalidValue     Reason = "invalid_value"
	ReasonNotWritable      Reason = "not_writable"
	ReasonUnresolved       Reason = "unresolved"
	ReasonMissingField     Reason = "missing_field"
	ReasonRejected         Reason = "rejected"
	ReasonReadbackMismatch Reason = "readback_mismatch"
)

// Retryable reports whether writing the same message again can succeed.
//
//	invalid_value, not_writable, missing_field, unresolved: no. The message or
//	  the config is wrong, and the same message fails the same way. (An
//	  unresolved address could appear after a PLC download; that case is left
//	  to a new message, not to a retry loop.)
//	rejected, readback_mismatch: yes. The device state can change, for example
//	  a mode switch or a busy PLC, so a later attempt may be accepted.
//
// A lost connection is not a WriteError. It returns service.ErrNotConnected,
// and Benthos reconnects on its own.
func (r Reason) Retryable() bool {
	switch r {
	case ReasonRejected, ReasonReadbackMismatch:
		return true
	default:
		return false
	}
}

// ParseReason reads the reason from the start of an error text, for example
// the fallback_error metadata. It returns false when the text does not start
// with a known reason, which means the error did not come from this output.
func ParseReason(text string) (Reason, bool) {
	prefix, _, _ := strings.Cut(text, ":")
	switch reason := Reason(prefix); reason {
	case ReasonInvalidValue, ReasonNotWritable, ReasonUnresolved,
		ReasonMissingField, ReasonRejected, ReasonReadbackMismatch:
		return reason, true
	default:
		return "", false
	}
}

// IsRetryable reports whether another attempt at the same message can
// succeed. A lost connection counts as retryable, because the message was
// never judged. An error that is neither one of those is not retryable.
func IsRetryable(err error) bool {
	if errors.Is(err, service.ErrNotConnected) {
		return true
	}
	var writeErr *WriteError
	return errors.As(err, &writeErr) && writeErr.Reason.Retryable()
}

// WriteError is every error Write returns, except service.ErrNotConnected.
type WriteError struct {
	Reason  Reason
	Address string
	Err     error
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("%s: %s: %v", e.Reason, e.Address, e.Err)
}

func (e *WriteError) Unwrap() error { return e.Err }

// reasonFor maps an error to its reason. Unknown errors default to rejected.
func reasonFor(err error) Reason {
	switch {
	case errors.Is(err, ErrInvalidValue):
		return ReasonInvalidValue
	case errors.Is(err, ErrNotWritable):
		return ReasonNotWritable
	case errors.Is(err, ErrUnresolved):
		return ReasonUnresolved
	case errors.Is(err, ErrMissingField):
		return ReasonMissingField
	case errors.Is(err, ErrReadbackMismatch):
		return ReasonReadbackMismatch
	default:
		return ReasonRejected
	}
}

func newWriteError(address string, err error) *WriteError {
	return &WriteError{Reason: reasonFor(err), Address: address, Err: err}
}
