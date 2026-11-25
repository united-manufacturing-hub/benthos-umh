package handshake

import (
	"context"
	"errors"
)

// A Binding is one industrial protocol: it says what an address is and performs the
// read, write and call a script can ask for. UnifiedAddress is the slot the protocol's
// own address type fills, so one API spans OPC UA, S7 and the rest while each keeps its
// own notation. Implementations need not be safe for concurrent use.
type Binding[UnifiedAddress any] interface {
	// Namespace is the protocol's own word, as a script writes it: the prefix on read,
	// write and wait_until. A script is written against one machine in that machine's
	// vocabulary, so the engine takes the word from the binding and from nowhere else.
	Namespace() string

	// ParseAddress checks the shape of an address a script passed and returns it in this
	// binding's own form. No I/O: it runs on every call.
	ParseAddress(raw map[string]any) (UnifiedAddress, error)

	// Read returns what the machine currently holds for one address.
	Read(ctx context.Context, address UnifiedAddress) (ReadResponse, error)

	// Write sends one value to one address, and returns nil only when the machine
	// accepted it.
	Write(ctx context.Context, address UnifiedAddress, value any) error

	// Describe renders an address for an error message, in the binding's own notation.
	Describe(address UnifiedAddress) string
}

// A Caller is a binding whose protocol has callable addresses, such as OPC UA methods.
// The engine asks for it and offers call_method only when it is there, so a protocol
// without methods implements nothing rather than a method that always refuses.
type Caller[UnifiedAddress any] interface {
	Call(ctx context.Context, address UnifiedAddress, inputs []any) (CallResponse, error)
}

// A ReadResponse is what one machine answered to one read. It is a struct, not a
// bare value, so a binding with more to say has somewhere to put it.
type ReadResponse struct {
	// Value is what the machine holds, in Go's own types: bool, int64, float64,
	// string, or a slice of those for an array tag. A binding that cannot produce one
	// of those reports a persistent fault instead.
	Value any
}

// A CallResponse is what one machine answered to one method call.
type CallResponse struct {
	// Outputs are the method's output arguments, in the order the machine declares
	// them, each one a value in the same sense as ReadResponse.Value.
	Outputs []any
}

// ErrPersistent marks a fault that repeating cannot fix; an error without it is
// treated as transient.
var ErrPersistent = errors.New("fault will not resolve by repeating")
