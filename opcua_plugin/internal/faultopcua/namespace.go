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
	"net"
	"sync"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
	"github.com/gopcua/opcua/uasc"
)

// faultKind names one way a machine can misbehave. The names are the plant's
// words for what went wrong, not the protocol's. Unexported: the API speaks in
// arm methods, and nothing outside the package has a reason to spell a kind.
type faultKind uint8

const (
	faultAcceptWriteNeverApply faultKind = iota
	faultRefuseWrite
	faultRefuseRead
	faultFlapValue
	faultAnswerLate
	faultNeverAnswer
	faultDropSession
	faultRaiseFlagLate
)

// idReadRequest and idWriteRequest are the type IDs gopcua's server dispatches
// the Read and Write services on. We register our own handlers for them before
// Start, which is the extension point gopcua documents for exactly this.
const (
	idReadRequest  = id.ReadRequest_Encoding_DefaultBinary
	idWriteRequest = id.WriteRequest_Encoding_DefaultBinary
)

// noConnection is the port a read or write carries when no client connection
// is behind it: gopcua reading a node for a monitored item on its own.
const noConnection = 0

// faultNamespace is where a fault takes effect: it sits in front of the real
// gopcua namespace and answers first, so a read or a write meets the fault
// before it meets the node. Unexported, and the only place a fault kind is
// interpreted.
//
// It implements gopcua's namespace interface. Attribute and SetAttribute are
// the calls gopcua makes on its own, with no request behind them; the
// server's Read and Write handlers call readFrom and writeFrom instead, which
// also carry the connection the request arrived on.
type faultNamespace struct {
	*server.NodeNameSpace

	// t is the test this namespace serves, and the only witness a fault
	// can call to when it catches itself lying at request time, on a
	// goroutine that is not the test's own.
	t TB

	mu     sync.Mutex
	faults map[string]*Fault // keyed by the node's StringID; one armed fault per node
	vars   map[string]*Variable

	// notifying holds the node IDs of the writes being stored right now on
	// behalf of a fault that has already answered them. See writeThrough.
	notifying map[*ua.NodeID]struct{}

	// closed releases anything NeverAnswer or AnswerLate is holding, so a
	// stuck request ends with the server rather than outliving the test
	// that armed it.
	closed chan struct{}

	// onDrop cuts the connection that arrived on port and reports how many
	// sockets it closed. The proxy keeps the count, because only the proxy
	// can publish it before the close it records becomes visible to the
	// client.
	onDrop func(port int) int
}

// Attribute is the read gopcua makes on its own: the value it sends a monitored
// item when the item is created or the node changes. No client connection is
// behind it, so a DropSession it meets has nothing to cut.
func (ns *faultNamespace) Attribute(id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	return ns.readFrom(noConnection, id, attr)
}

// SetAttribute is the write gopcua makes on its own. No client connection is
// behind it, so a DropSession it meets has nothing to cut.
func (ns *faultNamespace) SetAttribute(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	return ns.writeFrom(noConnection, id, attr, val)
}

// readFrom is the read side. port is the proxy port of the connection the
// read arrived on, or noConnection. Each caller passes its own, so two reads
// in flight at once -- the service loop's and a monitored item's -- cannot
// see each other's connection.
//
// A read reaches here for every attribute, not only the value, so a fault
// that refused all of them would also refuse the browse and type reads a
// client makes while connecting, and the test would fail before reaching
// what it meant to test. Faults therefore apply to the value attribute only.
func (ns *faultNamespace) readFrom(port int, id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	if ns.isNotifying(id) {
		// The change notification of a write a fault has already
		// answered: part of that write, not a read of its own.
		return ns.NodeNameSpace.Attribute(id, attr)
	}
	if v := ns.varFor(id); v != nil {
		v.sawRead()
	}
	f, armed := ns.faultFor(id)
	if !armed || attr != ua.AttributeIDValue {
		return ns.NodeNameSpace.Attribute(id, attr)
	}
	switch f.kind {
	case faultDropSession:
		f.tookEffect()
		ns.drop(f, port)
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadConnectionClosed,
		}
	case faultRefuseRead:
		f.tookEffect()
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          f.status,
		}
	case faultAnswerLate:
		f.tookEffect()
		ns.wait(f.delay)
		return ns.NodeNameSpace.Attribute(id, attr)
	case faultNeverAnswer:
		f.tookEffect()
		<-ns.closed
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadServerHalted,
		}
	case faultFlapValue:
		f.tookEffect()
		if f.toggle() {
			return server.DataValueFromValue(f.until)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	case faultRaiseFlagLate:
		if time.Since(f.armed) < f.after {
			f.tookEffect()
			return server.DataValueFromValue(f.until)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	default:
		return ns.NodeNameSpace.Attribute(id, attr)
	}
}

// writeFrom is the write side; port is as for readFrom. A node with no fault
// armed is written through to the real namespace, which is what makes this
// server usable as its own control.
func (ns *faultNamespace) writeFrom(port int, id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	if v := ns.varFor(id); v != nil {
		v.sawWrite()
	}
	f, armed := ns.faultFor(id)
	if !armed {
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
	}
	switch f.kind {
	case faultDropSession:
		// Record and cut before the value is stored, because the question
		// this fault asks is what a caller does when it cannot find out
		// whether the write landed. Storing it first would answer that.
		f.tookEffect()
		ns.drop(f, port)
		return ua.StatusBadConnectionClosed
	case faultRefuseWrite:
		f.tookEffect()
		return f.status
	case faultAcceptWriteNeverApply:
		// Tell the client it worked and change nothing. The client has no
		// way to know from this answer alone, which is the point: only a
		// read-back finds it.
		f.tookEffect()
		return ua.StatusOK
	case faultAnswerLate:
		// The write path answers late exactly as the read path does: the
		// machine is working, it is only slow, and the deadline is the
		// client's to keep.
		f.tookEffect()
		ns.wait(f.delay)
		return ns.writeThrough(id, attr, val)
	case faultNeverAnswer:
		// Nothing comes back until the server closes, on writes as on
		// reads: the half-state is the whole point of this fault.
		f.tookEffect()
		<-ns.closed
		return ua.StatusBadServerHalted
	default:
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
	}
}

// writeThrough stores a write that a fault has already answered.
//
// gopcua's namespace announces every stored value to the node's monitored
// items, and does it by reading the node back through Attribute, on the same
// goroutine and with the same *ua.NodeID it was handed. Without this, that
// read would meet the fault again: one write would count twice, and an
// AnswerLate write to a monitored node would be late twice. So the write is
// stored under a copy of the ID that no other caller holds, and readFrom
// answers a read of exactly that pointer honestly. A monitored item's own
// reads use the item's ID, so they still meet the fault.
func (ns *faultNamespace) writeThrough(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	own := *id
	key := &own
	ns.mu.Lock()
	ns.notifying[key] = struct{}{}
	ns.mu.Unlock()
	defer func() {
		ns.mu.Lock()
		delete(ns.notifying, key)
		ns.mu.Unlock()
	}()
	return ns.NodeNameSpace.SetAttribute(key, attr, val)
}

// isNotifying reports whether id is the ID a writeThrough is storing under.
func (ns *faultNamespace) isNotifying(id *ua.NodeID) bool {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	_, ok := ns.notifying[id]
	return ok
}

// read and write are the server's Read and Write service handlers. They do
// what gopcua's own do -- dispatch each node of the request to the namespace
// it lives in and answer with the same response -- with one addition: they
// tell this package's namespace which connection is making the request.
// gopcua calls a namespace with only the node and the attribute, so without
// this the namespace could not know whose session a DropSession is supposed
// to cut.
func (s *Server) read(sc *uasc.SecureChannel, r ua.Request, _ uint32) (ua.Response, error) {
	req, ok := r.(*ua.ReadRequest)
	if !ok {
		return nil, ua.StatusBadRequestTypeInvalid
	}
	port := portOf(sc)
	results := make([]*ua.DataValue, len(req.NodesToRead))
	for i, n := range req.NodesToRead {
		ns, err := s.srv.Namespace(int(n.NodeID.Namespace()))
		switch {
		case err != nil:
			results[i] = &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBad,
			}
		case ns == s.ns:
			results[i] = s.ns.readFrom(port, n.NodeID, n.AttributeID)
		default:
			results[i] = ns.Attribute(n.NodeID, n.AttributeID)
		}
	}
	return &ua.ReadResponse{
		ResponseHeader: responseHeader(req.RequestHeader.RequestHandle, ua.StatusOK),
		Results:        results,
	}, nil
}

func (s *Server) write(sc *uasc.SecureChannel, r ua.Request, _ uint32) (ua.Response, error) {
	req, ok := r.(*ua.WriteRequest)
	if !ok {
		return nil, ua.StatusBadRequestTypeInvalid
	}
	port := portOf(sc)
	status := make([]ua.StatusCode, len(req.NodesToWrite))
	for i, n := range req.NodesToWrite {
		ns, err := s.srv.Namespace(int(n.NodeID.Namespace()))
		switch {
		case err != nil:
			status[i] = ua.StatusBadNodeIDUnknown
		case ns == s.ns:
			status[i] = s.ns.writeFrom(port, n.NodeID, n.AttributeID, n.Value)
		default:
			status[i] = ns.SetAttribute(n.NodeID, n.AttributeID, n.Value)
		}
	}
	return &ua.WriteResponse{
		ResponseHeader:  responseHeader(req.RequestHeader.RequestHandle, ua.StatusOK),
		Results:         status,
		DiagnosticInfos: []*ua.DiagnosticInfo{},
	}, nil
}

// portOf names a secure channel by the port its TCP connection comes from.
// Every client connects through the proxy, so from the server's side of the
// hop that port is the proxy's, and the proxy knows which of its connections
// it is.
func portOf(sc *uasc.SecureChannel) int {
	if sc == nil {
		return noConnection
	}
	if tcp, ok := sc.RemoteAddr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return noConnection
}

// responseHeader is gopcua's own response header, built the same way its
// service handlers build it, so a client cannot tell our handlers from the
// ones they replace.
func responseHeader(reqID uint32, statusCode ua.StatusCode) *ua.ResponseHeader {
	return &ua.ResponseHeader{
		Timestamp:          time.Now(),
		RequestHandle:      reqID,
		ServiceResult:      statusCode,
		ServiceDiagnostics: &ua.DiagnosticInfo{},
		StringTable:        []string{},
		AdditionalHeader:   ua.NewExtensionObject(nil),
	}
}

// wait holds a request for delay, or until the server closes.
func (ns *faultNamespace) wait(delay time.Duration) {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ns.closed:
	}
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

// drop cuts the connection that arrived on port. The proxy counts what it
// closed.
//
// When there is no connection to cut, it fails the test rather than
// returning quietly, because the fault has already recorded a firing and
// answered with a ConnectionClosed: a fault that reports it fired while
// closing nothing is the one thing this package must never do. The
// connection is missing when the read that touched the node is gopcua's
// own, for a monitored item, with no request and no session behind it.
func (ns *faultNamespace) drop(f *Fault, port int) {
	if ns.onDrop == nil {
		return
	}
	if cut := ns.onDrop(port); cut == 0 {
		ns.report("faultopcua: DropSession fired on %q with no connection to cut (the request's port was %d): "+
			"the session was not dropped and nothing was closed, so the fault has reported a firing it did not perform -- "+
			"a fault that reports it fired while closing nothing is the one thing this package must never do", f.node, port)
	}
}

// report fails the test from one of the server's goroutines. testing.T's
// Errorf is safe there. GinkgoT's records the failure and then panics to end
// the spec, which on a goroutine that is not the spec's would crash the whole
// test binary instead, so the panic is recovered here once the failure is on
// record, as GinkgoRecover would.
func (ns *faultNamespace) report(format string, args ...any) {
	defer func() { _ = recover() }()
	ns.t.Errorf(format, args...)
}

// faultFor finds the fault armed against a node, if any.
func (ns *faultNamespace) faultFor(id *ua.NodeID) (*Fault, bool) {
	if id == nil {
		return nil, false
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	f, ok := ns.faults[id.StringID()]
	return f, ok
}

// varFor finds the handle of a node, if the node is one of ours, so a
// request that reaches it can be counted on the handle the test holds.
func (ns *faultNamespace) varFor(id *ua.NodeID) *Variable {
	if id == nil {
		return nil
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.vars[id.StringID()]
}
