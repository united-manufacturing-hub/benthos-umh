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
// It implements gopcua's server.NameSpace: Attribute for reads, SetAttribute
// for writes.
type faultNamespace struct {
	*server.NodeNameSpace

	// t is the test this namespace serves, and the only witness a fault
	// can call to when it catches itself lying at request time, on a
	// goroutine that is not the test's own.
	t TB

	mu     sync.Mutex
	faults map[string]*Fault // keyed by the node's StringID; one armed fault per node
	vars   map[string]*Variable

	// writingThrough counts the write-throughs in progress per node.
	// gopcua's SetAttribute notifies the node's subscribers synchronously,
	// and the notification reads the node back through Attribute: without
	// this, that read would meet the read-side fault and count as a read
	// the client never made.
	writingThrough map[string]int

	// closed releases anything NeverAnswer or AnswerLate is holding, so a
	// stuck request ends with the server rather than outliving the test that
	// armed it.
	closed chan struct{}

	// onDrop cuts the connection that arrived on port and reports how many
	// sockets it closed. The proxy keeps the count, because only the proxy
	// can publish it before the close it records becomes visible to the
	// client.
	onDrop func(port int) int
}

// request is one Read or Write service request as the namespace sees it. The
// port names the client's connection, so a DropSession cuts the client that
// touched the node rather than every client there is. A request that did not
// come through the read and write handlers -- gopcua answering a monitored
// item from its own goroutine -- has none, and port is zero.
type request struct {
	port    int
	dropped bool // a DropSession has cut this request's connection
}

// Attribute is the read side for a read that no client request carries:
// gopcua calls it from its own goroutine to answer a monitored item.
func (ns *faultNamespace) Attribute(id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	return ns.read(&request{}, id, attr)
}

// read answers one node of a read. Faults and counters apply to the value
// attribute only: a client also reads the browse and type attributes while
// connecting, and a fault that refused those would fail the test before it
// reached what it meant to test.
func (ns *faultNamespace) read(req *request, id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
	if attr != ua.AttributeIDValue || ns.isWritingThrough(id) {
		return ns.NodeNameSpace.Attribute(id, attr)
	}
	if v := ns.varFor(id); v != nil {
		v.sawRead()
	}
	f, armed := ns.faultFor(id)
	if !armed {
		return ns.NodeNameSpace.Attribute(id, attr)
	}
	switch f.kind {
	case faultDropSession:
		f.tookEffect()
		ns.drop(req, f)
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
		ns.hold(f, time.After(f.delay))
		return ns.NodeNameSpace.Attribute(id, attr)
	case faultNeverAnswer:
		f.tookEffect()
		ns.hold(f, nil)
		return &ua.DataValue{
			EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
			ServerTimestamp: time.Now(),
			Status:          ua.StatusBadServerHalted,
		}
	case faultFlapValue:
		f.tookEffect()
		if f.toggle() {
			return server.DataValueFromValue(f.served)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	case faultRaiseFlagLate:
		if time.Since(f.armed) < f.after {
			f.tookEffect()
			return server.DataValueFromValue(f.served)
		}
		return ns.NodeNameSpace.Attribute(id, attr)
	default:
		return ns.NodeNameSpace.Attribute(id, attr)
	}
}

// SetAttribute is the write side for a write that no client request carries.
func (ns *faultNamespace) SetAttribute(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	return ns.write(&request{}, id, attr, val)
}

// write answers one node of a write. A node with no fault armed is written
// through to the real namespace, which is what makes this server usable as
// its own control. As on the read side, faults and counters apply to the
// value attribute only.
func (ns *faultNamespace) write(req *request, id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	if attr != ua.AttributeIDValue {
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
	}
	if v := ns.varFor(id); v != nil {
		v.sawWrite()
	}
	f, armed := ns.faultFor(id)
	if !armed {
		return ns.writeThrough(id, attr, val)
	}
	switch f.kind {
	case faultDropSession:
		// Record and cut before the value is stored, because the question
		// this fault asks is what a caller does when it cannot find out
		// whether the write landed. Storing it first would answer that.
		f.tookEffect()
		ns.drop(req, f)
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
		ns.hold(f, time.After(f.delay))
		return ns.writeThrough(id, attr, val)
	case faultNeverAnswer:
		// Nothing comes back until the fault is disarmed or the server
		// closes, on writes as on reads: the half-state is the whole point
		// of this fault.
		f.tookEffect()
		ns.hold(f, nil)
		return ua.StatusBadServerHalted
	default:
		return ns.writeThrough(id, attr, val)
	}
}

// writeThrough stores the value in the real namespace. The subscribers'
// notification that gopcua sends from inside it reads the node through
// Attribute, and that read passes the faults and counters by, because no
// client made it. The guard is per node, so a monitored item's first read
// that lands on the same node during a write-through passes them by too.
func (ns *faultNamespace) writeThrough(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
	key := id.StringID()
	ns.mu.Lock()
	ns.writingThrough[key]++
	ns.mu.Unlock()
	defer func() {
		ns.mu.Lock()
		ns.writingThrough[key]--
		ns.mu.Unlock()
	}()
	return ns.NodeNameSpace.SetAttribute(id, attr, val)
}

func (ns *faultNamespace) isWritingThrough(id *ua.NodeID) bool {
	if id == nil {
		return false
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.writingThrough[id.StringID()] > 0
}

// hold blocks until done fires, the fault is disarmed, or the server closes.
// A nil done holds until one of the other two.
func (ns *faultNamespace) hold(f *Fault, done <-chan time.Time) {
	select {
	case <-done:
	case <-f.disarmed:
	case <-ns.closed:
	}
}

// read and write are the server's Read and Write service handlers. They do
// what gopcua's own do -- dispatch each node of the request to the namespace
// it lives in and answer with the same response -- with two additions. They
// tell the namespace which connection is making the request: gopcua calls a
// namespace with only the node and the attribute, so without this the
// namespace could not know whose session a DropSession is supposed to cut.
// And once a DropSession has cut the connection, they stop: the rest of the
// request is answered BadConnectionClosed without reaching a node, because a
// machine that dropped the session does not go on serving it.
func (s *Server) read(sc *uasc.SecureChannel, r ua.Request, _ uint32) (ua.Response, error) {
	req, ok := r.(*ua.ReadRequest)
	if !ok {
		return nil, ua.StatusBadRequestTypeInvalid
	}
	call := &request{port: portOf(sc)}
	results := make([]*ua.DataValue, len(req.NodesToRead))
	for i, n := range req.NodesToRead {
		if call.dropped {
			results[i] = &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBadConnectionClosed,
			}
			continue
		}
		if s.isOurs(n.NodeID) {
			results[i] = s.ns.read(call, n.NodeID, n.AttributeID)
			continue
		}
		ns, err := s.srv.Namespace(int(n.NodeID.Namespace()))
		if err != nil {
			results[i] = &ua.DataValue{
				EncodingMask:    ua.DataValueServerTimestamp | ua.DataValueStatusCode,
				ServerTimestamp: time.Now(),
				Status:          ua.StatusBad,
			}
			continue
		}
		results[i] = ns.Attribute(n.NodeID, n.AttributeID)
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
	call := &request{port: portOf(sc)}
	status := make([]ua.StatusCode, len(req.NodesToWrite))
	for i := range req.NodesToWrite {
		n := req.NodesToWrite[i]
		if call.dropped {
			status[i] = ua.StatusBadConnectionClosed
			continue
		}
		if s.isOurs(n.NodeID) {
			status[i] = s.ns.write(call, n.NodeID, n.AttributeID, n.Value)
			continue
		}
		ns, err := s.srv.Namespace(int(n.NodeID.Namespace()))
		if err != nil {
			status[i] = ua.StatusBadNodeIDUnknown
			continue
		}
		status[i] = ns.SetAttribute(n.NodeID, n.AttributeID, n.Value)
	}
	return &ua.WriteResponse{
		ResponseHeader:  responseHeader(req.RequestHeader.RequestHandle, ua.StatusOK),
		Results:         status,
		DiagnosticInfos: []*ua.DiagnosticInfo{},
	}, nil
}

// isOurs reports whether a node lives in the fault namespace.
func (s *Server) isOurs(id *ua.NodeID) bool {
	return id != nil && id.Namespace() == s.ns.ID()
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

// drop cuts the connection the request came in on, and marks the request
// dropped so the handler serves nothing more of it. The proxy counts what it
// closed.
//
// When there is no connection to cut, it fails the test rather than
// returning quietly, because the fault has already recorded a firing and
// answered with a ConnectionClosed: a fault that reports it fired while
// closing nothing is the one thing this package must never do. A read that
// gopcua makes for a monitored item has no connection. This runs on the
// service loop, not the test's goroutine, so it reports with t.Errorf.
func (ns *faultNamespace) drop(req *request, f *Fault) {
	if ns.onDrop == nil {
		return
	}
	req.dropped = true
	if cut := ns.onDrop(req.port); cut == 0 {
		ns.report("faultopcua: DropSession fired on %q with no connection to cut (the request's port was %d): "+
			"the session was not dropped and nothing was closed, so the fault has reported a firing it did not perform -- "+
			"a fault that reports it fired while closing nothing is the one thing this package must never do", f.node, req.port)
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
