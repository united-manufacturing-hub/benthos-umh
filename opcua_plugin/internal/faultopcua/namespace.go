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

// faultNamespace is where a fault takes effect: it sits in front of the real
// gopcua namespace and answers first, so a read or a write meets the fault
// before it meets the node. Unexported, and the only place a fault kind is
// interpreted.
//
// In the real package this implements gopcua's namespace interface: Attribute
// for reads, SetAttribute for writes.
type faultNamespace struct {
	*server.NodeNameSpace

	mu     sync.Mutex
	faults map[string]*Fault // keyed by the node's StringID; one armed fault per node
	vars   map[string]*Variable

	// closed releases anything NeverAnswer is holding, so a stuck read ends
	// with the server rather than outliving the test that armed it.
	closed chan struct{}

	// onDrop cuts the connection underneath the client. The proxy keeps the
	// count, because only the proxy can publish it before the close it
	// records becomes visible to the client.
	onDrop func(port int) int

	// reqPort is the proxy port of the connection whose request the server's
	// single service loop is running right now. The read and write handlers
	// set it before dispatching to the namespace and clear it after, so a
	// DropSession firing inside a request cuts the connection that touched
	// the node rather than every connection there is.
	reqPort int

	// flap alternates FlapValue between the real value and the fault's.
	flap bool
}

// Attribute is the read side.
//
// A read reaches here for every attribute, not only the value, so a fault
// that refused all of them would also refuse the browse and type reads a
// client makes while connecting, and the test would fail before reaching
// what it meant to test. Faults therefore apply to the value attribute only.
func (ns *faultNamespace) Attribute(id *ua.NodeID, attr ua.AttributeID) *ua.DataValue {
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
		ns.drop()
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
		select {
		case <-time.After(f.delay):
		case <-ns.closed:
		}
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
		if ns.toggleFlap() {
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

// SetAttribute is the write side. A node with no fault armed is written
// through to the real namespace, which is what makes this server usable as
// its own control.
func (ns *faultNamespace) SetAttribute(id *ua.NodeID, attr ua.AttributeID, val *ua.DataValue) ua.StatusCode {
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
		ns.drop()
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
		select {
		case <-time.After(f.delay):
		case <-ns.closed:
		}
		return ns.NodeNameSpace.SetAttribute(id, attr, val)
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

// read and write are the server's Read and Write service handlers. They do
// what gopcua's own do -- dispatch each node of the request to the namespace
// it lives in and answer with the same response -- with one addition: they
// tell the namespace which connection is making the request. gopcua calls a
// namespace with only the node and the attribute, so without this the
// namespace could not know whose session a DropSession is supposed to cut,
// and would have to cut everyone's.
func (s *Server) read(sc *uasc.SecureChannel, r ua.Request, _ uint32) (ua.Response, error) {
	req, ok := r.(*ua.ReadRequest)
	if !ok {
		return nil, ua.StatusBadRequestTypeInvalid
	}
	s.ns.servePort(portOf(sc))
	defer s.ns.servePort(0)
	results := make([]*ua.DataValue, len(req.NodesToRead))
	for i, n := range req.NodesToRead {
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
	s.ns.servePort(portOf(sc))
	defer s.ns.servePort(0)
	status := make([]ua.StatusCode, len(req.NodesToWrite))
	for i := range req.NodesToWrite {
		n := req.NodesToWrite[i]
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

// portOf names a secure channel by the port its TCP connection comes from.
// Every client connects through the proxy, so from the server's side of the
// hop that port is the proxy's, and the proxy knows which of its connections
// it is.
func portOf(sc *uasc.SecureChannel) int {
	if sc == nil {
		return 0
	}
	if tcp, ok := sc.RemoteAddr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
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

// servePort records whose request the single service loop is running. Set by
// the read and write handlers around each dispatch into the namespace.
func (ns *faultNamespace) servePort(port int) {
	ns.mu.Lock()
	ns.reqPort = port
	ns.mu.Unlock()
}

// drop cuts the client's connection. The proxy counts what it closed.
func (ns *faultNamespace) drop() {
	if ns.onDrop == nil {
		return
	}
	ns.mu.Lock()
	port := ns.reqPort
	ns.mu.Unlock()
	ns.onDrop(port)
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

// toggleFlap flips the shared phase FlapValue alternates on, under the lock,
// because two requests can race for it even though the service loop cannot.
func (ns *faultNamespace) toggleFlap() bool {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.flap = !ns.flap
	return ns.flap
}
