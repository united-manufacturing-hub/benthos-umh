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

package beckhoff_ads_plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"

	adsLib "github.com/RuneRoven/go-ads/v2"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// The Kinds of the warnings and errors this input logs. This input reads from
// Beckhoff PLCs over ADS, Beckhoff's protocol for its TwinCAT controllers,
// through the go-ads client library (adsLib).
//
// The Kinds and owners below are the contract umh-core relies on: umh-core
// groups and routes the lines of this input by them. Renaming a Kind or
// changing its owner changes that contract. error_class_test.go pins every
// Kind and owner.
var (
	ads = errclass.NewComponent("ads")

	kindConnectionDropped    = ads.UserKind("connection_dropped")
	kindSessionDisconnected  = ads.UserKind("session_disconnected")
	kindHeartbeatResubscribe = ads.UserKind("heartbeat_resubscribe")
	kindResubscribeFailed    = ads.UserKind("resubscribe_failed")
	kindHeartbeatObserveOnly = ads.UserKind("heartbeat_observe_only")
	kindTargetUnreachable    = ads.UserKind("target_unreachable")
	kindTargetPortNotFound   = ads.UserKind("target_port_not_found")
	kindReconnectFailing     = ads.UserKind("reconnect_failing")
	kindRouteFault           = ads.UserKind("route_fault")
	kindPLCNotRunning        = ads.UserKind("plc_not_running")
	kindSymbolConfig         = ads.UserKind("symbol_config")
	kindInvalidHostAMS       = ads.UserKind("invalid_host_ams")
	kindInvalidTargetAMS     = ads.UserKind("invalid_target_ams")
	kindInvalidConfig        = ads.UserKind("invalid_config")
	kindSessionCreateFailed  = ads.UMHKind("session_create_failed")
	kindSymbolCacheMiss      = ads.UMHKind("symbol_cache_miss")
	kindNoInitialSamples     = ads.UMHKind("no_initial_samples")
	kindReadGap              = ads.UMHKind("read_gap")
	kindBatchReadFallback    = ads.UserKind("batch_read_fallback")
	kindReadTimeout          = ads.UserKind("read_timeout")
	kindCloseFailed          = ads.UserKind("close_failed")
	kindProtocolDecode       = ads.UMHKind("protocol_decode")
	kindInternalBug          = ads.UMHKind("internal_bug")
)

// classifier gives every warning and error of this input its Kind. The
// errclass.Logger uses it for the errors this input logs, and for the records
// go-ads logs through Logger.Slog.
//
// The Kind is for logs only. Whether to retry, how long to back off and at
// which level to log are decided by isRouteFault, isTransportGone and
// isReadTimeout.
//
// The first matching rule wins. One error can match several rules, so the
// rule for the more specific cause comes first. For example, a route the PLC
// will not serve also wraps a closed transport.
//
// MessageIs rules match the exact text of a go-ads log record. If a new go-ads
// version rewords one, a test in error_class_test.go fails. Errors this input
// creates itself carry their Kind (kindX.Errorf) and need no rule.
var classifier = ads.Classifier(
	// go-ads records that the message classifies, whatever error they carry.
	// Most are events that go-ads only logs, with no error to inspect.
	errclass.MessageIs("PLC dropped an established connection, transport down", kindConnectionDropped),
	errclass.MessageIs("PLC closed connection, transport down", kindConnectionDropped),
	errclass.MessageIs("listen read error, transport down", kindConnectionDropped),
	errclass.MessageIs("no frame has arrived on either socket; treating the transport as dead and reconnecting", kindConnectionDropped),
	errclass.MessageIs("listen body read error, transport down", kindConnectionDropped),
	errclass.MessageIs("error setting the write deadline, transport down", kindConnectionDropped),
	errclass.MessageIs("error sending data on conn, transport down", kindConnectionDropped),
	errclass.MessageIs("error flushing data on conn, transport down", kindConnectionDropped),
	errclass.MessageIs("error clearing the write deadline, transport down", kindConnectionDropped),
	errclass.MessageIs("session disconnected, reconnecting; no data is read until it succeeds", kindSessionDisconnected),
	errclass.MessageIs("connection flapping, applying cross-cycle cooldown before reconnect", kindSessionDisconnected),
	errclass.MessageIs("subscriptions are missing; recovering", kindHeartbeatResubscribe),
	errclass.MessageIs("heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))", kindHeartbeatObserveOnly),
	errclass.MessageIs("notification target symbol no longer in cache; skipping parse", kindSymbolCacheMiss),
	errclass.MessageIs("batch read result for symbol no longer in cache; skipping", kindSymbolCacheMiss),
	// The same causes that logBatchFailures and setupNotifications log for
	// the items of a batch read or subscribe.
	errclass.MessageIs("no batch read result for symbol", kindReadGap),
	errclass.MessageIs("error parsing symbol in batch read", kindReadGap),
	errclass.MessageIs("symbol read error in batch", kindSymbolConfig),
	errclass.MessageIs("error adding notification in batch", kindSymbolConfig),
	errclass.MessageIs("duplicate notification rejected (already subscribed)", kindSymbolConfig),
	errclass.MessageIs("duplicate notification rejected (within batch)", kindSymbolConfig),
	errclass.MessageIs("notification symbol gone after reconnect, dropping subscription", kindSymbolConfig),
	// A user-defined type needs loadSymbols: true.
	errclass.MessageIs("cannot resolve the base type of a user-defined type; no datatype table is loaded", kindSymbolConfig),
	errclass.MessageIs("inferring base type from size (no datatype table loaded; LoadSymbols() recommended for user-defined types)", kindSymbolConfig),
	errclass.MessageIs("target NetID differs from the NetID this device reports for itself", kindInvalidTargetAMS),
	errclass.MessageIs("resubscribe: dropping configs after max retries", kindResubscribeFailed),
	errclass.MessageIs("SumReadEx not supported, using individual reads", kindBatchReadFallback),
	errclass.MessageIs("sendRequest aborted due to timeout", kindReadTimeout),
	errclass.MessageIs("max reconnect attempts exhausted, closing session", kindReconnectFailing),
	errclass.MessageIs("header too short", kindProtocolDecode),
	errclass.MessageIs("Error parsing header", kindProtocolDecode),
	errclass.MessageIs("Error parsing body", kindProtocolDecode),
	errclass.MessageIs("device notification decode failed", kindProtocolDecode),
	errclass.MessageIs("failed to parse notification response", kindProtocolDecode),
	errclass.MessageIs("error during parse of notification", kindProtocolDecode),
	errclass.MessageIs("listen header decode error, transport down", kindProtocolDecode),
	errclass.MessageIs("AMS packet length exceeds sanity limit, transport down", kindProtocolDecode),
	errclass.MessageIs("SumRead errored-item declared length exceeds remaining bytes — protocol drift", kindProtocolDecode),
	errclass.MessageIs("SumRead per-item undersize — protocol drift", kindProtocolDecode),
	errclass.MessageIs("SumRead per-item oversize — protocol drift or wire corruption", kindProtocolDecode),
	errclass.MessageIs("SumRead truncated — likely protocol corruption", kindProtocolDecode),
	// A malformed symbol or datatype table uploaded from the PLC.
	errclass.MessageIs("error during binary read", kindProtocolDecode),
	errclass.MessageIs("makeArrayChildren: array Elements exceeds sanity cap, refusing to allocate", kindProtocolDecode),
	errclass.MessageIs("makeArrayChildren: LBound + Elements overflows uint32, refusing", kindProtocolDecode),
	errclass.MessageIs("collectSubtree hit depth cap; possible Children cycle or malformed symbol tree", kindProtocolDecode),
	errclass.MessageIs("addOffset hit depth cap; possible datatype self-cycle in PLC response", kindProtocolDecode),
	errclass.MessageIs("binary.Write failed", kindInternalBug),
	errclass.MessageIs("notification batch callback got an out-of-range index", kindInternalBug),
	errclass.MessageIs("notification batch: success with a zero handle", kindInternalBug),
	errclass.MessageIs("notification send panicked — caller closed the update channel?", kindInternalBug),
	errclass.MessageIs("orphan delete goroutine panic recovered", kindInternalBug),
	errclass.MessageIs("Error during sendRequest encode", kindInternalBug),
	errclass.MessageIs("endHandshake without a matching beginHandshake; clamping", kindInternalBug),
	errclass.MessageIs("FSM invalid transition (ignoring)", kindInternalBug),
	// Options this input builds from a validated configuration.
	errclass.MessageIs("WithBackoff: invalid config, keeping current value", kindInternalBug),
	errclass.MessageIs("WithHeartbeatRecovery: unrecognised mode, keeping the default", kindInternalBug), //nolint:misspell // go-ads spelling
	// go-ads parses a value that it formatted itself.
	errclass.MessageIs("parseTree: invalid BOOL value, defaulting to false", kindInternalBug),
	errclass.MessageIs("parseTree: invalid signed integer value, defaulting to 0", kindInternalBug),
	errclass.MessageIs("parseTree: invalid unsigned integer value, defaulting to 0", kindInternalBug),
	errclass.MessageIs("parseTree: invalid float value, defaulting to 0", kindInternalBug),
	errclass.MessageIs("parseTree: invalid numeric value, defaulting to 0", kindInternalBug),
	errclass.MessageIs("no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing", kindHeartbeatResubscribe),
	errclass.MessageIs("connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns", kindPLCNotRunning),
	errclass.MessageIs("route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it", kindPLCNotRunning),
	errclass.MessageIs("re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window", kindResubscribeFailed),
	errclass.MessageIs("re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window", kindResubscribeFailed),

	// Errors, both returned ones and those in a go-ads record.
	//
	// A router that does not answer is unreachable for a moment, not
	// misconfigured. go-ads wraps this error in a route-registration text
	// that isUntypedRouteFault matches, so this rule comes first.
	errclass.ErrorIs(adsLib.ErrRouterUnresponsive, kindTargetUnreachable),
	errclass.ErrorMatches(isUntypedRouteFault, kindRouteFault),
	errclass.ErrorMatches(isDialFailure, kindTargetUnreachable),
	errclass.ErrorMatches(isUDPUnanswered, kindTargetUnreachable),
	errclass.ErrorIs(adsLib.ErrRouteNotServed, kindRouteFault),
	errclass.ErrorIs(adsLib.ErrRuntimeNotRunning, kindPLCNotRunning),
	// errors.Is matches both an ADS device error (ReturnCode) and a
	// rejection by the AMS router, the message router in front of the PLC
	// (AMSError), that carry the same code.
	errclass.ErrorIs(adsLib.ReturnCodeGlobalTargetPortNotFound, kindTargetPortNotFound),
	// With runtimePort 0, go-ads infers the runtime port from the TwinCAT
	// version the device reports. Without a version, the user has to set the
	// port. go-ads returns this only as text.
	errclass.ErrorContains(noTwinCATVersion, kindTargetPortNotFound),
	errclass.ErrorIs(adsLib.ReturnCodeDeviceSymbolNoFound, kindSymbolConfig),
	errclass.ErrorIs(adsLib.ErrBatchSymbolUnresolved, kindSymbolConfig),
	// The other reasons why go-ads returns no value for an item of a batch
	// read.
	errclass.ErrorIs(adsLib.ErrBatchNoResult, kindReadGap),
	errclass.ErrorIs(adsLib.ErrBatchValueUnparsable, kindReadGap),
	errclass.ErrorIs(adsLib.ErrBatchSymbolVanished, kindReadGap),
	// The reasons why go-ads refuses to register a notification. The user
	// fixes a symbol that is configured twice or that an online change
	// removed. A second update channel is a bug in this input.
	errclass.ErrorIs(adsLib.ErrNotificationDuplicate, kindSymbolConfig),
	errclass.ErrorIs(adsLib.ErrNotificationSymbolVanished, kindSymbolConfig),
	errclass.ErrorIs(adsLib.ErrNotificationChannelMismatch, kindInternalBug),
	// The symbol cache was reloaded during a batch subscribe. The symbol
	// name is fine, and the user cannot change anything to avoid this.
	errclass.ErrorIs(adsLib.ErrNotificationStrandedByReload, kindInternalBug),
	// The ADS transport went away mid-operation. ErrRouteNotServed wraps
	// such an error too, but usually needs credentials or an address
	// fixed, so its rule comes before these.
	errclass.ErrorIs(adsLib.ErrTransportClosed, kindConnectionDropped),
	errclass.ErrorIs(adsLib.ErrDisconnected, kindConnectionDropped),
	errclass.ErrorIs(adsLib.ErrEstablishedDropped, kindConnectionDropped),
	// A batch subscribe whose transport failed. A known cause matched a
	// rule above.
	errclass.ErrorIs(adsLib.ErrNotificationTransportFailure, kindConnectionDropped),
	// A request deadline, or a TCP socket deadline on a live session.
	errclass.ErrorIs(context.DeadlineExceeded, kindReadTimeout),
	errclass.ErrorIs(os.ErrDeadlineExceeded, kindReadTimeout),

	// go-ads records whose error says more than their message. These rules
	// come after the error rules, so they apply only if no error rule matches
	// the record's error.
	errclass.MessageIs("reconnect dial/start failed, retrying", kindReconnectFailing),
	errclass.MessageIs("reconnect step failed, retrying", kindReconnectFailing),
	// go-ads logs this with the error of the symbol lookup, which can also be
	// a dropped connection or a timeout.
	errclass.MessageIs("error getting symbol for batch notification", kindSymbolConfig),
	errclass.MessageIs("error getting symbol for batch read", kindSymbolConfig),
	errclass.MessageIs("on-demand symbol unavailable after reconnect, skipping", kindSymbolConfig),
	errclass.MessageIs("could not dial the PLC", kindTargetUnreachable),
	errclass.MessageIs("send aborted due to timeout", kindReadTimeout),
	errclass.MessageIs("AddDeviceNotification fallback aborted: transport failed mid-batch", kindConnectionDropped),
)

// noTwinCATVersion is part of the text of the error that go-ads v2.3.4 returns
// from NewSession when runtimePort is 0 and the device reports no TwinCAT
// version:
// https://github.com/RuneRoven/go-ads/blob/v2.3.4/session.go#L443
const noTwinCATVersion = "reported no TwinCAT version"

// reasonKinds maps the go-ads Reason of a session event to its Kind. A Reason
// not listed here is logged as unclassified, so a new Reason shows up in
// Sentry and can get a row in the contract.
var reasonKinds = map[string]*errclass.Kind{
	string(adsLib.ReasonSymbolNotFound):  kindSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): kindSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): kindHeartbeatObserveOnly,
}

// degradedError returns the error for a go-ads session event that stops
// delivery. It has the Kind of reason, if reasonKinds lists one.
func degradedError(reason string, detail string) error {
	const msg = "ADS session can no longer deliver (%s: %s); rebuilding it on the next read"
	if kind, ok := reasonKinds[reason]; ok {
		return kind.Errorf(msg, reason, detail)
	}
	return fmt.Errorf(msg, reason, detail)
}

// sessionCreateError wraps an error of adsLib.NewSession. Without a
// targetAMS, NewSession first discovers the target over UDP. A classified
// cause, such as an unanswered discovery, keeps its own Kind. Any other error
// means go-ads rejected options that config parsing already validated, which
// is a bug in that validation.
func sessionCreateError(err error) error {
	const msg = "creating the ADS session failed: %w"
	if classifier.Classify(err) != ads.Unclassified() {
		return fmt.Errorf(msg, err)
	}
	return kindSessionCreateFailed.Errorf(msg, err)
}

// isRouteFault reports whether a connect failed because the PLC would not
// serve this input's route. That is a configuration fault that a retry cannot
// fix. The connect then backs off on the longer route-fault ramp and logs an
// error.
func isRouteFault(err error) bool {
	// A router that does not answer is unreachable for a moment, and there is
	// nothing to correct. go-ads wraps this error in a route-registration text
	// that isUntypedRouteFault matches. Without this check, the long
	// route-fault ramp would keep the bridge down for minutes over an outage
	// of a few seconds.
	if errors.Is(err, adsLib.ErrRouterUnresponsive) {
		return false
	}
	if errors.Is(err, adsLib.ErrRouteNotServed) {
		return true
	}
	return isUntypedRouteFault(err)
}

// isTransportGone reports whether the ADS transport went away mid-operation.
// The next connect or poll recovers from that.
func isTransportGone(err error) bool {
	return errors.Is(err, adsLib.ErrTransportClosed) ||
		errors.Is(err, adsLib.ErrDisconnected) ||
		errors.Is(err, adsLib.ErrEstablishedDropped)
}

// isReadTimeout reports whether a request or a TCP socket ran past its
// deadline: the PLC was reachable but did not reply within requestTimeout.
func isReadTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

// untypedRouteFaults are the texts of the route faults that go-ads v2.3.4
// returns without a sentinel error. Each links to the code that creates it:
//   - the PLC refused the registration:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/route.go#L319
//   - the route was registered but never served within the activation budget:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/session.go#L1287
//     It wraps the last probe error, typically ErrTransportClosed, so its
//     rule comes before the ErrTransportClosed rule.
var untypedRouteFaults = []string{
	"route registration failed with error code",
	"was registered but the PLC did not serve it",
}

func isUntypedRouteFault(err error) bool {
	msg := err.Error()
	for _, text := range untypedRouteFaults {
		if strings.Contains(msg, text) {
			return true
		}
	}
	return false
}

// isUDPUnanswered reports whether a UDP exchange timed out or was refused.
// ADS uses UDP port 48899 only for target discovery and route registration,
// both before a session exists. A timeout or a refusal there means no ADS
// device answered at that address.
func isUDPUnanswered(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !strings.HasPrefix(opErr.Net, "udp") {
		return false
	}
	return opErr.Timeout() || errors.Is(opErr, syscall.ECONNREFUSED)
}

// isDialFailure reports whether err is a TCP or UDP connection that could not
// be opened.
func isDialFailure(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
