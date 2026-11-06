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

	adsLib "github.com/RuneRoven/go-ads/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Every Warn and Error line this input emits carries error_id and error_class.
// umh-core reads only those two fields to decide what reaches Sentry, so the IDs
// are part of the Sentry fingerprint and must not change once shipped. The
// contract, including which line gets which ID, is the Linear document "ADS
// error classes: contract between benthos-umh and umh-core".
const (
	// classBug: only UMH can fix it. Reported to Sentry.
	classBug = "bug"
	// classUnknown: nothing matched. Reported to Sentry so it gets classified.
	classUnknown = "unknown"
	// classKnown: the customer can fix it (configuration, PLC state, network).
	classKnown = "known"
	// classOperational: transient, a retry fixes it.
	classOperational = "operational"
)

// errClass is one row of the contract: a stable ID and the class it belongs to.
type errClass struct {
	ID    string
	Class string
}

var (
	errConnectionDropped    = errClass{"ads::connection_dropped", classOperational}
	errSessionDisconnected  = errClass{"ads::session_disconnected", classOperational}
	errHeartbeatResubscribe = errClass{"ads::heartbeat_resubscribe", classOperational}
	errResubscribeFailed    = errClass{"ads::resubscribe_failed", classOperational}
	errHeartbeatObserveOnly = errClass{"ads::heartbeat_observe_only", classKnown}
	errTargetUnreachable    = errClass{"ads::target_unreachable", classKnown}
	errTargetPortNotFound   = errClass{"ads::target_port_not_found", classKnown}
	errReconnectFailing     = errClass{"ads::reconnect_failing", classKnown}
	errRouteFault           = errClass{"ads::route_fault", classKnown}
	errPLCNotRunning        = errClass{"ads::plc_not_running", classKnown}
	errSymbolConfig         = errClass{"ads::symbol_config", classKnown}
	errSymbolCacheMiss      = errClass{"ads::symbol_cache_miss", classBug}
	errNoInitialSamples     = errClass{"ads::no_initial_samples", classBug}
	errReadGap              = errClass{"ads::read_gap", classBug}
	errBatchReadFallback    = errClass{"ads::batch_read_fallback", classOperational}
	errReadTimeout          = errClass{"ads::read_timeout", classOperational}
	errCloseFailed          = errClass{"ads::close_failed", classOperational}
	errProtocolDecode       = errClass{"ads::protocol_decode", classBug}
	errInternalBug          = errClass{"ads::internal_bug", classBug}
	errUnclassified         = errClass{"ads::unclassified", classUnknown}
)

// errNoSymbolsRegistered: every configured symbol failed to register for
// notifications, which is a symbol name or PLC program problem.
var errNoSymbolsRegistered = errors.New("no symbols registered for notifications")

// classify returns the contract row for one Warn or Error line. Every such line
// in this package gets its row from here, attached with on:
//
//	classify(err, errSymbolConfig).on(a.Log).Warnf(...)
//
// err is the error the line reports, or nil. If its type matches a row in
// errorRules, that row wins: the error knows the cause better than the call
// site. Otherwise fallback applies: the row this call site stands for, or
// errUnclassified if it stands for nothing in particular. A zero fallback also
// means errUnclassified, so a missed lookup in a table can be passed straight in.
func classify(err error, fallback errClass) errClass {
	if err != nil {
		for _, r := range errorRules {
			if r.matches(err) {
				return r.class
			}
		}
	}
	if fallback == (errClass{}) {
		return errUnclassified
	}
	return fallback
}

// on returns l with the two contract fields attached.
func (c errClass) on(l *service.Logger) *service.Logger {
	return l.With("error_id", c.ID, "error_class", c.Class)
}

// errorRule maps errors that match onto a contract row.
type errorRule struct {
	matches func(error) bool
	class   errClass
}

// errorRules is the type half of the contract table. The first matching rule
// wins, so this is a slice, not a map: one error can match several rules, and
// the earlier rule is the more specific cause. A route the PLC will not serve,
// for example, also wraps a closed transport.
var errorRules = []errorRule{
	{is(errNoSymbolsRegistered), errSymbolConfig},
	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// route-registration text it is wrapped in. Before isUntypedRouteFault.
	{is(adsLib.ErrRouterUnresponsive), errTargetUnreachable},
	{isUntypedRouteFault, errRouteFault},
	{isDialFailure, errTargetUnreachable},
	{isUDPTimeout, errTargetUnreachable},
	{is(adsLib.ErrRouteNotServed), errRouteFault},
	{is(adsLib.ErrRuntimeNotRunning), errPLCNotRunning},
	// errors.Is matches both an ADS device verdict (ReturnCode) and an AMS router
	// rejection (AMSError) carrying the same code.
	{is(adsLib.ReturnCodeGlobalTargetPortNotFound), errTargetPortNotFound},
	{is(adsLib.ReturnCodeDeviceSymbolNoFound), errSymbolConfig},
	{is(adsLib.ErrBatchSymbolUnresolved), errSymbolConfig},
	// The symbol cache was reloaded while a batch subscribe was in flight. The
	// symbol name is fine; nothing the customer can change avoids it.
	{is(adsLib.ErrNotificationStrandedByReload), errInternalBug},
	{isTransportGone, errConnectionDropped},
	// A request deadline, or a TCP socket deadline on a live session.
	{is(context.DeadlineExceeded), errReadTimeout},
	{is(os.ErrDeadlineExceeded), errReadTimeout},
}

// goADSMessages classify go-ads log records by their exact message, whatever
// error the record carries. Most are events go-ads only logs, with no error to
// inspect. A go-ads bump that rewords one fails the guard test instead of
// turning it unclassified.
var goADSMessages = map[string]errClass{
	"PLC dropped an established connection, transport down":                                  errConnectionDropped,
	"PLC closed connection, transport down":                                                  errConnectionDropped,
	"listen read error, transport down":                                                      errConnectionDropped,
	"no frame has arrived on either socket; treating the transport as dead and reconnecting": errConnectionDropped,
	"session disconnected, reconnecting; no data is read until it succeeds":                  errSessionDisconnected,
	"connection flapping, applying cross-cycle cooldown before reconnect":                    errSessionDisconnected,
	"subscriptions are missing; recovering":                                                  errHeartbeatResubscribe,
	"heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))":                  errHeartbeatObserveOnly,
	"error getting symbol for batch notification":                                            errSymbolConfig,
	"notification target symbol no longer in cache; skipping parse":                          errSymbolCacheMiss,
	"header too short":                                       errProtocolDecode,
	"Error parsing header":                                   errProtocolDecode,
	"Error parsing body":                                     errProtocolDecode,
	"device notification decode failed":                      errProtocolDecode,
	"failed to parse notification response":                  errProtocolDecode,
	"error during parse of notification":                     errProtocolDecode,
	"listen header decode error, transport down":             errProtocolDecode,
	"AMS packet length exceeds sanity limit, transport down": errProtocolDecode,
	"SumRead errored-item declared length exceeds remaining bytes — protocol drift": errProtocolDecode,
	"SumRead per-item undersize — protocol drift":                                   errProtocolDecode,
	"SumRead per-item oversize — protocol drift or wire corruption":                 errProtocolDecode,
	"SumRead truncated — likely protocol corruption":                                errProtocolDecode,
	"binary.Write failed": errInternalBug,
	"notification batch callback got an out-of-range index":                                                                 errInternalBug,
	"notification batch: success with a zero handle":                                                                        errInternalBug,
	"notification send panicked — caller closed the update channel?":                                                        errInternalBug,
	"orphan delete goroutine panic recovered":                                                                               errInternalBug,
	"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing": errHeartbeatResubscribe,
	"connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns":              errPLCNotRunning,
	"route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it":                               errPLCNotRunning,
	"re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window":      errResubscribeFailed,
	"re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window":   errResubscribeFailed,
}

// goADSMessageFallbacks classify go-ads records whose error is the better
// signal: the message is only the fallback passed to classify.
var goADSMessageFallbacks = map[string]errClass{
	"reconnect dial/start failed, retrying": errReconnectFailing,
	"reconnect step failed, retrying":       errReconnectFailing,
}

// reasonClasses classify the go-ads Reason a session event carries. A Reason
// not listed here is unclassified, so a new one reaches Sentry and gets a row.
var reasonClasses = map[string]errClass{
	string(adsLib.ReasonSymbolNotFound):  errSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): errSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): errHeartbeatObserveOnly,
}

// is matches err and anything wrapping it, by errors.Is.
func is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// isUntypedRouteFault matches the route faults go-ads v2.3.4 returns without a
// sentinel, by the text of the one call that creates each:
//   - route.go:319, the PLC refused the registration:
//     fmt.Errorf("route registration failed with error code: %d", ...)
//   - session.go:1287, the route was registered but never served within the
//     activation budget. It wraps the last probe error, typically
//     ErrTransportClosed, so this rule has to come before isTransportGone.
func isUntypedRouteFault(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "route registration failed with error code") ||
		strings.Contains(msg, "was registered but the PLC did not serve it")
}

// isDialFailure reports a TCP or UDP connection that could not be opened.
func isDialFailure(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// isUDPTimeout reports an unanswered UDP exchange. ADS uses UDP 48899 only for
// identify (target discovery) and route registration, both before a session
// exists, so a timeout there means the device did not answer at all.
func isUDPTimeout(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && strings.HasPrefix(opErr.Net, "udp") && opErr.Timeout()
}

// classifiedError carries its class in the error text so the line Benthos core
// logs for a failed Connect ("Failed to connect to ads: ...") carries it too.
// Unwrap keeps errors.Is working for Benthos and for the connect gate.
type classifiedError struct {
	class errClass
	err   error
}

func (e *classifiedError) Error() string {
	return fmt.Sprintf("[error_id=%s error_class=%s] %v", e.class.ID, e.class.Class, e.err)
}

func (e *classifiedError) Unwrap() error { return e.err }

// withClassPrefix prefixes err with its class, once.
func withClassPrefix(err error) error {
	if err == nil {
		return nil
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return err
	}
	return &classifiedError{class: classify(err, errUnclassified), err: err}
}
