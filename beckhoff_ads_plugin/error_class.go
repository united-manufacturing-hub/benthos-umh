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
	"log/slog"
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

// on returns l with the two contract fields attached.
func (c errClass) on(l *service.Logger) *service.Logger {
	return l.With("error_id", c.ID, "error_class", c.Class)
}

// classifyRule maps errors matching cause onto a contract row.
type classifyRule struct {
	cause func(error) bool
	class errClass
}

// is matches err and anything wrapping it, by errors.Is.
func is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// classifyRules is the type half of the contract table. The first matching
// rule wins, so this is a slice, not a map: one error can match several rules,
// and the earlier rule is the more specific cause. A route the PLC will not
// serve, for example, also wraps a closed transport.
var classifyRules = []classifyRule{
	{is(errNoSymbolsRegistered), errSymbolConfig},
	{is(adsLib.ErrRouterUnresponsive), errTargetUnreachable},
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

// classify maps an error onto the contract by type alone.
func classify(err error) errClass {
	if err == nil {
		return errUnclassified
	}
	for _, r := range classifyRules {
		if r.cause(err) {
			return r.class
		}
	}
	return errUnclassified
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

// classifyOr is classify with a call-site default for errors no type matches.
func classifyOr(err error, fallback errClass) errClass {
	if c := classify(err); c != errUnclassified {
		return c
	}
	return fallback
}

// untypedRouteFaults are the route faults go-ads v2.3.4 returns without a
// sentinel, matched by the text of the one call that creates each:
//   - route.go:319, the PLC refused the registration:
//     fmt.Errorf("route registration failed with error code: %d", ...)
//   - session.go:1287, the route was registered but never served within the
//     activation budget. It wraps the last probe error, typically
//     ErrTransportClosed, which is why it is matched before classify.
var untypedRouteFaults = []string{
	"route registration failed with error code",
	"was registered but the PLC did not serve it",
}

// classifyConnect is classify plus the route faults go-ads returns untyped.
func classifyConnect(err error) errClass {
	if err == nil {
		return errUnclassified
	}
	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// text it is wrapped in.
	if !errors.Is(err, adsLib.ErrRouterUnresponsive) {
		msg := err.Error()
		for _, s := range untypedRouteFaults {
			if strings.Contains(msg, s) {
				return errRouteFault
			}
		}
	}
	return classify(err)
}

// reasonClasses maps the go-ads Reason a session event carries. A Reason not
// listed here is unclassified, so a new one reaches Sentry and gets a row.
var reasonClasses = map[adsLib.Reason]errClass{
	adsLib.ReasonSymbolNotFound:  errSymbolConfig,
	adsLib.ReasonSymbolNotActive: errSymbolConfig,
	adsLib.ReasonHeartbeatSilent: errHeartbeatObserveOnly,
}

func classifyReason(reason string) errClass {
	if c, ok := reasonClasses[adsLib.Reason(reason)]; ok {
		return c
	}
	return errUnclassified
}

// goADSMessages classifies go-ads log records by their exact message. These are
// events go-ads only logs, so there is no error value to inspect. A go-ads bump
// that rewords one fails the guard test instead of turning it unclassified.
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

// goADSMessageDefaults classify go-ads records whose error value is the better
// signal: the message applies only when classify finds nothing in the error.
var goADSMessageDefaults = map[string]errClass{
	"reconnect dial/start failed, retrying": errReconnectFailing,
	"reconnect step failed, retrying":       errReconnectFailing,
}

// classifyRecord picks the contract row for a go-ads record: the message table
// first, then the record's error value, then the message defaults.
func classifyRecord(msg string, attrs []slog.Attr) errClass {
	if c, ok := goADSMessages[msg]; ok {
		return c
	}
	for _, a := range attrs {
		if err, ok := a.Value.Any().(error); ok {
			if c := classify(err); c != errUnclassified {
				return c
			}
		}
	}
	if c, ok := goADSMessageDefaults[msg]; ok {
		return c
	}
	return errUnclassified
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

// withConnectClass prefixes err with its connect classification once.
func withConnectClass(err error) error {
	if err == nil {
		return nil
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return err
	}
	return &classifiedError{class: classifyConnect(err), err: err}
}
