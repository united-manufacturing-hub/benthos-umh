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
	"net"
	"os"
	"strings"

	adsLib "github.com/RuneRoven/go-ads/v2"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// The IDs this input logs under. Which line gets which ID is part of the
// contract with umh-core, the Linear document "Plugin error classes: contract
// between benthos-umh and umh-core", section ADS.
var (
	errConnectionDropped    = errclass.Class{ID: "ads::connection_dropped", Class: errclass.Operational}
	errSessionDisconnected  = errclass.Class{ID: "ads::session_disconnected", Class: errclass.Operational}
	errHeartbeatResubscribe = errclass.Class{ID: "ads::heartbeat_resubscribe", Class: errclass.Operational}
	errResubscribeFailed    = errclass.Class{ID: "ads::resubscribe_failed", Class: errclass.Operational}
	errHeartbeatObserveOnly = errclass.Class{ID: "ads::heartbeat_observe_only", Class: errclass.Known}
	errTargetUnreachable    = errclass.Class{ID: "ads::target_unreachable", Class: errclass.Known}
	errTargetPortNotFound   = errclass.Class{ID: "ads::target_port_not_found", Class: errclass.Known}
	errReconnectFailing     = errclass.Class{ID: "ads::reconnect_failing", Class: errclass.Known}
	errRouteFault           = errclass.Class{ID: "ads::route_fault", Class: errclass.Known}
	errPLCNotRunning        = errclass.Class{ID: "ads::plc_not_running", Class: errclass.Known}
	errSymbolConfig         = errclass.Class{ID: "ads::symbol_config", Class: errclass.Known}
	errSymbolCacheMiss      = errclass.Class{ID: "ads::symbol_cache_miss", Class: errclass.Bug}
	errNoInitialSamples     = errclass.Class{ID: "ads::no_initial_samples", Class: errclass.Bug}
	errReadGap              = errclass.Class{ID: "ads::read_gap", Class: errclass.Bug}
	errBatchReadFallback    = errclass.Class{ID: "ads::batch_read_fallback", Class: errclass.Operational}
	errReadTimeout          = errclass.Class{ID: "ads::read_timeout", Class: errclass.Operational}
	errCloseFailed          = errclass.Class{ID: "ads::close_failed", Class: errclass.Operational}
	errProtocolDecode       = errclass.Class{ID: "ads::protocol_decode", Class: errclass.Bug}
	errInternalBug          = errclass.Class{ID: "ads::internal_bug", Class: errclass.Bug}
	errUnclassified         = errclass.Class{ID: "ads::unclassified", Class: errclass.Unknown}
)

// errNoSymbolsRegistered: every configured symbol failed to register for
// notifications, which is a symbol name or PLC program problem.
var errNoSymbolsRegistered = errors.New("no symbols registered for notifications")

// classifier is where every Warn and Error line of this input gets its class:
// the Logger it returns (a.Log) applies it. Call Classify directly only where
// the class decides what the code does next.
var classifier = &errclass.Classifier{Rules: errorRules, Unclassified: errUnclassified}

// errorRules is the type half of the contract table. The first matching rule
// wins, so this is a slice, not a map: one error can match several rules, and
// the earlier rule is the more specific cause. A route the PLC will not serve,
// for example, also wraps a closed transport.
var errorRules = []errclass.Rule{
	{Matches: errclass.Is(errNoSymbolsRegistered), Class: errSymbolConfig},
	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// route-registration text it is wrapped in. Before isUntypedRouteFault.
	{Matches: errclass.Is(adsLib.ErrRouterUnresponsive), Class: errTargetUnreachable},
	{Matches: isUntypedRouteFault, Class: errRouteFault},
	{Matches: errclass.IsDialFailure, Class: errTargetUnreachable},
	{Matches: isUDPTimeout, Class: errTargetUnreachable},
	{Matches: errclass.Is(adsLib.ErrRouteNotServed), Class: errRouteFault},
	{Matches: errclass.Is(adsLib.ErrRuntimeNotRunning), Class: errPLCNotRunning},
	// errors.Is matches both an ADS device verdict (ReturnCode) and an AMS router
	// rejection (AMSError) carrying the same code.
	{Matches: errclass.Is(adsLib.ReturnCodeGlobalTargetPortNotFound), Class: errTargetPortNotFound},
	{Matches: errclass.Is(adsLib.ReturnCodeDeviceSymbolNoFound), Class: errSymbolConfig},
	{Matches: errclass.Is(adsLib.ErrBatchSymbolUnresolved), Class: errSymbolConfig},
	// The symbol cache was reloaded while a batch subscribe was in flight. The
	// symbol name is fine; nothing the customer can change avoids it.
	{Matches: errclass.Is(adsLib.ErrNotificationStrandedByReload), Class: errInternalBug},
	{Matches: isTransportGone, Class: errConnectionDropped},
	// A request deadline, or a TCP socket deadline on a live session.
	{Matches: errclass.Is(context.DeadlineExceeded), Class: errReadTimeout},
	{Matches: errclass.Is(os.ErrDeadlineExceeded), Class: errReadTimeout},
}

// goADSMessages classify go-ads log records by their exact message, whatever
// error the record carries. Most are events go-ads only logs, with no error to
// inspect. A go-ads bump that rewords one fails the guard test instead of
// turning it unclassified.
var goADSMessages = map[string]errclass.Class{
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
// signal: the message is only the fallback passed to Classify.
var goADSMessageFallbacks = map[string]errclass.Class{
	"reconnect dial/start failed, retrying": errReconnectFailing,
	"reconnect step failed, retrying":       errReconnectFailing,
}

// reasonClasses classify the go-ads Reason a session event carries. A Reason
// not listed here is unclassified, so a new one reaches Sentry and gets a row.
var reasonClasses = map[string]errclass.Class{
	string(adsLib.ReasonSymbolNotFound):  errSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): errSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): errHeartbeatObserveOnly,
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

// isUDPTimeout reports an unanswered UDP exchange. ADS uses UDP 48899 only for
// identify (target discovery) and route registration, both before a session
// exists, so a timeout there means the device did not answer at all.
func isUDPTimeout(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && strings.HasPrefix(opErr.Net, "udp") && opErr.Timeout()
}
