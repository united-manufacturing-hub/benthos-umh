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
	ads = errclass.NewNamespace("ads")

	idConnectionDropped    = ads.ID("connection_dropped", errclass.Operational)
	idSessionDisconnected  = ads.ID("session_disconnected", errclass.Operational)
	idHeartbeatResubscribe = ads.ID("heartbeat_resubscribe", errclass.Operational)
	idResubscribeFailed    = ads.ID("resubscribe_failed", errclass.Operational)
	idHeartbeatObserveOnly = ads.ID("heartbeat_observe_only", errclass.Known)
	idTargetUnreachable    = ads.ID("target_unreachable", errclass.Known)
	idTargetPortNotFound   = ads.ID("target_port_not_found", errclass.Known)
	idReconnectFailing     = ads.ID("reconnect_failing", errclass.Known)
	idRouteFault           = ads.ID("route_fault", errclass.Known)
	idPLCNotRunning        = ads.ID("plc_not_running", errclass.Known)
	idSymbolConfig         = ads.ID("symbol_config", errclass.Known)
	idSymbolCacheMiss      = ads.ID("symbol_cache_miss", errclass.Bug)
	idNoInitialSamples     = ads.ID("no_initial_samples", errclass.Bug)
	idReadGap              = ads.ID("read_gap", errclass.Bug)
	idBatchReadFallback    = ads.ID("batch_read_fallback", errclass.Operational)
	idReadTimeout          = ads.ID("read_timeout", errclass.Operational)
	idCloseFailed          = ads.ID("close_failed", errclass.Operational)
	idProtocolDecode       = ads.ID("protocol_decode", errclass.Bug)
	idInternalBug          = ads.ID("internal_bug", errclass.Bug)
	idUnclassified         = ads.ID("unclassified", errclass.Unknown)
)

// errNoSymbolsRegistered: every configured symbol failed to register for
// notifications, which is a symbol name or PLC program problem.
var errNoSymbolsRegistered = errors.New("no symbols registered for notifications")

// classifier is where every Warn and Error line of this input gets its ID:
// the Logger it returns (a.Log) applies it. Call Classify directly only where
// the ID decides what the code does next, or where the line's error decides
// its ID but does not belong in its text.
var classifier = &errclass.Classifier{Rules: errorRules, Unclassified: idUnclassified}

// errorRules is the type half of the contract table. The first matching rule
// wins, so this is a slice, not a map: one error can match several rules, and
// the earlier rule is the more specific cause. A route the PLC will not serve,
// for example, also wraps a closed transport.
var errorRules = []errclass.Rule{
	{Matches: errclass.Is(errNoSymbolsRegistered), ID: idSymbolConfig},
	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// route-registration text it is wrapped in. Before isUntypedRouteFault.
	{Matches: errclass.Is(adsLib.ErrRouterUnresponsive), ID: idTargetUnreachable},
	{Matches: isUntypedRouteFault, ID: idRouteFault},
	{Matches: errclass.IsDialFailure, ID: idTargetUnreachable},
	{Matches: isUDPTimeout, ID: idTargetUnreachable},
	{Matches: errclass.Is(adsLib.ErrRouteNotServed), ID: idRouteFault},
	{Matches: errclass.Is(adsLib.ErrRuntimeNotRunning), ID: idPLCNotRunning},
	// errors.Is matches both an ADS device verdict (ReturnCode) and an AMS router
	// rejection (AMSError) carrying the same code.
	{Matches: errclass.Is(adsLib.ReturnCodeGlobalTargetPortNotFound), ID: idTargetPortNotFound},
	{Matches: errclass.Is(adsLib.ReturnCodeDeviceSymbolNoFound), ID: idSymbolConfig},
	{Matches: errclass.Is(adsLib.ErrBatchSymbolUnresolved), ID: idSymbolConfig},
	// The symbol cache was reloaded while a batch subscribe was in flight. The
	// symbol name is fine; nothing the customer can change avoids it.
	{Matches: errclass.Is(adsLib.ErrNotificationStrandedByReload), ID: idInternalBug},
	{Matches: isTransportGone, ID: idConnectionDropped},
	// A request deadline, or a TCP socket deadline on a live session.
	{Matches: errclass.Is(context.DeadlineExceeded), ID: idReadTimeout},
	{Matches: errclass.Is(os.ErrDeadlineExceeded), ID: idReadTimeout},
}

// goADSMessages give go-ads log records their ID by their exact message, whatever
// error the record carries. Most are events go-ads only logs, with no error to
// inspect. A go-ads bump that rewords one fails the guard test instead of
// turning it unclassified.
var goADSMessages = map[string]errclass.ID{
	"PLC dropped an established connection, transport down":                                  idConnectionDropped,
	"PLC closed connection, transport down":                                                  idConnectionDropped,
	"listen read error, transport down":                                                      idConnectionDropped,
	"no frame has arrived on either socket; treating the transport as dead and reconnecting": idConnectionDropped,
	"session disconnected, reconnecting; no data is read until it succeeds":                  idSessionDisconnected,
	"connection flapping, applying cross-cycle cooldown before reconnect":                    idSessionDisconnected,
	"subscriptions are missing; recovering":                                                  idHeartbeatResubscribe,
	"heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))":                  idHeartbeatObserveOnly,
	"error getting symbol for batch notification":                                            idSymbolConfig,
	"notification target symbol no longer in cache; skipping parse":                          idSymbolCacheMiss,
	"header too short":                                       idProtocolDecode,
	"Error parsing header":                                   idProtocolDecode,
	"Error parsing body":                                     idProtocolDecode,
	"device notification decode failed":                      idProtocolDecode,
	"failed to parse notification response":                  idProtocolDecode,
	"error during parse of notification":                     idProtocolDecode,
	"listen header decode error, transport down":             idProtocolDecode,
	"AMS packet length exceeds sanity limit, transport down": idProtocolDecode,
	"SumRead errored-item declared length exceeds remaining bytes — protocol drift": idProtocolDecode,
	"SumRead per-item undersize — protocol drift":                                   idProtocolDecode,
	"SumRead per-item oversize — protocol drift or wire corruption":                 idProtocolDecode,
	"SumRead truncated — likely protocol corruption":                                idProtocolDecode,
	"binary.Write failed": idInternalBug,
	"notification batch callback got an out-of-range index":                                                                 idInternalBug,
	"notification batch: success with a zero handle":                                                                        idInternalBug,
	"notification send panicked — caller closed the update channel?":                                                        idInternalBug,
	"orphan delete goroutine panic recovered":                                                                               idInternalBug,
	"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing": idHeartbeatResubscribe,
	"connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns":              idPLCNotRunning,
	"route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it":                               idPLCNotRunning,
	"re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window":      idResubscribeFailed,
	"re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window":   idResubscribeFailed,
}

// goADSMessageFallbacks give the ID of go-ads records whose error is the
// better signal: the message only applies when no rule matches that error.
var goADSMessageFallbacks = map[string]errclass.ID{
	"reconnect dial/start failed, retrying": idReconnectFailing,
	"reconnect step failed, retrying":       idReconnectFailing,
}

// reasonIDs give the ID of the go-ads Reason a session event carries. A Reason
// not listed here is unclassified, so a new one reaches Sentry and gets a row.
var reasonIDs = map[string]errclass.ID{
	string(adsLib.ReasonSymbolNotFound):  idSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): idSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): idHeartbeatObserveOnly,
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
