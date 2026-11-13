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

// The Kinds this input logs under. Which line gets which Kind is part of the
// contract with umh-core, section ADS:
// https://linear.app/united-manufacturing-hub/document/plugin-error-classes-contract-between-benthos-umh-and-umh-core-e51f0744258b
var (
	ads = errclass.NewPlugin("ads")

	kindConnectionDropped    = ads.Define("connection_dropped", errclass.Known)
	kindSessionDisconnected  = ads.Define("session_disconnected", errclass.Known)
	kindHeartbeatResubscribe = ads.Define("heartbeat_resubscribe", errclass.Known)
	kindResubscribeFailed    = ads.Define("resubscribe_failed", errclass.Known)
	kindHeartbeatObserveOnly = ads.Define("heartbeat_observe_only", errclass.Known)
	kindTargetUnreachable    = ads.Define("target_unreachable", errclass.Known)
	kindTargetPortNotFound   = ads.Define("target_port_not_found", errclass.Known)
	kindReconnectFailing     = ads.Define("reconnect_failing", errclass.Known)
	kindRouteFault           = ads.Define("route_fault", errclass.Known)
	kindPLCNotRunning        = ads.Define("plc_not_running", errclass.Known)
	kindSymbolConfig         = ads.Define("symbol_config", errclass.Known)
	kindInvalidHostAMS       = ads.Define("invalid_host_ams", errclass.Known)
	kindInvalidTargetAMS     = ads.Define("invalid_target_ams", errclass.Known)
	kindSessionCreateFailed  = ads.Define("session_create_failed", errclass.Bug)
	kindSymbolCacheMiss      = ads.Define("symbol_cache_miss", errclass.Bug)
	kindNoInitialSamples     = ads.Define("no_initial_samples", errclass.Bug)
	kindReadGap              = ads.Define("read_gap", errclass.Bug)
	kindBatchReadFallback    = ads.Define("batch_read_fallback", errclass.Known)
	kindReadTimeout          = ads.Define("read_timeout", errclass.Known)
	kindCloseFailed          = ads.Define("close_failed", errclass.Known)
	kindProtocolDecode       = ads.Define("protocol_decode", errclass.Bug)
	kindInternalBug          = ads.Define("internal_bug", errclass.Bug)
	kindUnclassified         = ads.Define("unclassified", errclass.Unknown)
)

// errNoSymbolsRegistered: every configured symbol failed to register for
// notifications, which is a symbol name or PLC program problem.
var errNoSymbolsRegistered = errors.New("no symbols registered for notifications")

// errInvalidHostAMS and errInvalidTargetAMS: the configured NetID does not
// parse, which the customer fixes in the config.
var (
	errInvalidHostAMS   = errors.New("hostAMS is not a valid AMS NetID")
	errInvalidTargetAMS = errors.New("targetAMS is not a valid AMS NetID")
)

// errSessionCreate: go-ads could not create the session. Without a targetAMS,
// NewSession first discovers the target over UDP, and a discovery the device
// does not answer matches the network rules before this one. What is left is
// go-ads rejecting the session options. Config parsing validates everything
// go-ads checks there, so reaching it is a bug in that validation.
var errSessionCreate = errors.New("creating the ADS session failed")

// classifier is where every Warn and Error line of this input gets its Kind:
// the Logger it returns (a.Log) applies it. Call Classify directly only where
// the Kind decides what the code does next, or where the line's error decides
// its Kind but does not belong in its text.
var classifier = &errclass.Classifier{Rules: errorRules, Unclassified: kindUnclassified}

// errorRules is the type half of the contract table. The first matching rule
// wins, so this is a slice, not a map: one error can match several rules, and
// the earlier rule is the more specific cause. A route the PLC will not serve,
// for example, also wraps a closed transport.
var errorRules = []errclass.Rule{
	{Matches: errclass.Is(errNoSymbolsRegistered), Kind: kindSymbolConfig},
	{Matches: errclass.Is(errInvalidHostAMS), Kind: kindInvalidHostAMS},
	{Matches: errclass.Is(errInvalidTargetAMS), Kind: kindInvalidTargetAMS},
	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// route-registration text it is wrapped in. Before isUntypedRouteFault.
	{Matches: errclass.Is(adsLib.ErrRouterUnresponsive), Kind: kindTargetUnreachable},
	{Matches: isUntypedRouteFault, Kind: kindRouteFault},
	{Matches: isDialFailure, Kind: kindTargetUnreachable},
	{Matches: isUDPTimeout, Kind: kindTargetUnreachable},
	{Matches: errclass.Is(errSessionCreate), Kind: kindSessionCreateFailed},
	{Matches: errclass.Is(adsLib.ErrRouteNotServed), Kind: kindRouteFault},
	{Matches: errclass.Is(adsLib.ErrRuntimeNotRunning), Kind: kindPLCNotRunning},
	// errors.Is matches both an ADS device verdict (ReturnCode) and an AMS router
	// rejection (AMSError) carrying the same code.
	{Matches: errclass.Is(adsLib.ReturnCodeGlobalTargetPortNotFound), Kind: kindTargetPortNotFound},
	{Matches: errclass.Is(adsLib.ReturnCodeDeviceSymbolNoFound), Kind: kindSymbolConfig},
	{Matches: errclass.Is(adsLib.ErrBatchSymbolUnresolved), Kind: kindSymbolConfig},
	// The symbol cache was reloaded while a batch subscribe was in flight. The
	// symbol name is fine; nothing the customer can change avoids it.
	{Matches: errclass.Is(adsLib.ErrNotificationStrandedByReload), Kind: kindInternalBug},
	{Matches: isTransportGone, Kind: kindConnectionDropped},
	// A request deadline, or a TCP socket deadline on a live session.
	{Matches: errclass.Is(context.DeadlineExceeded), Kind: kindReadTimeout},
	{Matches: errclass.Is(os.ErrDeadlineExceeded), Kind: kindReadTimeout},
}

// goADSMessages give go-ads log records their Kind by their exact message,
// whatever error the record carries. Most are events go-ads only logs, with no
// error to inspect. A go-ads bump that rewords one fails the guard test instead
// of turning it unclassified.
var goADSMessages = map[string]errclass.Kind{
	"PLC dropped an established connection, transport down":                                  kindConnectionDropped,
	"PLC closed connection, transport down":                                                  kindConnectionDropped,
	"listen read error, transport down":                                                      kindConnectionDropped,
	"no frame has arrived on either socket; treating the transport as dead and reconnecting": kindConnectionDropped,
	"session disconnected, reconnecting; no data is read until it succeeds":                  kindSessionDisconnected,
	"connection flapping, applying cross-cycle cooldown before reconnect":                    kindSessionDisconnected,
	"subscriptions are missing; recovering":                                                  kindHeartbeatResubscribe,
	"heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))":                  kindHeartbeatObserveOnly,
	"error getting symbol for batch notification":                                            kindSymbolConfig,
	"notification target symbol no longer in cache; skipping parse":                          kindSymbolCacheMiss,
	"header too short":                                       kindProtocolDecode,
	"Error parsing header":                                   kindProtocolDecode,
	"Error parsing body":                                     kindProtocolDecode,
	"device notification decode failed":                      kindProtocolDecode,
	"failed to parse notification response":                  kindProtocolDecode,
	"error during parse of notification":                     kindProtocolDecode,
	"listen header decode error, transport down":             kindProtocolDecode,
	"AMS packet length exceeds sanity limit, transport down": kindProtocolDecode,
	"SumRead errored-item declared length exceeds remaining bytes — protocol drift": kindProtocolDecode,
	"SumRead per-item undersize — protocol drift":                                   kindProtocolDecode,
	"SumRead per-item oversize — protocol drift or wire corruption":                 kindProtocolDecode,
	"SumRead truncated — likely protocol corruption":                                kindProtocolDecode,
	"binary.Write failed": kindInternalBug,
	"notification batch callback got an out-of-range index":                                                                 kindInternalBug,
	"notification batch: success with a zero handle":                                                                        kindInternalBug,
	"notification send panicked — caller closed the update channel?":                                                        kindInternalBug,
	"orphan delete goroutine panic recovered":                                                                               kindInternalBug,
	"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing": kindHeartbeatResubscribe,
	"connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns":              kindPLCNotRunning,
	"route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it":                               kindPLCNotRunning,
	"re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window":      kindResubscribeFailed,
	"re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window":   kindResubscribeFailed,
}

// goADSMessageFallbacks give the Kind of go-ads records whose error is the
// better signal: the message only applies when no rule matches that error.
var goADSMessageFallbacks = map[string]errclass.Kind{
	"reconnect dial/start failed, retrying": kindReconnectFailing,
	"reconnect step failed, retrying":       kindReconnectFailing,
}

// reasonKinds give the Kind of the go-ads Reason a session event carries. A
// Reason not listed here is unclassified, so a new one reaches Sentry and gets
// a row.
var reasonKinds = map[string]errclass.Kind{
	string(adsLib.ReasonSymbolNotFound):  kindSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): kindSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): kindHeartbeatObserveOnly,
}

// untypedRouteFaults are the texts of the route faults go-ads v2.3.4 returns
// without a sentinel, one per call that creates each:
//   - the PLC refused the registration:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/route.go#L319
//   - the route was registered but never served within the activation budget:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/session.go#L1287
//     It wraps the last probe error, typically ErrTransportClosed, so its rule
//     has to come before isTransportGone.
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

// isUDPTimeout reports an unanswered UDP exchange. ADS uses UDP 48899 only for
// identify (target discovery) and route registration, both before a session
// exists, so a timeout there means the device did not answer at all.
func isUDPTimeout(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && strings.HasPrefix(opErr.Net, "udp") && opErr.Timeout()
}

// isDialFailure reports a TCP or UDP connection that could not be opened.
func isDialFailure(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
