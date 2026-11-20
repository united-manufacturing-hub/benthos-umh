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

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// The Kinds this input logs under. Which line gets which Kind is part of the
// contract with umh-core, section ADS:
// https://linear.app/united-manufacturing-hub/document/plugin-error-classes-contract-between-benthos-umh-and-umh-core-e51f0744258b
var (
	ads = errclass.NewPlugin("ads")

	kindConnectionDropped    = ads.User("connection_dropped")
	kindSessionDisconnected  = ads.User("session_disconnected")
	kindHeartbeatResubscribe = ads.User("heartbeat_resubscribe")
	kindResubscribeFailed    = ads.User("resubscribe_failed")
	kindHeartbeatObserveOnly = ads.User("heartbeat_observe_only")
	kindTargetUnreachable    = ads.User("target_unreachable")
	kindTargetPortNotFound   = ads.User("target_port_not_found")
	kindReconnectFailing     = ads.User("reconnect_failing")
	kindRouteFault           = ads.User("route_fault")
	kindPLCNotRunning        = ads.User("plc_not_running")
	kindSymbolConfig         = ads.User("symbol_config")
	kindInvalidHostAMS       = ads.User("invalid_host_ams")
	kindInvalidTargetAMS     = ads.User("invalid_target_ams")
	kindSessionCreateFailed  = ads.UMH("session_create_failed")
	kindSymbolCacheMiss      = ads.UMH("symbol_cache_miss")
	kindNoInitialSamples     = ads.UMH("no_initial_samples")
	kindReadGap              = ads.UMH("read_gap")
	kindBatchReadFallback    = ads.User("batch_read_fallback")
	kindReadTimeout          = ads.User("read_timeout")
	kindCloseFailed          = ads.User("close_failed")
	kindProtocolDecode       = ads.UMH("protocol_decode")
	kindInternalBug          = ads.UMH("internal_bug")
)

// classifier gives every Warn and Error line of this input its Kind: the
// Logger and the go-ads *slog.Logger it returns apply it. Which line gets which
// Kind is part of the contract with umh-core.
//
// The first matching rule wins, so the order matters: one error can match
// several rules, and the earlier rule is the more specific cause. A route the
// PLC will not serve, for example, also wraps a closed transport.
//
// Message rules match the exact text of a go-ads log record. A go-ads bump that
// rewords one fails the guard test instead of turning it unclassified. Errors
// this input creates itself carry their Kind (kindX.Errorf) and need no rule.
var classifier = ads.Classifier(
	// go-ads records whose message decides, whatever error they carry. Most are
	// events go-ads only logs, with no error to inspect.
	errclass.Message("PLC dropped an established connection, transport down", kindConnectionDropped),
	errclass.Message("PLC closed connection, transport down", kindConnectionDropped),
	errclass.Message("listen read error, transport down", kindConnectionDropped),
	errclass.Message("no frame has arrived on either socket; treating the transport as dead and reconnecting", kindConnectionDropped),
	errclass.Message("session disconnected, reconnecting; no data is read until it succeeds", kindSessionDisconnected),
	errclass.Message("connection flapping, applying cross-cycle cooldown before reconnect", kindSessionDisconnected),
	errclass.Message("subscriptions are missing; recovering", kindHeartbeatResubscribe),
	errclass.Message("heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))", kindHeartbeatObserveOnly),
	errclass.Message("error getting symbol for batch notification", kindSymbolConfig),
	errclass.Message("notification target symbol no longer in cache; skipping parse", kindSymbolCacheMiss),
	errclass.Message("header too short", kindProtocolDecode),
	errclass.Message("Error parsing header", kindProtocolDecode),
	errclass.Message("Error parsing body", kindProtocolDecode),
	errclass.Message("device notification decode failed", kindProtocolDecode),
	errclass.Message("failed to parse notification response", kindProtocolDecode),
	errclass.Message("error during parse of notification", kindProtocolDecode),
	errclass.Message("listen header decode error, transport down", kindProtocolDecode),
	errclass.Message("AMS packet length exceeds sanity limit, transport down", kindProtocolDecode),
	errclass.Message("SumRead errored-item declared length exceeds remaining bytes — protocol drift", kindProtocolDecode),
	errclass.Message("SumRead per-item undersize — protocol drift", kindProtocolDecode),
	errclass.Message("SumRead per-item oversize — protocol drift or wire corruption", kindProtocolDecode),
	errclass.Message("SumRead truncated — likely protocol corruption", kindProtocolDecode),
	errclass.Message("binary.Write failed", kindInternalBug),
	errclass.Message("notification batch callback got an out-of-range index", kindInternalBug),
	errclass.Message("notification batch: success with a zero handle", kindInternalBug),
	errclass.Message("notification send panicked — caller closed the update channel?", kindInternalBug),
	errclass.Message("orphan delete goroutine panic recovered", kindInternalBug),
	errclass.Message("no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing", kindHeartbeatResubscribe),
	errclass.Message("connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns", kindPLCNotRunning),
	errclass.Message("route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it", kindPLCNotRunning),
	errclass.Message("re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window", kindResubscribeFailed),
	errclass.Message("re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window", kindResubscribeFailed),

	// Errors, wherever they show up: returned, or in a go-ads record.

	// A deaf router is unreachable for a moment, not misconfigured, whatever
	// route-registration text it is wrapped in. Before isUntypedRouteFault.
	errclass.Is(adsLib.ErrRouterUnresponsive, kindTargetUnreachable),
	errclass.Match(isUntypedRouteFault, kindRouteFault),
	errclass.Match(isDialFailure, kindTargetUnreachable),
	errclass.Match(isUDPTimeout, kindTargetUnreachable),
	errclass.Is(adsLib.ErrRouteNotServed, kindRouteFault),
	errclass.Is(adsLib.ErrRuntimeNotRunning, kindPLCNotRunning),
	// errors.Is matches both an ADS device verdict (ReturnCode) and an AMS router
	// rejection (AMSError) carrying the same code.
	errclass.Is(adsLib.ReturnCodeGlobalTargetPortNotFound, kindTargetPortNotFound),
	errclass.Is(adsLib.ReturnCodeDeviceSymbolNoFound, kindSymbolConfig),
	errclass.Is(adsLib.ErrBatchSymbolUnresolved, kindSymbolConfig),
	// The other reasons go-ads produced no value for an item of a batch read.
	errclass.Is(adsLib.ErrBatchNoResult, kindReadGap),
	errclass.Is(adsLib.ErrBatchValueUnparsable, kindReadGap),
	errclass.Is(adsLib.ErrBatchSymbolVanished, kindReadGap),
	// The reasons go-ads refused to commit a notification. A symbol configured
	// twice, or one an online change removed, is the customer's to fix; a
	// second update channel would be this input's bug.
	errclass.Is(adsLib.ErrNotificationDuplicate, kindSymbolConfig),
	errclass.Is(adsLib.ErrNotificationSymbolVanished, kindSymbolConfig),
	errclass.Is(adsLib.ErrNotificationChannelMismatch, kindInternalBug),
	// The symbol cache was reloaded while a batch subscribe was in flight. The
	// symbol name is fine; nothing the customer can change avoids it.
	errclass.Is(adsLib.ErrNotificationStrandedByReload, kindInternalBug),
	// The ADS transport went away mid-operation. ErrRouteNotServed above wraps
	// one too, but usually needs credentials or an address fixed.
	errclass.Is(adsLib.ErrTransportClosed, kindConnectionDropped),
	errclass.Is(adsLib.ErrDisconnected, kindConnectionDropped),
	errclass.Is(adsLib.ErrEstablishedDropped, kindConnectionDropped),
	// A batch subscribe whose transport failed; the cause, if any, is above.
	errclass.Is(adsLib.ErrNotificationTransportFailure, kindConnectionDropped),
	// A request deadline, or a TCP socket deadline on a live session.
	errclass.Is(context.DeadlineExceeded, kindReadTimeout),
	errclass.Is(os.ErrDeadlineExceeded, kindReadTimeout),

	// go-ads records whose error is the better signal: the message only
	// decides when no rule above matches that error.
	errclass.Message("reconnect dial/start failed, retrying", kindReconnectFailing),
	errclass.Message("reconnect step failed, retrying", kindReconnectFailing),
)

// reasonKinds give the Kind of the go-ads Reason a session event carries. A
// Reason not listed here is unclassified, so a new one reaches Sentry and gets
// a row.
var reasonKinds = map[string]*errclass.Kind{
	string(adsLib.ReasonSymbolNotFound):  kindSymbolConfig,
	string(adsLib.ReasonSymbolNotActive): kindSymbolConfig,
	string(adsLib.ReasonHeartbeatSilent): kindHeartbeatObserveOnly,
}

// sessionCreateError wraps an error of adsLib.NewSession. Without a targetAMS,
// NewSession first discovers the target over UDP, and a classified cause such
// as an unanswered discovery keeps its own Kind. What is left is go-ads
// rejecting options that config parsing already validates, which is a bug in
// that validation.
func sessionCreateError(err error) error {
	if classifier.IsClassified(err) {
		return fmt.Errorf("creating the ADS session failed: %w", err)
	}
	return kindSessionCreateFailed.Errorf("creating the ADS session failed: %w", err)
}

// isRouteFault reports whether a connect failed because the PLC would not serve
// our route — a configuration fault that a retry cannot fix. It is the same
// verdict as the error_kind, so the backoff and the log cannot disagree.
func isRouteFault(err error) bool {
	return classifier.IsKind(err, kindRouteFault)
}

// untypedRouteFaults are the texts of the route faults go-ads v2.3.4 returns
// without a sentinel, one per call that creates each:
//   - the PLC refused the registration:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/route.go#L319
//   - the route was registered but never served within the activation budget:
//     https://github.com/RuneRoven/go-ads/blob/v2.3.4/session.go#L1287
//     It wraps the last probe error, typically ErrTransportClosed, so its rule
//     has to come before the ErrTransportClosed rule.
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
