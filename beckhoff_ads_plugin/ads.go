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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// PlcSymbol holds the configuration and resolved PLC metadata for a single symbol to read.
type PlcSymbol struct {
	Name      string
	MaxDelay  time.Duration
	CycleTime time.Duration

	// Resolved on connect (LoadSymbols) or first read; empty until then.
	DataType       string
	BaseType       string
	Size           uint32
	UnifiedAddress string
}

// AdsCommInput is the Beckhoff ADS Benthos input plugin: connection config
// plus the read requests used to fetch data from the PLC.
type AdsCommInput struct {
	TargetIP       string
	TargetAMS      string
	TargetPort     int
	RuntimePort    int
	HostAMS        string
	HostPort       int
	ReadType       string
	CycleTime      time.Duration
	MaxDelay       time.Duration
	IntervalTime   time.Duration
	RequestTimeout time.Duration

	// Passed through to go-ads; zero duration keeps its default.
	MaxReconnectInterval       time.Duration
	RouteActivationTimeout     time.Duration
	NotificationSilenceTimeout time.Duration
	HeartbeatRecovery          string

	client           Client
	Log              *service.Logger
	Symbols          []PlcSymbol
	symbolByName     map[string]*PlcSymbol // configured symbol name → *PlcSymbol, populated in initSymbolIndex (Connect)
	NotificationChan chan *Update
	TransmissionMode int

	// pendingInitial holds the initial notification samples captured during Connect's
	// readiness wait; flushed by the first read so the first value isn't lost.
	pendingInitial []*Update

	// warnedBatchFailures dedupes per-symbol batch-read warnings for one session.
	warnedBatchFailures map[string]bool

	// degradedReason is written by a go-ads goroutine and read by the Benthos read
	// goroutine, which consumes it and rebuilds. nil means healthy.
	degradedReason atomic.Pointer[string]

	// Connect gate. Benthos drives Connect and ReadBatch from one goroutine, so
	// these need no locking. Any successful connect clears all three.
	connectFailures int
	routeFaults     int
	nextAttempt     time.Time
	skipRouteUntil  time.Time

	LoadSymbols bool // download full symbol+datatype table on connect; required for struct/array symbols

	// Route registration; route registered when both Username and Password are set.
	Username string
	Password string
	HostIP   string // IP address the PLC uses to reach this client (auto-detected if empty)
}

// transmissionModeValue maps the transmissionMode config string to a plain int code;
// the go-ads adapter translates the code to adsLib.TransMode.
func transmissionModeValue(s string) int {
	switch s {
	case "serverOnChange":
		return 0
	case "serverCycle":
		return 1
	case "serverOnChange2":
		return 2
	case "serverCycle2":
		return 3
	default:
		return 0
	}
}

// NewAdsCommInput creates a new ADS input plugin from parsed Benthos configuration.
func NewAdsCommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	targetAddress, err := conf.FieldString("targetAddress")
	if err != nil {
		return nil, err
	}

	targetAMS, err := conf.FieldString("targetAMS")
	if err != nil {
		return nil, err
	}

	targetIP, targetPort, err := parseTargetAddress(targetAddress)
	if err != nil {
		return nil, fmt.Errorf("targetAddress: %w", err)
	}
	// Empty is legal: go-ads then asks the PLC for its own NetID on connect.
	if targetAMS != "" {
		if err = validateAMSNetID(targetAMS); err != nil {
			return nil, fmt.Errorf("targetAMS: %w", err)
		}
	}

	runtimePort, err := conf.FieldInt("runtimePort")
	if err != nil {
		return nil, err
	}
	if runtimePort < 0 || runtimePort > 65535 {
		return nil, fmt.Errorf("runtimePort %d out of range 0–65535", runtimePort)
	}

	hostAMS, err := conf.FieldString("hostAMS")
	if err != nil {
		return nil, err
	}

	if hostAMS != "auto" && hostAMS != "" {
		if err = validateAMSNetID(hostAMS); err != nil {
			return nil, fmt.Errorf("hostAMS: %w", err)
		}
	}

	hostPort, err := conf.FieldInt("hostPort")
	if err != nil {
		return nil, err
	}
	if hostPort < 0 || hostPort > 65535 {
		return nil, fmt.Errorf("hostPort %d out of range 0–65535", hostPort)
	}

	readType, err := conf.FieldString("readType")
	if err != nil {
		return nil, err
	}

	maxDelay, err := conf.FieldDuration("maxDelay")
	if err != nil {
		return nil, err
	}

	cycleTime, err := conf.FieldDuration("cycleTime")
	if err != nil {
		return nil, err
	}

	unifiedAddress, err := conf.FieldStringList("unifiedAddress")
	if err != nil {
		return nil, err
	}

	symbols, err := conf.FieldStringList("symbols")
	if err != nil {
		return nil, err
	}

	if len(symbols) == 0 && len(unifiedAddress) == 0 {
		return nil, fmt.Errorf("at least one of unifiedAddress or symbols is required")
	}

	intervalTime, err := conf.FieldDuration("intervalTime")
	if err != nil {
		return nil, err
	}
	requestTimeout, err := conf.FieldDuration("requestTimeout")
	if err != nil {
		return nil, err
	}

	maxReconnectInterval, err := durationField(conf, "maxReconnectInterval")
	if err != nil {
		return nil, err
	}
	routeActivationTimeout, err := durationField(conf, "routeActivationTimeout")
	if err != nil {
		return nil, err
	}
	notificationSilenceTimeout, err := durationField(conf, "notificationSilenceTimeout")
	if err != nil {
		return nil, err
	}

	heartbeatRecovery, err := conf.FieldString("heartbeatRecovery")
	if err != nil {
		return nil, err
	}
	switch heartbeatRecovery {
	case heartbeatRecoveryImmediate, heartbeatRecoveryConfirm, heartbeatRecoveryRebuild:
	default:
		return nil, fmt.Errorf("heartbeatRecovery %q is not supported (use %q, %q or %q)",
			heartbeatRecovery, heartbeatRecoveryImmediate, heartbeatRecoveryConfirm, heartbeatRecoveryRebuild)
	}

	transmissionModeStr, err := conf.FieldString("transmissionMode")
	if err != nil {
		return nil, err
	}
	transmissionMode := transmissionModeValue(transmissionModeStr)

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	hostIP, err := conf.FieldString("hostIP")
	if err != nil {
		return nil, err
	}
	// Empty means auto-detect on connect. A non-empty value feeds both route
	// registration and the derived hostAMS below, neither of which re-checks it.
	if hostIP != "" {
		if err = validateIP(hostIP); err != nil {
			return nil, fmt.Errorf("hostIP: %w", err)
		}
	}

	loadSymbols, err := conf.FieldBool("loadSymbols")
	if err != nil {
		return nil, err
	}

	if hostAMS == "auto" && hostIP != "" {
		hostAMS = hostIP + ".1.1"
	}

	symbolList, symbolWarnings := CreateSymbolList(symbols, cycleTime, maxDelay)
	unifiedList, unifiedWarnings := CreateSymbolList(unifiedAddress, cycleTime, maxDelay)
	for i := range unifiedList {
		unifiedList[i].UnifiedAddress = unifiedList[i].Name
	}
	symbolList = append(symbolList, unifiedList...)
	for _, w := range append(symbolWarnings, unifiedWarnings...) {
		mgr.Logger().Warnf("%s", w)
	}

	m := &AdsCommInput{
		TargetIP:       targetIP,
		TargetAMS:      targetAMS,
		TargetPort:     targetPort,
		RuntimePort:    runtimePort,
		HostAMS:        hostAMS,
		HostPort:       hostPort,
		ReadType:       readType,
		MaxDelay:       maxDelay,
		CycleTime:      cycleTime,
		Symbols:        symbolList,
		Log:            mgr.Logger(),
		IntervalTime:   intervalTime,
		RequestTimeout: requestTimeout,

		MaxReconnectInterval:       maxReconnectInterval,
		RouteActivationTimeout:     routeActivationTimeout,
		NotificationSilenceTimeout: notificationSilenceTimeout,
		HeartbeatRecovery:          heartbeatRecovery,

		NotificationChan: make(chan *Update, notificationBuffer),
		TransmissionMode: transmissionMode,
		LoadSymbols:      loadSymbols,
		Username:         username,
		Password:         password,
		HostIP:           hostIP,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {
	err := service.RegisterBatchInput(
		"ads", adsConf,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return NewAdsCommInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

// sessionConfig builds the library-agnostic connection spec passed to the
// go-ads adapter.
func (a *AdsCommInput) sessionConfig() SessionConfig {
	return SessionConfig{
		TargetIP:       a.TargetIP,
		TargetAMS:      a.TargetAMS,
		HostIP:         a.HostIP,
		HostAMS:        a.HostAMS,
		TargetPort:     a.TargetPort,
		RuntimePort:    a.RuntimePort,
		HostPort:       a.HostPort,
		Username:       a.Username,
		Password:       a.Password,
		RequestTimeout: a.RequestTimeout,

		MaxReconnectInterval:       a.MaxReconnectInterval,
		RouteActivationTimeout:     a.RouteActivationTimeout,
		NotificationSilenceTimeout: a.NotificationSilenceTimeout,
		HeartbeatRecovery:          a.HeartbeatRecovery,
		SkipRouteRegistration:      time.Now().Before(a.skipRouteUntil),
		OnSessionEvent:             a.onSessionEvent,
	}
}

// onSessionEvent records conditions the library reports but will not repair.
// Only stores and logs: closing the session here would deadlock its watcher.
func (a *AdsCommInput) onSessionEvent(ev SessionEvent, reason string) {
	switch ev {
	case SessionEventSubscriptionsDead:
		a.markDegraded(reason, "notification delivery stopped and the library is not re-subscribing (heartbeatRecovery: rebuild)")
	case SessionEventSymbolReloadGaveUp:
		a.markDegraded(reason, "the library gave up reloading symbols after a PLC online change, so the handles may address symbols that have moved")
	case SessionEventOther:
		// Handled inside the library — a symbol-version reload it is already
		// driving. Worth a line for correlation, not a restart.
		a.Log.With("reason", reason).Debug("ADS session reported a condition the library handles itself")
	default:
		// A SessionEvent added to client.go without a case here. Logged rather
		// than degraded: an unknown severity is not grounds for a rebuild.
		a.Log.With("event", int(ev), "reason", reason).
			Warn("ADS session reported an event this input does not handle; treating it as informational")
	}
}

// markDegraded flags the session for rebuild. First writer wins: that is the
// reason that caused it.
func (a *AdsCommInput) markDegraded(reason string, detail string) {
	if a.degradedReason.CompareAndSwap(nil, &reason) {
		a.Log.With("reason", reason, "detail", detail).
			Warn("ADS session can no longer deliver; rebuilding it on the next read")
	}
}

// rebuildIfDegraded closes a flagged session and reports it disconnected, which
// is what makes Benthos call Connect again.
func (a *AdsCommInput) rebuildIfDegraded() error {
	reason := a.degradedReason.Swap(nil)
	if reason == nil {
		return nil
	}
	a.Log.With("reason", *reason).Info("Closing the ADS session for a rebuild")
	a.closeHandler()
	return service.ErrNotConnected
}

// shuttingDown reports whether Benthos is stopping the pipeline. Operations
// aborted then are expected, so they are logged at debug rather than as faults.
func shuttingDown(ctx context.Context) bool { return ctx.Err() != nil }

// connectHint names the likely cause: a failed dial, one of go-ads' two drop
// verdicts, or a session the PLC rejected at the AMS layer.
func connectHint(err error) string {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return "the PLC did not accept a TCP connection - check targetAddress, that the PLC is powered and reachable, and that no firewall blocks the ADS port"
	}
	switch connectDropKind(err) {
	case dropUnknown: // falls through to the AMS-layer hint below
	case dropRouteNotServed:
		return "the PLC accepted the TCP connection but closed it without serving a single AMS frame - the route is not authorizing this client: check that username/password are valid, that hostIP is the address the PLC sees (set it explicitly behind NAT or a VPN), and that the route is not held by another client on the same host"
	case dropEstablished:
		return "the PLC dropped a connection that was already carrying AMS frames - this is a transport or device-side reset, not a configuration error: check the network path (VPN or subnet router flaps), and whether another client is evicting this one, since a Beckhoff AMS router serves one TCP connection per host and closes the older"
	}
	return "the PLC accepted the TCP connection then rejected the ADS session - check that targetAMS matches the PLC's own AMS NetID, that username/password are valid for route registration, and that runtimePort addresses a running runtime (851 on TwinCAT 3, 801 on TwinCAT 2)"
}

// connLogger attaches the connection parameters as structured fields, verbatim;
// go-ads separately logs what "auto" resolved to.
func (a *AdsCommInput) connLogger() *service.Logger {
	hostPort := strconv.Itoa(a.HostPort)
	if a.HostPort == 0 {
		hostPort = "random"
	}
	return a.Log.With(
		"targetAddress", net.JoinHostPort(a.TargetIP, strconv.Itoa(a.TargetPort)),
		"targetAMS", a.TargetAMS,
		"runtimePort", a.RuntimePort,
		"hostAMS", a.HostAMS,
		"hostPort", hostPort,
		"hostIP", a.HostIP,
	)
}

// Connect establishes the ADS session, resolves symbol metadata, and (depending
// on config) loads the full symbol table and registers notifications.
func (a *AdsCommInput) Connect(ctx context.Context) error {
	if a.client != nil {
		return nil
	}
	if err := a.awaitRetryWindow(ctx); err != nil {
		return err
	}

	c, err := newGoADSClient(ctx, a.sessionConfig(), a.Log)
	if err != nil {
		return err
	}
	// A flag left from the replaced session would tear this one down on first read.
	a.degradedReason.Store(nil)
	a.client = c

	if err = a.finishConnect(ctx); err != nil {
		a.client.Close()
		a.client = nil
		a.noteConnectFailure(err)
		return err
	}
	a.noteConnectSuccess()
	return nil
}

// awaitRetryWindow blocks until the gate set by the previous failure expires.
// It sleeps rather than returning early because Benthos calls Connect again as
// soon as it returns, so a fast return would spin instead of backing off.
func (a *AdsCommInput) awaitRetryWindow(ctx context.Context) error {
	wait := time.Until(a.nextAttempt)
	if wait <= 0 {
		return nil
	}
	a.connLogger().With("wait", wait.Round(time.Second).String(), "failures", a.connectFailures).
		Info("Waiting before the next connect attempt")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

// noteConnectFailure schedules the next attempt. A route the PLC will not serve
// backs off on its own, much longer ramp: retrying cannot fix it, and every
// attempt costs the PLC a route registration and a socket.
func (a *AdsCommInput) noteConnectFailure(err error) {
	a.connectFailures++
	first, ceiling := connectRetryFirst, connectRetryMax
	if isRouteFault(err) {
		a.routeFaults++
		first, ceiling = routeFaultRetryFirst, routeFaultRetryMax
		if a.routeFaults >= routeSkipAfter && a.skipRouteUntil.Before(time.Now()) {
			a.skipRouteUntil = time.Now().Add(routeSkipWindow)
			a.connLogger().With("routeFaults", a.routeFaults, "for", routeSkipWindow.String()).
				Warn("Not asking the PLC to register a route on the next attempts; set hostIP to the address the PLC sees (behind a NATing VPN gateway that is the gateway's own LAN address)")
		}
	} else {
		a.routeFaults = 0
	}
	a.nextAttempt = time.Now().Add(backoffFor(first, ceiling, a.connectFailures))
}

// noteConnectSuccess reopens the gate: a working connect is indistinguishable
// from a fixed network, so nothing is carried over.
func (a *AdsCommInput) noteConnectSuccess() {
	a.connectFailures = 0
	a.routeFaults = 0
	a.nextAttempt = time.Time{}
	a.skipRouteUntil = time.Time{}
}

// backoffFor doubles first up to ceiling for the nth consecutive failure.
func backoffFor(first time.Duration, ceiling time.Duration, n int) time.Duration {
	d := first
	for i := 1; i < n && d < ceiling; i++ {
		d *= 2
	}
	return min(d, ceiling)
}

// finishConnect drives connect → symbol resolution → (optional) symbol table
// load → (optional) notification setup; split out so Connect can clean up a.client on failure.
func (a *AdsCommInput) finishConnect(ctx context.Context) error {
	a.connLogger().Info("Connecting to PLC")
	if err := a.client.Connect(ctx); err != nil {
		return a.logConnectFailure(ctx, "Connecting to PLC failed", err)
	}
	a.Log.Info("Connected to PLC")
	// Before the index: resolving a user-defined type to its primitive needs the
	// datatype table, and an unresolved BaseType is never retried afterwards.
	if a.LoadSymbols {
		if err := a.loadSymbolTable(ctx); err != nil {
			return a.logConnectFailure(ctx, "Loading the symbol and datatype table failed", err)
		}
	}
	a.initSymbolIndex(ctx)
	if a.ReadType == "notification" {
		if err := a.setupNotifications(ctx); err != nil {
			return a.logConnectFailure(ctx, "Registering notifications failed", err)
		}
	}
	return nil
}

// logConnectFailure logs a failed connect step and returns err unchanged. A
// dropped transport heals on the next Connect; a rejected session does not.
func (a *AdsCommInput) logConnectFailure(ctx context.Context, what string, err error) error {
	switch {
	case shuttingDown(ctx):
		a.Log.Debugf("%s during shutdown: %v", what, err)
	case isRouteFault(err):
		// Before isTransportGone: activation wraps the probe's ErrTransportClosed,
		// so a route the PLC will not serve would otherwise read as a blip.
		a.connLogger().With("hint", routeFaultHint).Errorf("%s: %v", what, err)
	case isTransportGone(err):
		// No hint: connectHint's fallback diagnoses a rejected session.
		a.connLogger().Warnf("%s because the PLC dropped the connection; the next connect attempt retries: %v", what, err)
	default:
		a.connLogger().With("hint", connectHint(err)).Errorf("%s: %v", what, err)
	}
	return err
}

// ReadBatch dispatches to the notification or pull read path per ReadType.
func (a *AdsCommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	a.Log.Debugf("ReadBatch called")
	if err := a.rebuildIfDegraded(); err != nil {
		return nil, nil, err
	}
	if a.ReadType == "notification" {
		return a.ReadBatchNotification(ctx)
	}
	return a.ReadBatchPull(ctx)
}

func (a *AdsCommInput) Close(_ context.Context) error {
	if a.client == nil {
		return nil
	}
	a.connLogger().With("readType", a.ReadType, "symbols", len(a.Symbols)).Debug("Closing connection to PLC")

	err := a.client.Close()
	a.client = nil
	if err != nil {
		a.connLogger().Errorf("Closing connection to PLC failed: %v", err)
		return err
	}
	a.Log.Debug("Closed connection to PLC")
	return nil
}
