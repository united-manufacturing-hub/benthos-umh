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

import "time"

// Timing and sizing values. Only adsDiscoveryPort is fixed by the protocol; the
// rest are our own, and the comments say where each number actually comes from.
const (
	// Bounds the host-IP probe dial so a dead or firewalled PLC fails Connect in
	// seconds instead of the OS TCP timeout. 3s from trial and error: long enough
	// to absorb normal network latency, short enough that Benthos retries quickly.
	routeDialTimeout = 3 * time.Second

	// Paces retries after a transient read error so a persistently failing PLC
	// cannot spin the read loop. Conventional value, not measured.
	pullRetryBackoff = 100 * time.Millisecond

	// Caps how long a notification read blocks without data, so ReadBatch returns
	// to Benthos for context and shutdown checks even when no symbol changes.
	// 3s is a safe upper bound, not a tuned one — reducing it only costs empty wakeups.
	notificationWait = 3 * time.Second

	// Margin added on top of a symbol's cycleTime + maxDelay when Connect waits for
	// its first notification sample; covers slow links and slow PLCs. Symbols still
	// missing when it elapses are warned about, they do not fail the connect.
	initialSampleWait = 10 * time.Second

	// NotificationChan depth. go-ads sends non-blocking and drops on a full channel,
	// so this has to absorb a burst of simultaneous updates; go-ads' own docs suggest
	// sizing this per workload (their example uses 1024). Raise it if drops appear.
	notificationBuffer = 256

	// Fixed by TwinCAT: AMS route registration and discovery always use UDP 48899.
	adsDiscoveryPort = "48899"

	// TwinCAT's default ADS gateway (route) port; used when targetAddress omits a port.
	defaultTargetPort = 48898

	// Connect retry gate. Benthos re-calls Connect the moment it returns, and
	// each call is a fresh go-ads session, so the library's per-session cap on
	// route registrations does not bound what a retry loop asks of the PLC.
	connectRetryFirst = 1 * time.Second
	connectRetryMax   = 1 * time.Minute

	// A route the PLC will not serve needs a person, so it backs off much
	// further: measured 172 registrations and 232 dials in 5 minutes against one
	// PLC with a wrong hostIP, which is how a route table gets wedged.
	routeFaultRetryFirst = 30 * time.Second
	routeFaultRetryMax   = 5 * time.Minute

	// After this many consecutive route faults, stop asking the PLC to register
	// at all for routeSkipWindow; registration is the operation that writes to
	// its table.
	routeSkipAfter  = 3
	routeSkipWindow = 5 * time.Minute

	// routeFaultHint is the one remedy for a route the PLC will not serve, and it
	// is the same sentence the registration breaker logs.
	routeFaultHint = "the PLC accepted the connection but will not serve this route - set hostIP to the address the PLC sees; behind a NATing VPN gateway or subnet router that is the gateway's own LAN address, not this client's"

	// heartbeatRecovery values, checked in NewAdsCommInput: outside `benthos
	// lint` a string enum is not enforced, so a typo would read as deliberate.
	heartbeatRecoveryImmediate = "immediate"
	heartbeatRecoveryConfirm   = "confirm"
	heartbeatRecoveryRebuild   = "rebuild"
)
