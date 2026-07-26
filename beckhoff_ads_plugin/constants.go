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
)
