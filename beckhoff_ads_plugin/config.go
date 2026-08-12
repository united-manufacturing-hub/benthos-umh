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
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var adsConf = service.NewConfigSpec().
	Summary("Creates an input that reads data from Beckhoff PLCs using ADS protocol.").
	Description("This input plugin enables Benthos to read data directly from Beckhoff PLCs using the ADS protocol. " +
		"Configure the plugin by specifying the PLC's IP address, runtime port, target AMS net ID, and symbols to read.").
	Field(service.NewStringField("targetAddress").Description("IP address (and optional port) of the PLC's ADS gateway, as 'ip' or 'ip:port'. Port defaults to 48898.").Examples("192.168.1.100", "192.168.1.100:48898")).
	Field(service.NewStringField("targetAMS").Description("AMS net ID of the target PLC runtime (e.g. '192.168.1.100.1.1').")).
	Field(service.NewIntField("runtimePort").Description("ADS runtime port. TwinCAT 3: 851, TwinCAT 2: 801.").Default(851).Advanced().Examples(851, 801)).
	Field(service.NewStringField("hostAMS").Description("Local AMS net ID sent in ADS requests. 'auto' derives it from the outbound TCP source IP (or hostIP when set).").Default("auto").Advanced().Examples("auto")).
	Field(service.NewIntField("hostPort").Description("AMS source port in protocol headers. 0 uses a random port per session (recommended). Set fixed only in firewalled environments.").Default(0).Advanced().Examples(0, 10500)).
	Field(service.NewStringField("hostIP").Description("IP address the PLC uses to reach this client. Required in Docker bridge networking. When hostAMS is auto, derives NetID as hostIP+.1.1.").Default("").Advanced().Examples("192.168.1.50")).
	Field(service.NewStringField("username").Description("PLC username for automatic route registration. Both username and password must be set to activate. Requires UDP 48899.").Default("").Advanced().Examples("Administrator")).
	Field(service.NewStringField("password").Description("PLC password for automatic route registration.").Default("").Advanced().Secret().Examples("1")).
	Field(service.NewStringEnumField("readType", "notification", "interval").Description("Read type. notification = PLC pushes on change; interval = plugin polls at intervalTime.").Default("notification").Advanced().Examples("notification", "interval")).
	Field(service.NewStringEnumField("transmissionMode", "serverOnChange", "serverCycle", "serverOnChange2", "serverCycle2").Description("Notification transmission mode (notification readType only). serverOnChange2/serverCycle2 auto-fall back on older PLCs.").Default("serverOnChange").Advanced().Examples("serverOnChange", "serverCycle", "serverOnChange2", "serverCycle2")).
	Field(service.NewDurationField("cycleTime").Description("How often the PLC checks the symbol for changes and delivers notifications. Lower = more responsive but more PLC CPU.").Default("100ms").Advanced().Examples("100ms", "10ms", "500ms", "1s")).
	Field(service.NewDurationField("maxDelay").Description("Maximum time the PLC batches notifications before sending. All changes are delivered; this controls delivery latency vs network efficiency.").Default("100ms").Advanced().Examples("100ms", "0s", "500ms")).
	Field(service.NewDurationField("intervalTime").Description("Poll interval for readType interval.").Default("1s").Advanced().Examples("1s", "500ms")).
	Field(service.NewDurationField("requestTimeout").Description("Timeout for individual ADS requests.").Default("5s").Advanced().Examples("5s", "10s")).
	Field(service.NewBoolField("loadSymbols").Description("Download the full symbol and datatype table from the PLC on connect. Required for struct and array symbols. May cause brief real-time jitter on the PLC during initial connection; use with care on large programs.").Default(false).Advanced().Examples(true, false)).
	Field(service.NewStringListField("symbols").Description("Symbols to read. Format: 'name', 'name:maxDelayMs:cycleTimeMs', or 'name:maxDelay=100ms:cycleTime=100ms'. " +
		"Examples: 'GVL.counter', 'GVL.trigger:0s:10ms', '.globalVar:maxDelay=0s:cycleTime=50ms'")).
	Field(service.NewStringListField("unifiedAddress").Description("Symbols in unified address form; same parsing as `symbols`.").Default([]string{}).Advanced())

// validateIP checks that s is a valid IPv4 address. Is4 also rejects the
// IPv4-mapped IPv6 form (::ffff:192.168.1.1), which the PLC cannot route.
func validateIP(s string) error {
	addr, err := netip.ParseAddr(s)
	if err != nil || !addr.Is4() {
		return fmt.Errorf("%q is not a valid IPv4 address", s)
	}
	return nil
}

// parseTargetAddress splits targetAddress into an IPv4 host and port. A bare
// IP (no ":port") defaults to defaultTargetPort.
func parseTargetAddress(s string) (ip string, port int, err error) {
	host, portStr, splitErr := net.SplitHostPort(s)
	if splitErr != nil {
		host, portStr = s, strconv.Itoa(defaultTargetPort)
	}
	if err = validateIP(host); err != nil {
		return "", 0, err
	}
	port, err = strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("port %q out of range 0–65535", portStr)
	}
	return host, port, nil
}

// parseSymbolDuration parses a per-symbol timing override.
// Bare integers are treated as milliseconds for backward compatibility; otherwise time.ParseDuration is used.
func parseSymbolDuration(raw string) (time.Duration, error) {
	if v, err := strconv.Atoi(raw); err == nil {
		return time.Duration(v) * time.Millisecond, nil
	}
	return time.ParseDuration(raw)
}

// validateAMSNetID checks that s is a valid AMS NetID: 6 dot-separated octets.
// Beckhoff specifies 1–255 for the trailing suffix (conventionally .1.1).
func validateAMSNetID(s string) error {
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return fmt.Errorf("%q must have 6 dot-separated octets (e.g. 192.168.1.100.1.1)", s)
	}
	ipPart, suffix := strings.Join(parts[:4], "."), parts[4:]

	if err := validateIP(ipPart); err != nil {
		return fmt.Errorf("%q: first four octets %q are not a valid IPv4 address", s, ipPart)
	}
	for _, octet := range suffix {
		v, err := strconv.ParseUint(octet, 10, 8)
		if err != nil || v == 0 {
			return fmt.Errorf("%q contains invalid octet %q (must be 1–255)", s, octet)
		}
	}
	return nil
}

// parseSymbolOptions applies a symbol's option list onto sym (positional
// maxDelay:cycleTime, or keyed "key=value"); fullSpec names the offender in warnings.
func parseSymbolOptions(opts []string, fullSpec string, sym *PlcSymbol) []string {
	var warnings []string
	positionalIdx := 0 // 0=maxDelay, 1=cycleTime
	for _, opt := range opts {
		// Keyed option — overrides by name, does not consume a positional slot.
		if kv := strings.SplitN(opt, "=", 2); len(kv) == 2 {
			key, value := kv[0], kv[1]
			var target *time.Duration
			switch key {
			case "maxDelay":
				target = &sym.MaxDelay
			case "cycleTime":
				target = &sym.CycleTime
			default:
				warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring unknown option %q (supported: maxDelay, cycleTime)", fullSpec, key))
				continue
			}
			d, err := parseSymbolDuration(value)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring invalid %s value %q (using default)", fullSpec, key, value))
				continue
			}
			*target = d
			continue
		}

		// Positional option — always advances the slot index.
		// Empty string reserves the slot (keeps the default).
		slot := positionalIdx
		positionalIdx++
		if opt == "" {
			continue
		}
		if slot >= 2 {
			warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring extra positional option %q (only maxDelay:cycleTime supported)", fullSpec, opt))
			continue
		}
		d, err := parseSymbolDuration(opt)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring invalid positional option %q (using default)", fullSpec, opt))
			continue
		}
		switch slot {
		case 0:
			sym.MaxDelay = d
		case 1:
			sym.CycleTime = d
		}
	}
	return warnings
}

// CreateSymbolList parses a list of symbol strings ("name[:opt...]") into
// PlcSymbol structs. The second return value holds warnings for malformed
// options (nil if all valid); defaults are kept for options that fail to parse.
func CreateSymbolList(s []string, defaultCycleTime time.Duration, defaultMaxDelay time.Duration) ([]PlcSymbol, []string) {
	var (
		result   []PlcSymbol
		warnings []string
	)
	for _, symbol := range s {
		parts := strings.Split(symbol, ":")
		plcSym := PlcSymbol{
			Name:      parts[0],
			MaxDelay:  defaultMaxDelay,
			CycleTime: defaultCycleTime,
		}
		warnings = append(warnings, parseSymbolOptions(parts[1:], symbol, &plcSym)...)
		result = append(result, plcSym)
	}
	return result, warnings
}
