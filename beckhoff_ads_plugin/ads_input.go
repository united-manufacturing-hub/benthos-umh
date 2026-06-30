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
	"fmt"
	"strconv"
	"strings"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// PlcSymbol holds the configuration for a single PLC symbol to read.
type PlcSymbol struct {
	Name      string
	MaxDelay  time.Duration
	CycleTime time.Duration
}

// validateIP checks that s is a valid IPv4 address (4 dot-separated octets, each 0–255).
func validateIP(s string) error {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return fmt.Errorf("%q must have 4 dot-separated octets", s)
	}
	for _, p := range parts {
		v, err := strconv.Atoi(p)
		if err != nil || v < 0 || v > 255 {
			return fmt.Errorf("%q contains invalid octet %q (must be 0–255)", s, p)
		}
	}
	return nil
}

// parseSymbolDuration parses a per-symbol timing override.
// Bare integers are treated as milliseconds for backward compatibility; otherwise time.ParseDuration is used.
func parseSymbolDuration(raw string) (time.Duration, error) {
	if v, err := strconv.Atoi(raw); err == nil {
		return time.Duration(v) * time.Millisecond, nil
	}
	return time.ParseDuration(raw)
}

// validateAMSNetID checks that s is a valid AMS NetID (6 dot-separated octets, each 0–255).
func validateAMSNetID(s string) error {
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return fmt.Errorf("%q must have 6 dot-separated octets (e.g. 192.168.1.100.1.1)", s)
	}
	for _, p := range parts {
		v, err := strconv.Atoi(p)
		if err != nil || v < 0 || v > 255 {
			return fmt.Errorf("%q contains invalid octet %q (must be 0–255)", s, p)
		}
	}
	return nil
}

// CreateSymbolList parses a list of symbol strings into PlcSymbol structs.
//
// Format: "name[:opt1[:opt2...]]"
//
// Each option is either positional (plain integer) or keyed ("key=value").
// Positional options fill maxDelay then cycleTime in order; keyed options
// override by name regardless of position. Both forms can be mixed.
//
//	"MAIN.var"                           — all defaults
//	"MAIN.var:50:100"                    — maxDelay=50, cycleTime=100 (positional)
//	"MAIN.var:50"                        — maxDelay=50, cycleTime=default
//	"MAIN.var:cycleTime=100"             — maxDelay=default, cycleTime=100
//	"MAIN.var:50:cycleTime=100"          — maxDelay=50 (positional), cycleTime=100 (key)
//	"MAIN.var:maxDelay=50:cycleTime=100" — both keyed, any order
//
// The second return value is a slice of human-readable warning strings for
// malformed options; it is nil when all options are valid. Defaults are kept
// for any option that cannot be parsed.
func CreateSymbolList(s []string, defaultCycleTime time.Duration, defaultMaxDelay time.Duration) ([]PlcSymbol, []string) {
	var result []PlcSymbol
	var warnings []string
	for _, symbol := range s {
		parts := strings.SplitN(symbol, ":", 2)
		plcSym := PlcSymbol{
			Name:      parts[0],
			MaxDelay:  defaultMaxDelay,
			CycleTime: defaultCycleTime,
		}

		if len(parts) == 2 {
			positionalIdx := 0 // 0=maxDelay, 1=cycleTime
			for _, opt := range strings.Split(parts[1], ":") {
				if kv := strings.SplitN(opt, "=", 2); len(kv) == 2 {
					// Keyed option — overrides by name, does not consume positional slot
					if d, err := parseSymbolDuration(kv[1]); err == nil {
						switch kv[0] {
						case "maxDelay":
							plcSym.MaxDelay = d
						case "cycleTime":
							plcSym.CycleTime = d
						}
					} else {
						warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring invalid %s value %q (using default)", symbol, kv[0], kv[1]))
					}
				} else {
					// Positional option — always advances slot index.
					// Empty string reserves the slot (keeps default), non-empty sets value.
					if opt != "" {
						if positionalIdx >= 2 {
							warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring extra positional option %q (only maxDelay:cycleTime supported)", symbol, opt))
						} else if d, err := parseSymbolDuration(opt); err == nil {
							switch positionalIdx {
							case 0:
								plcSym.MaxDelay = d
							case 1:
								plcSym.CycleTime = d
							}
						} else {
							warnings = append(warnings, fmt.Sprintf("symbol %q: ignoring invalid positional option %q (using default)", symbol, opt))
						}
					}
					positionalIdx++
				}
			}
		}

		result = append(result, plcSym)
	}
	return result, warnings
}

// AdsCommInput defines the structure for the Beckhoff ADS Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Beckhoff PLC,
// along with the read requests to fetch data from the PLC.
type AdsCommInput struct {
	TargetIP         string              // IP address of the PLC.
	TargetAMS        string              // Target AMS net ID.
	TargetPort       int                 // Target port
	RuntimePort      int                 // Target runtime port. Default 801(Twincat 2)
	HostAMS          string              // The host AMS net ID, auto (default) automatically derives AMS from host IP. Enter manually if auto not working
	HostPort         int                 // AMS source port (0 = random per session, recommended)
	ReadType         string              // Read type, interval or notification
	CycleTime        time.Duration       // Cycle time for notification handler or interval read
	MaxDelay         time.Duration       // Max delay after value change before PLC sends notification
	IntervalTime     time.Duration       // Time duration before a connection attempt or read request times out.
	RequestTimeout   time.Duration       // Timeout for individual ADS requests.
	Handler          *adsLib.Session     // TCP handler to manage the connection.
	Log              *service.Logger     // Logger for logging plugin activity.
	Symbols          []PlcSymbol         // List of items to read from the PLC
	dataTypes        map[string]string   // configured symbol name → PLC data type (symbolic, e.g. "E_MachineState"), populated on connect
	baseTypes        map[string]string   // configured symbol name → resolved primitive (e.g. "DINT" for an INT-aliased enum)
	dataSizes        map[string]uint32   // configured symbol name → PLC reported byte length (STRING=Nbytes, primitives=2/4/8 etc)
	symbolNames      map[string]string   // strings.ToLower(configured name) → configured name (TC2 returns uppercase)
	NotificationChan chan *adsLib.Update // notification channel for PLC data
	TransmissionMode adsLib.TransMode    // Notification transmission mode

	// Shutdown signal - closed to signal goroutines to stop.
	// Used instead of closing NotificationChan to avoid send-on-closed-channel panics
	// from the go-ads library's notification goroutines.
	done chan struct{}

	LoadSymbols bool // download full symbol+datatype table on connect; required for struct/array symbols

	// Route registration settings
	Username string // PLC route registration username; route registered when both Username and Password are set
	Password string
	HostIP   string // IP address the PLC uses to reach this client (auto-detected if empty)
}

var adsConf = service.NewConfigSpec().
	Summary("Creates an input that reads data from Beckhoff PLCs using ADS protocol.").
	Description("This input plugin enables Benthos to read data directly from Beckhoff PLCs using the ADS protocol. " +
		"Configure the plugin by specifying the PLC's IP address, runtime port, target AMS net ID, and symbols to read.").
	// ---- Target connection (required) ----
	Field(service.NewStringField("targetIP").Description("IP address of the Beckhoff PLC.")).
	Field(service.NewStringField("targetAMS").Description("AMS net ID of the target PLC runtime (e.g. '192.168.1.100.1.1').")).
	Field(service.NewIntField("runtimePort").Description("ADS runtime port. TwinCAT 3: 851, TwinCAT 2: 801.").Default(851).Advanced().Examples(851, 801)).
	Field(service.NewIntField("targetPort").Description("TCP port of the PLC ADS gateway.").Default(48898).Advanced().Examples(48898)).
	// ---- Local AMS identity ----
	Field(service.NewStringField("hostAMS").Description("Local AMS net ID sent in ADS requests. 'auto' derives it from the outbound TCP source IP (or hostIP when set).").Default("auto").Advanced().Examples("auto")).
	Field(service.NewIntField("hostPort").Description("AMS source port in protocol headers. 0 uses a random port per session (recommended). Set fixed only in firewalled environments.").Default(0).Advanced().Examples(0, 10500)).
	Field(service.NewStringField("hostIP").Description("IP address the PLC uses to reach this client. Required in Docker bridge networking. When hostAMS is auto, derives NetID as hostIP+.1.1.").Default("").Advanced().Examples("192.168.1.50")).
	// ---- Route registration ----
	Field(service.NewStringField("username").Description("PLC username for automatic route registration. Both username and password must be set to activate. Requires UDP 48899.").Default("").Advanced().Examples("Administrator")).
	Field(service.NewStringField("password").Description("PLC password for automatic route registration.").Default("").Advanced().Secret().Examples("1")).
	// ---- Read mode ----
	Field(service.NewStringEnumField("readType", "notification", "interval").Description("Read type. notification = PLC pushes on change; interval = plugin polls at intervalTime.").Default("notification").Advanced().Examples("notification", "interval")).
	Field(service.NewStringEnumField("transmissionMode", "serverOnChange", "serverCycle", "serverOnChange2", "serverCycle2").Description("Notification transmission mode (notification readType only). serverOnChange2/serverCycle2 auto-fall back on older PLCs.").Default("serverOnChange").Advanced().Examples("serverOnChange", "serverCycle", "serverOnChange2", "serverCycle2")).
	Field(service.NewDurationField("cycleTime").Description("How often the PLC checks the symbol for changes and delivers notifications. Lower = more responsive but more PLC CPU.").Default("100ms").Advanced().Examples("100ms", "10ms", "500ms", "1s")).
	Field(service.NewDurationField("maxDelay").Description("Maximum time the PLC batches notifications before sending. All changes are delivered; this controls delivery latency vs network efficiency.").Default("100ms").Advanced().Examples("100ms", "0s", "500ms")).
	Field(service.NewDurationField("intervalTime").Description("Poll interval for readType interval.").Default("1s").Advanced().Examples("1s", "500ms")).
	// ---- Advanced ----
	Field(service.NewDurationField("requestTimeout").Description("Timeout for individual ADS requests.").Default("5s").Advanced().Examples("5s", "10s")).
	Field(service.NewBoolField("loadSymbols").Description("Download the full symbol and datatype table from the PLC on connect. Required for struct and array symbols. May cause brief real-time jitter on the PLC during initial connection; use with care on large programs.").Default(false).Advanced().Examples(true, false)).
	// ---- Symbols (list — placed last for readability in YAML) ----
	Field(service.NewStringListField("symbols").Description("Symbols to read. Format: 'name', 'name:maxDelayMs:cycleTimeMs', or 'name:maxDelay=100ms:cycleTime=100ms'. " +
		"Examples: 'GVL.counter', 'GVL.trigger:0s:10ms', '.globalVar:maxDelay=0s:cycleTime=50ms'"))

// NewAdsCommInput creates a new ADS input plugin from parsed Benthos configuration.
func NewAdsCommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	targetIP, err := conf.FieldString("targetIP")
	if err != nil {
		return nil, err
	}

	targetAMS, err := conf.FieldString("targetAMS")
	if err != nil {
		return nil, err
	}

	if err = validateIP(targetIP); err != nil {
		return nil, fmt.Errorf("targetIP: %w", err)
	}
	if err = validateAMSNetID(targetAMS); err != nil {
		return nil, fmt.Errorf("targetAMS: %w", err)
	}

	targetPort, err := conf.FieldInt("targetPort")
	if err != nil {
		return nil, err
	}
	if targetPort < 0 || targetPort > 65535 {
		return nil, fmt.Errorf("targetPort %d out of range 0–65535", targetPort)
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

	symbols, err := conf.FieldStringList("symbols")
	if err != nil {
		return nil, err
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("symbols: at least one symbol is required")
	}

	intervalTime, err := conf.FieldDuration("intervalTime")
	if err != nil {
		return nil, err
	}
	requestTimeout, err := conf.FieldDuration("requestTimeout")
	if err != nil {
		return nil, err
	}

	transmissionModeStr, err := conf.FieldString("transmissionMode")
	if err != nil {
		return nil, err
	}
	var transmissionMode adsLib.TransMode
	switch transmissionModeStr {
	case "serverOnChange":
		transmissionMode = adsLib.TransModeServerOnChange
	case "serverCycle":
		transmissionMode = adsLib.TransModeServerCycle
	case "serverOnChange2":
		transmissionMode = adsLib.TransModeServerOnChange2
	case "serverCycle2":
		transmissionMode = adsLib.TransModeServerCycle2
	default:
		transmissionMode = adsLib.TransModeServerOnChange
	}

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

	loadSymbols, err := conf.FieldBool("loadSymbols")
	if err != nil {
		return nil, err
	}

	// Derive hostAMS from hostIP when set to "auto"
	if hostAMS == "auto" && hostIP != "" {
		hostAMS = hostIP + ".1.1"
	}

	symbolList, symbolWarnings := CreateSymbolList(symbols, cycleTime, maxDelay)
	for _, w := range symbolWarnings {
		mgr.Logger().Warnf("%s", w)
	}
	m := &AdsCommInput{
		TargetIP:         targetIP,
		TargetAMS:        targetAMS,
		TargetPort:       targetPort,
		RuntimePort:      runtimePort,
		HostAMS:          hostAMS,
		HostPort:         hostPort,
		ReadType:         readType,
		MaxDelay:         maxDelay,
		CycleTime:        cycleTime,
		Symbols:          symbolList,
		Log:              mgr.Logger(),
		IntervalTime:     intervalTime,
		RequestTimeout:   requestTimeout,
		NotificationChan: make(chan *adsLib.Update, 256),
		done:             make(chan struct{}),
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

// Connect is implemented in the connectivity change (PR2). Stub keeps the
// package compiling and the input registrable while the stack is built.
func (g *AdsCommInput) Connect(_ context.Context) error {
	return service.ErrNotConnected
}

// ReadBatch is implemented in the reading change (PR3).
func (g *AdsCommInput) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, service.ErrNotConnected
}

// Close is implemented in the connectivity change (PR2).
func (g *AdsCommInput) Close(_ context.Context) error {
	return nil
}
