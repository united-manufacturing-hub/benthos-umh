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
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// benthosLogHandler is a slog.Handler that forwards go-ads v2 log records to a
// Benthos service.Logger. ADS library verbosity is controlled solely by the
// benthos pipeline log level (logger.level); there is no plugin-level log
// config. Trace-level records (per-packet wire/header dumps) are never
// forwarded — they are not useful inside the pipeline.
type benthosLogHandler struct {
	logger *service.Logger
	attrs  []slog.Attr
}

func (h *benthosLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	// Suppress trace (LevelTrace = -8); forward debug and above to benthos,
	// which then applies its own configured level filter.
	return level >= slog.LevelDebug
}

func (h *benthosLogHandler) Handle(_ context.Context, r slog.Record) error {
	var kvs []any
	for _, a := range h.attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		kvs = append(kvs, a.Key, a.Value.Any())
		return true
	})

	l := h.logger
	if len(kvs) > 0 {
		l = l.With(kvs...)
	}

	switch {
	case r.Level >= slog.LevelError:
		l.Errorf("%s", r.Message)
	case r.Level >= slog.LevelWarn:
		l.Warnf("%s", r.Message)
	case r.Level >= slog.LevelInfo:
		l.Infof("%s", r.Message)
	default:
		l.Debugf("%s", r.Message)
	}
	return nil
}

func (h *benthosLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &benthosLogHandler{logger: h.logger, attrs: append(h.attrs, attrs...)}
}

func (h *benthosLogHandler) WithGroup(_ string) slog.Handler { return h }

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

	// pendingInitial holds the initial notification samples captured during Connect's
	// readiness wait. Flushed by the first ReadBatchNotification so the first value of a
	// static serverOnChange symbol is not lost (go-ads delivers each sample only once).
	pendingInitial []*adsLib.Update

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

func isLikelyContainerIP(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	p := parsed.To4()
	if p == nil {
		return false
	}
	// Docker default bridge: 172.17.0.0/16
	if p[0] == 172 && p[1] >= 17 && p[1] <= 31 {
		return true
	}
	// Common container overlay/pod networks: 10.0.0.0/8
	if p[0] == 10 {
		return true
	}
	// CGNAT range used by some Kubernetes CNIs: 100.64.0.0/10
	if p[0] == 100 && p[1] >= 64 && p[1] <= 127 {
		return true
	}
	return false
}

func (g *AdsCommInput) Connect(ctx context.Context) error {
	if g.Handler != nil {
		return nil
	}

	// Ensure done channel is initialized (may be nil if constructed directly in tests)
	if g.done == nil {
		g.done = make(chan struct{})
	}

	// Build session options — route registration is handled automatically by Session.Connect
	// when WithRoute is set; no manual AddRemoteRoute call needed.
	g.Log.Infof("Creating new connection")
	var err error
	var connOpts []adsLib.SessionOption
	// go-ads verbosity is governed by the benthos pipeline log level via this
	// bridge handler; no global SetDefaultLogger (it would be last-input-wins
	// across multiple ADS inputs).
	adsLogger := slog.New(&benthosLogHandler{logger: g.Log})
	connOpts = append(connOpts, adsLib.WithLogger(adsLogger))
	if g.Username != "" && g.Password != "" {
		hostAddr := g.HostIP
		if hostAddr == "" {
			// Auto-detect via TCP connect to ADS port — guarantees same source IP
			// as the actual ADS connection. On multi-homed machines, UDP routing
			// can pick a different interface, causing the route to be registered
			// with the wrong IP and accumulating stale routes on the PLC.
			dialer := net.Dialer{Timeout: 3 * time.Second}
			tcpConn, dialErr := dialer.DialContext(ctx, "tcp4", net.JoinHostPort(g.TargetIP, strconv.Itoa(g.TargetPort)))
			if dialErr != nil {
				// PLC unreachable; fall back to UDP routing lookup (no packet sent)
				udpConn, udpErr := net.Dial("udp4", net.JoinHostPort(g.TargetIP, "48899"))
				if udpErr != nil {
					g.Log.Errorf("Failed to auto-detect local address: %v", dialErr)
					return dialErr
				}
				hostAddr = udpConn.LocalAddr().(*net.UDPAddr).IP.String()
				udpConn.Close()
			} else {
				hostAddr = tcpConn.LocalAddr().(*net.TCPAddr).IP.String()
				tcpConn.Close()
			}
		}
		if isLikelyContainerIP(hostAddr) {
			g.Log.Warnf("Auto-detected IP %s looks like a container IP. Set hostIP to the Docker host's IP for route registration to work.", hostAddr)
		}
		routeName := fmt.Sprintf("benthosADS-%s", hostAddr)
		g.Log.Infof("Route will be registered on PLC %s: name=%s, clientIP=%s", g.TargetIP, routeName, hostAddr)
		connOpts = append(connOpts, adsLib.WithRoute(routeName, g.Username, g.Password))
		connOpts = append(connOpts, adsLib.WithHostIP(hostAddr))
	}

	targetAMS, err := adsLib.NewAMSAddress(g.TargetAMS, uint16(g.RuntimePort))
	if err != nil {
		g.Log.Errorf("Invalid target AMS %q: %v", g.TargetAMS, err)
		return err
	}
	// "auto" (default) means go-ads derives local AMS from the TCP connection.
	if g.HostAMS != "" && g.HostAMS != "auto" {
		localAMS, lerr := adsLib.NewAMSAddress(g.HostAMS, uint16(g.HostPort))
		if lerr != nil {
			g.Log.Errorf("Invalid local AMS %q: %v", g.HostAMS, lerr)
			return lerr
		}
		connOpts = append(connOpts, adsLib.WithLocalAMS(localAMS))
	}
	if g.RequestTimeout > 0 {
		connOpts = append(connOpts, adsLib.WithRequestTimeout(g.RequestTimeout))
	}

	// NewSession ctx is the session-lifetime ctx (cancel → session shutdown).
	// Benthos passes a per-call ctx to Connect; using it here would tear the
	// session down as soon as Connect returns. Use Background; teardown is
	// driven by AdsCommInput.Close → g.Handler.Close().
	g.Handler, err = adsLib.NewSession(context.Background(), adsLib.AMSEndpoint{
		IP:   g.TargetIP,
		Port: g.TargetPort,
		AMS:  targetAMS,
	}, connOpts...)
	if err != nil {
		g.Log.Errorf("Failed to create connection: %v", err)
		return err
	}

	success := false
	defer func() {
		if !success && g.Handler != nil {
			_ = g.Handler.Close()
			g.Handler = nil
		}
	}()

	// Connect; Session handles route registration internally when WithRoute is set
	g.Log.Infof("Connecting to plc")
	err = g.Handler.Connect(ctx)
	if err != nil {
		g.Log.Errorf("Failed to connect to PLC at %s: %v", g.TargetIP, err)
		return err
	}

	// Build symbolNames map (lower-cased key → configured casing).
	// TC2 returns all symbol names uppercase; this map normalises back to
	// the casing the user configured.
	g.symbolNames = make(map[string]string, len(g.Symbols))
	g.dataTypes = make(map[string]string, len(g.Symbols))
	g.baseTypes = make(map[string]string, len(g.Symbols))
	g.dataSizes = make(map[string]uint32, len(g.Symbols))
	for _, sym := range g.Symbols {
		g.symbolNames[strings.ToLower(sym.Name)] = sym.Name
	}

	if g.LoadSymbols {
		g.Log.Infof("Loading symbol and datatype table from PLC (loadSymbols=true)")
		if err = g.Handler.LoadSymbols(ctx); err != nil {
			g.Log.Errorf("LoadSymbols failed: %v", err)
			return err
		}
		g.Log.Infof("Symbol table loaded")
	}

	if g.ReadType == "notification" {
		// Build notification configs for batch add
		configs := make([]adsLib.NotificationConfig, len(g.Symbols))
		for i, symbol := range g.Symbols {
			configs[i] = adsLib.NotificationConfig{
				SymbolName:       symbol.Name,
				MaxDelay:         symbol.MaxDelay,
				CycleTime:        symbol.CycleTime,
				TransmissionMode: g.TransmissionMode,
			}
		}

		// Connect() already ensures session is stable; no retry needed here.
		// If this fails, return error and let Benthos retry Connect().
		results, err := g.Handler.AddSymbolNotifications(ctx, configs, g.NotificationChan)
		if err != nil {
			g.Log.Errorf("Batch add notifications failed: %v", err)
			return err
		}

		// AddSymbolNotifications returns nil error even when all symbols fail to
		// resolve (e.g. PLC ADS not yet ready after route registration reconnect).
		// Detect this case and return an error so Benthos retries Connect().
		registered := 0
		for i, r := range results {
			switch {
			case r.Skipped == nil && r.Error == adsLib.ReturnCodeNoErrors:
				registered++
			case r.Skipped != nil:
				g.Log.Errorf("Notification symbol %q skipped (check symbol name): %v", configs[i].SymbolName, r.Skipped)
			default:
				g.Log.Errorf("Notification symbol %q rejected by PLC: ADS error 0x%X", configs[i].SymbolName, uint32(r.Error))
			}
		}
		if registered == 0 && len(configs) > 0 {
			return fmt.Errorf("no symbols registered for notifications (%d symbols all failed to resolve)", len(configs))
		}
		g.Log.Infof("Registered %d/%d notification symbols", registered, len(configs))

		// Populate metadata cache from go-ads symbol cache (no extra round-trips).
		// Without this, makeNotificationMessage reads from empty maps → null metadata.
		for _, sym := range g.Symbols {
			key := strings.ToLower(sym.Name)
			if view, viewErr := g.Handler.GetSymbol(ctx, sym.Name); viewErr == nil {
				g.dataTypes[key] = view.DataType
				g.dataSizes[key] = view.Length
				if bt := view.BaseTypeName(); bt != "" {
					g.baseTypes[key] = bt
				}
			} else {
				g.Log.Warnf("Failed to populate metadata cache for ADS symbol %q: %v", sym.Name, viewErr)
			}
		}

		// Wait for an initial sample from each registered symbol (readiness gate).
		// Captured samples are buffered in pendingInitial and flushed by the first
		// ReadBatchNotification so no initial value is lost.
		g.pendingInitial = nil
		needed := make(map[string]bool, registered)
		for i, r := range results {
			if r.Skipped == nil && r.Error == adsLib.ReturnCodeNoErrors {
				needed[strings.ToLower(configs[i].SymbolName)] = true
			}
		}
		initialCtx, initialCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer initialCancel()
		for len(needed) > 0 {
			select {
			case update := <-g.NotificationChan:
				if update != nil {
					g.pendingInitial = append(g.pendingInitial, update)
					delete(needed, strings.ToLower(update.Variable))
				}
			case <-initialCtx.Done():
				g.Log.Warnf("Timed out waiting for initial samples; %d symbols not yet received: %v", len(needed), needed)
				goto doneWaiting
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	doneWaiting:
	}
	success = true
	return nil
}

func (g *AdsCommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Infof("ReadBatch called")
	if g.ReadType == "notification" {
		return g.ReadBatchNotification(ctx)
	}
	return g.ReadBatchPull(ctx)
}

func (g *AdsCommInput) makeNotificationMessage(update *adsLib.Update) *service.Message {
	msg := service.NewMessage([]byte(update.Value))
	name := update.Variable
	if configured, ok := g.symbolNames[strings.ToLower(update.Variable)]; ok {
		name = configured
	}
	msg.MetaSet("symbol_name", name)
	return msg
}

func (g *AdsCommInput) ReadBatchNotification(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Debugf("ReadBatchNotification called")

	// Flush any initial samples captured during Connect before blocking on the channel,
	// so the first values (e.g. static serverOnChange symbols) are delivered.
	if len(g.pendingInitial) > 0 {
		msgs := make(service.MessageBatch, 0, len(g.pendingInitial))
		for _, u := range g.pendingInitial {
			if u != nil {
				msgs = append(msgs, g.makeNotificationMessage(u))
			}
		}
		g.pendingInitial = nil
		return msgs, func(_ context.Context, _ error) error { return nil }, nil
	}

	// Use a short-lived context so ReadBatch returns periodically even when no
	// notifications arrive (e.g. slow-changing symbols). Caller loops immediately.
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var first *adsLib.Update
	select {
	case first = <-g.NotificationChan:
		if first == nil {
			g.Log.Warnf("Received nil update from ADS library, skipping")
			return nil, func(_ context.Context, _ error) error { return nil }, nil
		}
	case <-g.done:
		return nil, nil, service.ErrEndOfInput
	case <-waitCtx.Done():
		if g.Handler != nil && g.Handler.IsClosed() {
			_ = g.Handler.Close()
			g.Handler = nil
			return nil, nil, service.ErrNotConnected
		}
		// No data within timeout — normal for slow-changing symbols or mid-reconnect.
		return nil, func(_ context.Context, _ error) error { return nil }, nil
	}

	msgs := service.MessageBatch{g.makeNotificationMessage(first)}

	// Drain buffered notifications, bounded to a snapshot of the channel depth so a
	// sustained producer can't grow this batch unboundedly. go-ads blocks if the channel
	// fills, so draining keeps buffer space available.
	pending := len(g.NotificationChan)
	for i := 0; i < pending; i++ {
		select {
		case update := <-g.NotificationChan:
			if update != nil {
				msgs = append(msgs, g.makeNotificationMessage(update))
			}
		default:
			return msgs, func(_ context.Context, _ error) error { return nil }, nil
		}
	}
	return msgs, func(_ context.Context, _ error) error { return nil }, nil
}

func (g *AdsCommInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Debugf("ReadBatchPull called")
	start := time.Now()
	if g.Handler == nil {
		return nil, nil, service.ErrNotConnected
	}

	names := make([]string, len(g.Symbols))
	for i, symbol := range g.Symbols {
		names[i] = symbol.Name
	}

	values, err := g.Handler.ReadMultipleSymbols(ctx, names)
	if err != nil {
		g.Log.Errorf("Batch read failed: %v", err)
		if g.Handler.IsClosed() {
			old := g.Handler
			g.Handler = nil
			go func() { _ = old.Close() }()
			return nil, nil, service.ErrNotConnected
		}
		g.Log.Warnf("Batch read failed (will retry): %v", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		return service.MessageBatch{}, func(_ context.Context, _ error) error { return nil }, nil
	}

	msgs := service.MessageBatch{}
	for _, symbol := range g.Symbols {
		val, ok := values[symbol.Name]
		if !ok {
			continue
		}
		valueMsg := service.NewMessage([]byte(val))
		valueMsg.MetaSet("symbol_name", symbol.Name)
		msgs = append(msgs, valueMsg)
	}

	if remaining := g.IntervalTime - time.Since(start); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return msgs, func(_ context.Context, _ error) error { return nil }, nil
}

// Close shuts down the ADS connection. ctx is required by the service.BatchInput interface;
// go-ads does not support context cancellation on close, so it is not forwarded.
//
//nolint:revive
func (g *AdsCommInput) Close(ctx context.Context) error {
	g.Log.Infof("Close called")
	// Signal shutdown to ReadBatchNotification before closing the handler,
	// so it stops waiting for notifications.
	if g.done != nil {
		close(g.done)
		g.done = nil
	}
	if g.Handler != nil {
		g.Log.Infof("Closing down, cleaning up PLC handles")
		if cerr := g.Handler.Close(); cerr != nil {
			g.Log.Warnf("Handler close error: %v", cerr)
		}
		g.Handler = nil
	}

	return nil
}
