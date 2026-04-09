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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/rs/zerolog"

	adsLib "github.com/RuneRoven/go-ads"
)

// PlcSymbol holds the configuration for a single PLC symbol to read.
type PlcSymbol struct {
	Name      string
	MaxDelay  int
	CycleTime int
}

func sanitize(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	return re.ReplaceAllString(s, "_")
}

// isLikelyContainerIP checks if an IP address looks like a Docker/container-internal
// address that is probably not routable from an external PLC network.
// Docker bridge networks typically use 172.17.0.0/16, 172.18-31.0.0/16, or 10.0.0.0/8.
// Kubernetes pod networks commonly use 10.x.x.x or 100.64-127.x.x (CGNAT range).
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

// CreateSymbolList parses a list of symbol strings into PlcSymbol structs.
// Symbols can be plain names ("MAIN.var") or include custom timing ("MAIN.var:maxDelay:cycleTime").
func CreateSymbolList(s []string, defaultCycleTime int, defaultMaxDelay int, upperCase bool) []PlcSymbol {
	var newPlcSymbol []PlcSymbol
	// Iterate through symbol list
	for _, symbol := range s {
		if upperCase {
			symbol = strings.ToUpper(symbol)
		}
		// Count colons in the symbol
		colons := strings.Count(symbol, ":")

		// Create a new PlcSymbol instance
		var plcSym PlcSymbol

		// If there are anything other than two colons, add default value
		if colons != 2 {
			parts := strings.Split(symbol, ":")
			if len(parts) > 0 {
				plcSym.Name = parts[0]
			}
			plcSym.MaxDelay = defaultMaxDelay
			plcSym.CycleTime = defaultCycleTime
		} else {
			parts := strings.Split(symbol, ":")
			plcSym.Name = parts[0]

			// Convert maxDelay and cycleTime, with default values in case of errors
			maxDelay, err1 := strconv.Atoi(parts[1])
			if err1 != nil {
				maxDelay = defaultMaxDelay
			}
			cycleTime, err2 := strconv.Atoi(parts[2])
			if err2 != nil {
				cycleTime = defaultCycleTime
			}
			plcSym.MaxDelay = maxDelay
			plcSym.CycleTime = cycleTime

		}
		// Append the created PlcSymbol to the list
		newPlcSymbol = append(newPlcSymbol, plcSym)
	}

	return newPlcSymbol
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
	HostPort         int                 // The host port
	ReadType         string              // Read type, interval or notification
	CycleTime        int                 // Read interval time if read type interval, cycle time if read type notification in milliseconds
	MaxDelay         int                 // Max delay time after value change before PLC should send message, in milliseconds
	IntervalTime     time.Duration       // Time duration before a connection attempt or read request times out.
	RequestTimeout   time.Duration       // Timeout for individual ADS requests.
	Handler          *adsLib.Connection  // TCP handler to manage the connection.
	Log              *service.Logger     // Logger for logging plugin activity.
	Symbols          []PlcSymbol         // List of items to read from the PLC
	DeviceInfo       adsLib.DeviceInfo   // PLC device info
	DeviceSymbols    adsLib.Symbol       // Received symbols from PLC
	NotificationChan chan *adsLib.Update // notification channel for PLC data
	TransmissionMode adsLib.TransMode   // Notification transmission mode

	// Route registration settings
	RouteUsername    string // If set, register a route before connecting
	RoutePassword   string
	RouteHostAddress string // Address the PLC uses to reach this client (auto-detected if empty)
}

var adsConf = service.NewConfigSpec().
	Summary("Creates an input that reads data from Beckhoff PLCs using ADS protocol.").
	Description("This input plugin enables Benthos to read data directly from Beckhoff PLCs using the ADS protocol. " +
		"Configure the plugin by specifying the PLC's IP address, runtime port, target AMS net ID, and symbols to read.").
	Field(service.NewStringField("targetIP").Description("IP address of the Beckhoff PLC.")).
	Field(service.NewStringField("targetAMS").Description("Target AMS net ID.")).
	Field(service.NewIntField("targetPort").Description("Target port. Default 48898 (Twincat 2)").Default(48898)).
	Field(service.NewIntField("runtimePort").Description("Target runtime port. Default 801(Twincat 2)").Default(801)).
	Field(service.NewStringField("hostAMS").Description("The host AMS net ID, use auto (default) to automatically derive AMS from host IP. Enter manually if auto not working").Default("auto")).
	Field(service.NewIntField("hostPort").Description("AMS source port used in protocol headers. This is a logical port, not a network port. Any arbitrary value works.").Default(10500)).
	Field(service.NewStringField("readType").Description("Read type, interval or notification (default)").Default("notification")).
	Field(service.NewIntField("maxDelay").Description("Max delay time after value change before PLC should send message, in milliseconds. Default 100").Default(100)).
	Field(service.NewIntField("cycleTime").Description("Requested read interval time for PLC to scan for changes if read type notification, in milliseconds.").Default(1000)).
	Field(service.NewIntField("intervalTime").Description("The interval time between reads milliseconds for read requests.").Default(1000)).
	Field(service.NewBoolField("upperCase").Description("Convert symbol names to uppercase(needed on some older PLCs). Default true ").Default(true)).
	Field(service.NewStringField("logLevel").Description("Log level for ADS connection. Default disabled").Default("disabled")).
	Field(service.NewIntField("requestTimeout").Description("Timeout for individual ADS requests in milliseconds. Default 5000").Default(5000)).
	Field(service.NewStringField("transmissionMode").Description("Notification transmission mode: serverOnChange (default), serverCycle, serverOnChange2, serverCycle2").Default("serverOnChange")).
	Field(service.NewStringField("routeUsername").Description("Username for UDP route registration on the PLC. If set, a route will be registered before connecting.").Default("")).
	Field(service.NewStringField("routePassword").Description("Password for UDP route registration on the PLC.").Default("")).
	Field(service.NewStringField("routeHostAddress").Description("The address the PLC should use to reach this client. Auto-detected from outbound connection if empty.").Default("")).
	Field(service.NewStringListField("symbols").Description("List of symbols to read in the format 'function.name', e.g., 'MAIN.counter', '.globalCounter' " +
		"If using custom max delay and cycle time for a symbol the format is 'function.name:maxDelay:cycleTime', e.g,. 'MAIN.counter:0:100', '.globalCounter:100:10'"))

// NewAdsCommInput creates a new ADS input plugin from parsed Benthos configuration.
func NewAdsCommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	logLevel, err := conf.FieldString("logLevel")
	if err != nil {
		return nil, err
	}
	// set log level of ADS library
	switch logLevel {
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "disabled":
		zerolog.SetGlobalLevel(zerolog.Disabled)
	default:
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}

	targetIP, err := conf.FieldString("targetIP")
	if err != nil {
		return nil, err
	}

	targetAMS, err := conf.FieldString("targetAMS")
	if err != nil {
		return nil, err
	}

	targetPort, err := conf.FieldInt("targetPort")
	if err != nil {
		return nil, err
	}

	runtimePort, err := conf.FieldInt("runtimePort")
	if err != nil {
		return nil, err
	}

	hostAMS, err := conf.FieldString("hostAMS")
	if err != nil {
		return nil, err
	}

	hostPort, err := conf.FieldInt("hostPort")
	if err != nil {
		return nil, err
	}

	readType, err := conf.FieldString("readType")
	if err != nil {
		return nil, err
	}
	if !(readType == "notification" || readType == "interval") {
		return nil, errors.New("wrong read type")
	}

	maxDelay, err := conf.FieldInt("maxDelay")
	if err != nil {
		return nil, err
	}

	cycleTime, err := conf.FieldInt("cycleTime")
	if err != nil {
		return nil, err
	}

	symbols, err := conf.FieldStringList("symbols")
	if err != nil {
		return nil, err
	}

	timeoutInt, err := conf.FieldInt("intervalTime")
	if err != nil {
		return nil, err
	}
	upperCase, err := conf.FieldBool("upperCase")
	if err != nil {
		return nil, err
	}

	requestTimeoutInt, err := conf.FieldInt("requestTimeout")
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

	routeUsername, err := conf.FieldString("routeUsername")
	if err != nil {
		return nil, err
	}

	routePassword, err := conf.FieldString("routePassword")
	if err != nil {
		return nil, err
	}

	routeHostAddress, err := conf.FieldString("routeHostAddress")
	if err != nil {
		return nil, err
	}

	symbolList := CreateSymbolList(symbols, maxDelay, cycleTime, upperCase)
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
		IntervalTime:     time.Duration(timeoutInt) * time.Millisecond,
		RequestTimeout:   time.Duration(requestTimeoutInt) * time.Millisecond,
		NotificationChan: make(chan *adsLib.Update),
		TransmissionMode: transmissionMode,
		RouteUsername:     routeUsername,
		RoutePassword:     routePassword,
		RouteHostAddress:  routeHostAddress,
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

func (g *AdsCommInput) Connect(ctx context.Context) error {
	if g.Handler != nil {
		return nil
	}

	// Create a new connection
	g.Log.Infof("Creating new connection")
	var err error
	g.Handler, err = adsLib.NewConnection(ctx, g.TargetIP, g.TargetPort, g.TargetAMS, g.RuntimePort, g.HostAMS, g.HostPort, g.RequestTimeout)
	if err != nil {
		g.Log.Errorf("Failed to create connection: %v", err)
		return err
	}

	// Register route if configured
	if g.RouteUsername != "" {
		hostAddr := g.RouteHostAddress
		if hostAddr == "" {
			// Auto-detect local IP using UDP (doesn't require PLC to accept the connection)
			udpConn, dialErr := net.Dial("udp4", net.JoinHostPort(g.TargetIP, "48899"))
			if dialErr != nil {
				g.Log.Errorf("Failed to auto-detect local address: %v", dialErr)
				return dialErr
			}
			hostAddr = udpConn.LocalAddr().(*net.UDPAddr).IP.String()
			udpConn.Close()
		}

		// Determine the AMS NetID for route registration.
		// When HostAMS is "auto", derive from RouteHostAddress (or auto-detected hostAddr)
		// so that route registration works correctly in Docker bridge networking.
		amsForRoute := g.HostAMS
		if amsForRoute == "auto" || amsForRoute == "" {
			if isLikelyContainerIP(hostAddr) {
				g.Log.Warnf("Auto-detected IP %s looks like a container IP. Set routeHostAddress to the Docker host's IP for route registration to work.", hostAddr)
			}
			amsForRoute = hostAddr + ".1.1"
			// Also update HostAMS so the ADS connection uses the same NetID
			g.HostAMS = amsForRoute
			// Re-create the connection with the correct source NetID
			g.Handler, err = adsLib.NewConnection(ctx, g.TargetIP, g.TargetPort, g.TargetAMS, g.RuntimePort, g.HostAMS, g.HostPort, g.RequestTimeout)
			if err != nil {
				g.Log.Errorf("Failed to re-create connection with derived HostAMS: %v", err)
				return err
			}
			g.Log.Infof("Derived HostAMS from RouteHostAddress: %s", amsForRoute)
		}

		sourceNetId, err := adsLib.StringToNetID(amsForRoute)
		if err != nil {
			g.Log.Errorf("Failed to parse AMS NetID %s: %v", amsForRoute, err)
			return err
		}
		routeName := fmt.Sprintf("benthosADS-%s", hostAddr)
		g.Log.Infof("Registering route on PLC %s: name=%s, clientIP=%s, amsNetId=%s", g.TargetIP, routeName, hostAddr, amsForRoute)
		err = adsLib.AddRemoteRoute(g.TargetIP, sourceNetId, routeName, hostAddr, g.RouteUsername, g.RoutePassword)
		if err != nil {
			g.Log.Warnf("Route registration failed (will attempt TCP connection anyway): %v", err)
		} else {
			// Give the PLC time to apply the route before connecting via TCP.
			// Older PLCs (TwinCAT 2) may need this delay.
			time.Sleep(1 * time.Second)
		}

		// Check for IP mismatch between registered route and actual TCP source
		tcpProbe, probeErr := net.DialTimeout("tcp",
			net.JoinHostPort(g.TargetIP, fmt.Sprintf("%d", g.TargetPort)),
			3*time.Second)
		if probeErr == nil {
			actualIP := tcpProbe.LocalAddr().(*net.TCPAddr).IP.String()
			tcpProbe.Close()
			if actualIP != hostAddr {
				g.Log.Warnf("Route registered with hostAddress=%s but TCP connections originate from a different IP. "+
					"Some PLCs (especially TwinCAT 2) validate that the TCP source IP matches the route and will reject the connection. "+
					"Set routeHostAddress to the IP that the PLC sees as the TCP source.", hostAddr)
			}
		}
	}

	// Connect to the PLC
	g.Log.Infof("Connecting to plc")
	err = g.Handler.Connect(false)
	if err != nil {
		g.Log.Errorf("Failed to connect to PLC at %s: %v", g.TargetIP, err)
		g.Handler.Close()
		g.Handler = nil
		return err
	}

	if g.ReadType == "notification" {
		g.Log.Infof("wait 2s to establish connection before adding notifications")
		time.Sleep(2 * time.Second)

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
		err = g.Handler.AddSymbolNotifications(configs, g.NotificationChan)
		if err != nil {
			g.Log.Errorf("Batch add notifications failed: %v", err)
			return err
		}

		g.Log.Infof("wait 1s after adding notifications")
		time.Sleep(1 * time.Second)
	}
	if g.ReadType != "notification" {
		g.Log.Infof("wait 2s to establish connection")
		time.Sleep(2 * time.Second)
	}
	return nil
}

func (g *AdsCommInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Debugf("ReadBatchPull called")
	if g.Handler == nil {
		return nil, nil, errors.New("client is nil")
	}

	// Collect symbol names for batch read
	names := make([]string, len(g.Symbols))
	for i, symbol := range g.Symbols {
		names[i] = symbol.Name
	}

	values, err := g.Handler.ReadMultipleSymbols(names)
	if err != nil {
		g.Log.Errorf("Batch read failed: %v", err)
		return nil, nil, err
	}

	msgs := service.MessageBatch{}
	for _, symbol := range g.Symbols {
		val, ok := values[symbol.Name]
		if !ok {
			continue
		}
		valueMsg := service.NewMessage([]byte(val))
		valueMsg.MetaSet("symbol_name", sanitize(symbol.Name))
		msgs = append(msgs, valueMsg)
	}

	time.Sleep(g.IntervalTime)
	return msgs, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (g *AdsCommInput) ReadBatchNotification(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Infof("ReadBatchNotification called")
	var res *adsLib.Update
	select {
	case res = <-g.NotificationChan:
		if res == nil {
			// Channel was closed, shutting down
			return nil, nil, service.ErrEndOfInput
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-time.After(10 * time.Second):
		return nil, nil, fmt.Errorf("timeout waiting for update")
	}
	msgs := service.MessageBatch{}
	b := make([]byte, 0)
	b = append(b, []byte(res.Value)...)
	valueMsg := service.NewMessage(b)
	valueMsg.MetaSet("symbol_name", sanitize(res.Variable))
	msgs = append(msgs, valueMsg)

	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *AdsCommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Infof("ReadBatch called")
	if g.ReadType == "notification" {
		return g.ReadBatchNotification(ctx)
	} else {
		return g.ReadBatchPull(ctx)
	}
}

func (g *AdsCommInput) Close(ctx context.Context) error {
	g.Log.Infof("Close called")
	if g.Handler != nil {
		g.Log.Infof("Closing down, cleaning up PLC handles")
		g.Handler.Close()
		g.Handler = nil
	}
	if g.NotificationChan != nil {
		close(g.NotificationChan)
		g.NotificationChan = nil
	}

	return nil
}
