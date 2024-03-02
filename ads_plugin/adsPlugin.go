package adsPlugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	adsLib "benthos-umh/ads_plugin/nativeADS"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/zerolog"
)

type plcSymbol struct {
	name      string
	maxDelay  int
	cycleTime int
}

func sanitize(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	return re.ReplaceAllString(s, "_")
}
func createSymbolList(s []string, defaultCycleTime int, defaultMaxDelay int, upperCase bool) []plcSymbol {
	var newPlcSymbol []plcSymbol
	// Iterate through symbol list
	for _, symbol := range s {
		if upperCase {
			symbol = strings.ToUpper(symbol)
		}
		// Count colons in the symbol
		colons := strings.Count(symbol, ":")

		// Create a new plcSymbol instance
		var plcSym plcSymbol

		// If there are anything other than two colons, add default value
		if colons != 2 {
			parts := strings.Split(symbol, ":")
			if len(parts) > 0 {
				plcSym.name = parts[0]
			}
			plcSym.maxDelay = defaultMaxDelay
			plcSym.cycleTime = defaultCycleTime
		} else {
			parts := strings.Split(symbol, ":")
			plcSym.name = parts[0]

			// Convert maxDelay and cycleTime, with default values in case of errors
			maxDelay, err1 := strconv.Atoi(parts[1])
			if err1 != nil {
				maxDelay = defaultMaxDelay
			}
			cycleTime, err2 := strconv.Atoi(parts[2])
			if err2 != nil {
				cycleTime = defaultCycleTime
			}
			plcSym.maxDelay = maxDelay
			plcSym.cycleTime = cycleTime

		}
		// Append the created plcSymbol to the list
		newPlcSymbol = append(newPlcSymbol, plcSym)
	}

	return newPlcSymbol
}

// ads communication struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Beckhoff PLC,
// along with the read requests to fetch data from the PLC.
type adsCommInput struct {
	targetIP         string              // IP address of the PLC.
	targetAMS        string              // Target AMS net ID.
	targetPort       int                 // Target port
	runtimePort      int                 // Target runtime port. Default 801(Twincat 2)
	hostAMS          string              // The host AMS net ID, auto (default) automatically derives AMS from host IP. Enter manually if auto not working
	hostPort         int                 // The host port
	readType         string              // Read type, interval or notification
	cycleTime        int                 // Read interval time if read type interval, cycle time if read type notification in milliseconds
	maxDelay         int                 // Max delay time after value change before PLC should send message, in milliseconds
	intervalTime     time.Duration       // Time duration before a connection attempt or read request times out.
	handler          *adsLib.Connection  // TCP handler to manage the connection.
	log              *service.Logger     // Logger for logging plugin activity.
	symbols          []plcSymbol         // List of items to read from the PLC
	deviceInfo       adsLib.DeviceInfo   // PLC device info
	deviceSymbols    adsLib.Symbol       // Received symbols from PLC
	notificationChan chan *adsLib.Update // notification channel for PLC data
}

var adsConf = service.NewConfigSpec().
	Summary("Creates an input that reads data from Beckhoff PLCs using adsLib. Created by Daniel H & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Beckhoff PLCs using the ADS protocol. " +
		"Configure the plugin by specifying the PLC's IP address, runtime port, target AMS net ID, etc. etc, add more here.").
	Field(service.NewStringField("targetIP").Description("IP address of the Beckhoff PLC.")).
	Field(service.NewStringField("targetAMS").Description("Target AMS net ID.")).
	Field(service.NewIntField("targetPort").Description("Target port. Default 48898 (Twincat 2)").Default(48898)).
	Field(service.NewIntField("runtimePort").Description("Target runtime port. Default 801(Twincat 2)").Default(801)).
	Field(service.NewStringField("hostAMS").Description("The host AMS net ID, use auto (default) to automatically derive AMS from host IP. Enter manually if auto not working").Default("auto")).
	Field(service.NewIntField("hostPort").Description("Host port. Default 48898 (Twincat 2)").Default(10500)).
	Field(service.NewStringField("readType").Description("Read type, interval or notification (default)").Default("notification")).
	Field(service.NewIntField("maxDelay").Description("Max delay time after value change before PLC should send message, in milliseconds. Default 100").Default(100)).
	Field(service.NewIntField("cycleTime").Description("Requested read interval time for PLC to scan for changes if read type notification, in milliseconds.").Default(1000)).
	Field(service.NewIntField("intervalTime").Description("The interval time between reads milliseconds for read requests.").Default(1000)).
	Field(service.NewBoolField("upperCase").Description("Convert symbol names to uppercase(needed on some older PLCs). Default true ").Default(true)).
	Field(service.NewStringField("logLevel").Description("Log level for ADS connection. Default disabled").Default("disabled")).
	Field(service.NewStringListField("symbols").Description("List of symbols to read in the format 'function.name', e.g., 'MAIN.counter', '.globalCounter' " +
		"If using custom max delay and cycle time for a symbol the format is 'function.name:maxDelay:cycleTime', e.g,. 'MAIN.counter:0:100', '.globalCounter:100:10'"))

func newAdsCommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

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

	symbolList := createSymbolList(symbols, maxDelay, cycleTime, upperCase)
	m := &adsCommInput{
		targetIP:         targetIP,
		targetAMS:        targetAMS,
		targetPort:       targetPort,
		runtimePort:      runtimePort,
		hostAMS:          hostAMS,
		hostPort:         hostPort,
		readType:         readType,
		maxDelay:         maxDelay,
		cycleTime:        cycleTime,
		symbols:          symbolList,
		log:              mgr.Logger(),
		intervalTime:     time.Duration(timeoutInt) * time.Millisecond,
		notificationChan: make(chan *adsLib.Update),
	}

	return service.AutoRetryNacksBatched(m), nil

}

func init() {
	err := service.RegisterBatchInput(
		"ads", adsConf,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newAdsCommInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *adsCommInput) Connect(ctx context.Context) error {
	if g.handler != nil {
		return nil
	}

	// Create a new connection
	g.log.Infof("Creating new connection")
	var err error
	g.handler, err = adsLib.NewConnection(ctx, g.targetIP, g.targetPort, g.targetAMS, g.runtimePort, g.hostAMS, g.hostPort)
	if err != nil {
		g.log.Errorf("Failed to create connection: %v", err)
		return err
	}

	// Connect to the PLC
	g.log.Infof("Connecting to plc")
	err = g.handler.Connect(false)
	if err != nil {
		g.log.Errorf("Failed to connect to PLC at %s: %v", g.targetIP, err)
		g.handler.Close()
		g.handler = nil
		return err
	}

	// Read device info
	/*   g.log.Infof("Read device info")
	g.deviceInfo, err = g.handler.ReadDeviceInfo()
	if err != nil {
		g.log.Errorf("Failed to read device info: %v", err)
		return
	}*/
	// g.log.Infof("Successfully connected to \"%s\" version %d.%d (build %d)", g.deviceInfo.DeviceName, g.deviceInfo.Major, g.deviceInfo.Minor, g.deviceInfo.Version)

	if g.readType == "notification" {
		g.log.Infof("wait 2s to establish connection before adding notifications")
		time.Sleep(2 * time.Second)
		// Add symbol notifications
		for _, symbol := range g.symbols {
			g.log.Infof("Adding symbol notification for %s", symbol.name)
			g.handler.AddSymbolNotification(symbol.name, symbol.maxDelay, symbol.cycleTime, g.notificationChan)
		}
		g.log.Infof("wait 1s after adding notifications")
		time.Sleep(1 * time.Second)
	}
	if g.readType != "notification" {
		g.log.Infof("wait 3s to establish connection")
		time.Sleep(2 * time.Second)
	}
	return nil
}

func (g *adsCommInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.log.Infof("ReadBatchPull called")
	if g.handler == nil {
		return nil, nil, errors.New("client is nil")
	}
	//

	msgs := service.MessageBatch{}

	for _, symbol := range g.symbols {
		b := make([]byte, 0)
		g.log.Infof("reading symbol  %s", symbol.name)
		res, _ := g.handler.ReadFromSymbol(symbol.name)
		b = append(b, []byte(res)...)
		valueMsg := service.NewMessage(b)
		valueMsg.MetaSet("symbol_name", sanitize(symbol.name))

		msgs = append(msgs, valueMsg)
	}
	time.Sleep(g.intervalTime)
	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *adsCommInput) ReadBatchNotification(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.log.Infof("ReadBatchNotification called")
	var res *adsLib.Update
	select {
	case res = <-g.notificationChan:
		// Successfully received an update
	case <-ctx.Done():
		// Context canceled, return an error or handle accordingly
		return nil, nil, fmt.Errorf("context canceled")
	case <-time.After(10 * time.Second): // Add a timeout to avoid blocking indefinitely
		// Handle timeout, e.g., return an error or empty message
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

func (g *adsCommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.log.Infof("ReadBatch called")
	if g.readType == "notification" {
		return g.ReadBatchNotification(ctx)
	} else {
		return g.ReadBatchPull(ctx)
	}
}

func (g *adsCommInput) Close(ctx context.Context) error {
	g.log.Infof("Close called")
	if g.handler != nil {
		g.log.Infof("Closing down")
		if g.notificationChan != nil {
			close(g.notificationChan)
		}
		g.handler.Close()
		g.handler = nil
	}

	return nil
}
