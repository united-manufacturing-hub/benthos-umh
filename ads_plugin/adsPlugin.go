package adsPlugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	adsLib "github.com/united-manufacturing-hub/benthos-umh/v2/ads_plugin/nativeADS"

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
func CreateSymbolList(s []string, defaultCycleTime int, defaultMaxDelay int, upperCase bool) []plcSymbol {
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

// Ads communication struct defines the structure for our custom Benthos input plugin.
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
	Handler          *adsLib.Connection  // TCP handler to manage the connection.
	Log              *service.Logger     // Logger for logging plugin activity.
	Symbols          []plcSymbol         // List of items to read from the PLC
	NotificationChan chan *adsLib.Update // notification channel for PLC data
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
		NotificationChan: make(chan *adsLib.Update),
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
	g.Handler, err = adsLib.NewConnection(ctx, g.TargetIP, g.TargetPort, g.TargetAMS, g.RuntimePort, g.HostAMS, g.HostPort)
	if err != nil {
		g.Log.Errorf("Failed to create connection: %v", err)
		return err
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

	// Read device info
	/*   g.Log.Infof("Read device info")
	g.deviceInfo, err = g.Handler.ReadDeviceInfo()
	if err != nil {
		g.Log.Errorf("Failed to read device info: %v", err)
		return
	}*/
	// g.Log.Infof("Successfully connected to \"%s\" version %d.%d (build %d)", g.deviceInfo.DeviceName, g.deviceInfo.Major, g.deviceInfo.Minor, g.deviceInfo.Version)

	if g.ReadType == "notification" {
		g.Log.Infof("wait 2s to establish connection before adding notifications")
		time.Sleep(2 * time.Second)
		// Add symbol notifications
		for _, symbol := range g.Symbols {
			g.Log.Infof("Adding symbol notification for %s", symbol.name)
			g.Handler.AddSymbolNotification(symbol.name, symbol.maxDelay, symbol.cycleTime, g.NotificationChan)
		}
		g.Log.Infof("wait 1s after adding notifications")
		time.Sleep(1 * time.Second)
	}
	if g.ReadType != "notification" {
		g.Log.Infof("wait 3s to establish connection")
		time.Sleep(2 * time.Second)
	}
	return nil
}

func (g *AdsCommInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Infof("ReadBatchPull called")
	if g.Handler == nil {
		return nil, nil, errors.New("client is nil")
	}
	//

	msgs := service.MessageBatch{}

	for _, symbol := range g.Symbols {
		b := make([]byte, 0)
		g.Log.Infof("reading symbol  %s", symbol.name)
		res, _ := g.Handler.ReadFromSymbol(symbol.name)
		b = append(b, []byte(res)...)
		valueMsg := service.NewMessage(b)
		valueMsg.MetaSet("symbol_name", sanitize(symbol.name))

		msgs = append(msgs, valueMsg)
	}
	time.Sleep(g.IntervalTime)
	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *AdsCommInput) ReadBatchNotification(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.Log.Infof("ReadBatchNotification called")
	var res *adsLib.Update
	select {
	case res = <-g.NotificationChan:
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
		g.Log.Infof("Closing down")
		if g.NotificationChan != nil {
			close(g.NotificationChan)
		}
		g.Handler.Close()
		g.Handler = nil
	}

	return nil
}
