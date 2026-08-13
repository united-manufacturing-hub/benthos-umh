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
	TargetIP         string
	TargetAMS        string
	TargetPort       int
	RuntimePort      int
	HostAMS          string
	HostPort         int
	ReadType         string
	CycleTime        time.Duration
	MaxDelay         time.Duration
	IntervalTime     time.Duration
	RequestTimeout   time.Duration
	client           Client
	Log              *service.Logger
	Symbols          []PlcSymbol
	symbolByName     map[string]*PlcSymbol // configured symbol name → *PlcSymbol, populated in NewAdsCommInput
	NotificationChan chan *Update
	TransmissionMode int

	// pendingInitial holds the initial notification samples captured during Connect's
	// readiness wait; flushed by the first read so the first value isn't lost.
	pendingInitial []*Update

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
	if err = validateAMSNetID(targetAMS); err != nil {
		return nil, fmt.Errorf("targetAMS: %w", err)
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

	symbolByName := make(map[string]*PlcSymbol, len(symbolList))
	for i := range symbolList {
		symbolByName[symbolList[i].Name] = &symbolList[i]
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
		symbolByName:     symbolByName,
		Log:              mgr.Logger(),
		IntervalTime:     intervalTime,
		RequestTimeout:   requestTimeout,
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

// Connect, ReadBatch and Close are stubs; the go-ads adapter (Task 2) fills in
// connection, read and shutdown behaviour behind the Client seam.

func (a *AdsCommInput) Connect(_ context.Context) error {
	return service.ErrNotConnected
}

func (a *AdsCommInput) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, service.ErrNotConnected
}

func (a *AdsCommInput) Close(_ context.Context) error {
	return nil
}
