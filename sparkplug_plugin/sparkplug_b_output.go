// Copyright 2025 UMH Systems GmbH
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

// Package sparkplug_plugin implements Device-Level PARRIS architecture for Sparkplug B.
//
// Architecture Overview:
// This implementation focuses exclusively on device-level messaging (DBIRTH/DDATA)
// rather than the full node-level + device-level complexity. This design choice
// aligns with UMH's asset-centric approach and simplifies the codebase significantly.
//
// Key Concepts:
// - DBIRTH: Device birth certificates that define ALL metric aliases for a device
// - DDATA: Device data messages using aliases for efficient transmission
// - PARRIS Method: Dynamic Device ID generation from UMH location_path metadata
// - Alias Resolution: Numeric aliases resolved to metric names via cached DBIRTH certificates
//
// Message Flow:
// 1. UMH message arrives with location_path metadata
// 2. location_path converted to Sparkplug Device ID (PARRIS method)
// 3. Device metrics tracked and accumulated across messages
// 4. DBIRTH published with complete metric definitions (first time OR new metrics)
// 5. DDATA published with aliases for efficient transmission
//
// This approach eliminates NBIRTH/NDATA complexity while maintaining full Sparkplug B compliance.
package sparkplug_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
	"google.golang.org/protobuf/proto"
)

func init() {
	outputSpec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Sparkplug B MQTT output acting as Edge Node").
		Description(`The Sparkplug B output acts as an Edge Node, publishing data to Sparkplug MQTT topics 
with complete session lifecycle management. It handles BIRTH/DEATH certificates, maintains sequence 
numbers, manages alias mappings, and ensures full Sparkplug B compliance.

Key features:
- Always operates as Edge Node (no role configuration needed)
- Automatic NBIRTH/DBIRTH on connect with metric definitions
- NDEATH/DDEATH Last Will Testament on disconnect
- Sequence number management with proper wrapping
- Alias-based metric publishing for bandwidth efficiency
- Automatic type detection and conversion
- Configurable metric definitions with aliases
- Robust reconnection handling with proper rebirth
- UNS metadata integration for seamless data flow

The output connects to an MQTT broker, publishes BIRTH certificates to announce available metrics,
and then publishes DATA messages as Benthos messages flow through the pipeline.`).
		// MQTT Transport Configuration
		Field(service.NewObjectField("mqtt",
			service.NewStringListField("urls").
				Description("List of MQTT broker URLs to connect to").
				Example([]string{"tcp://localhost:1883", "ssl://broker.hivemq.com:8883"}).
				Default([]string{"tcp://localhost:1883"}),
			service.NewStringField("client_id").
				Description("MQTT client ID for this edge node").
				Default("benthos-sparkplug-output"),
			service.NewObjectField("credentials",
				service.NewStringField("username").
					Description("MQTT username for authentication").
					Default("").
					Optional(),
				service.NewStringField("password").
					Description("MQTT password for authentication").
					Default("").
					Secret().
					Optional()).
				Description("MQTT authentication credentials").
				Optional(),
			service.NewIntField("qos").
				Description("QoS level for MQTT publishing (0, 1, or 2)").
				Default(1),
			service.NewDurationField("keep_alive").
				Description("MQTT keep alive interval").
				Default("60s"),
			service.NewDurationField("connect_timeout").
				Description("MQTT connection timeout").
				Default("30s"),
			service.NewBoolField("clean_session").
				Description("MQTT clean session flag").
				Default(true)).
			Description("MQTT transport configuration")).
		// Sparkplug Identity Configuration
		Field(service.NewObjectField("identity",
			service.NewStringField("group_id").
				Description("Sparkplug Group ID (e.g., 'FactoryA')").
				Example("FactoryA"),
			service.NewStringField("edge_node_id").
				Description("Edge Node ID within the group (e.g., 'Line3'). If empty, auto-generated from location_path metadata using Parris Method").
				Example("Line3").
				Optional(),
			service.NewStringField("device_id").
				Description("Device ID under the edge node (optional, if not specified acts as node-level)").
				Default("").
				Optional()).
			Description("Sparkplug identity configuration")).

		// Output-specific Configuration
		Field(service.NewObjectListField("metrics",
			service.NewStringField("name").
				Description("Metric name as it will appear in BIRTH messages"),
			service.NewIntField("alias").
				Description("Numeric alias for this metric (1-65535)"),
			service.NewStringField("type").
				Description("Data type: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float, double, boolean, string").
				Default("double"),
			service.NewStringField("value_from").
				Description("JSONPath or field name in the message to extract value from").
				Default("value")).
			Description("Metric definitions for BIRTH messages and alias mapping").
			Optional()).
		// Behaviour Configuration
		Field(service.NewObjectField("behaviour",
			service.NewBoolField("auto_extract_tag_name").
				Description("Whether to automatically extract tag_name from message metadata").
				Default(true),
			service.NewBoolField("retain_last_values").
				Description("Whether to retain last known values for BIRTH messages after reconnection").
				Default(true)).
			Description("Processing behavior configuration").
			Optional())

	err := service.RegisterOutput(
		"sparkplug_b",
		outputSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			output, err := newSparkplugOutput(conf, mgr)
			if err != nil {
				return nil, 0, err
			}
			return output, 1, nil // maxInFlight = 1 for MQTT outputs
		})
	if err != nil {
		panic(err)
	}
}

type MetricConfig struct {
	Name      string `json:"name"`
	Alias     uint64 `json:"alias"`
	Type      string `json:"type"`
	ValueFrom string `json:"value_from"`
}

type sparkplugOutput struct {
	config             Config
	metrics            []MetricConfig
	autoExtractTagName bool
	retainLastValues   bool
	logger             *service.Logger

	// MQTT client and state
	client mqtt.Client
	mu     sync.RWMutex

	// Sparkplug state
	bdSeq         uint64
	seqCounter    uint8
	metricAliases map[string]uint64      // metric name -> alias
	metricTypes   map[string]string      // metric name -> type
	lastValues    map[string]interface{} // metric name -> last value (global/node-level)
	stateMu       sync.RWMutex

	// Device-specific value tracking for DBIRTH messages
	deviceLastValues map[string]map[string]interface{} // device_id -> metric_name -> last_value

	// Dynamic alias management for P5 implementation
	nextAlias         uint64       // Next available alias number for dynamic assignment
	rebirthPending    bool         // Flag to prevent NDATA during rebirth
	lastRebirthTime   time.Time    // Last rebirth timestamp for debouncing
	rebirthDebounceMs int64        // Debounce period in milliseconds (default: 5000ms)
	dynamicMu         sync.RWMutex // Separate mutex for dynamic alias operations

	// Device-level PARRIS state management
	seenDevices   map[string]bool              // Track devices published in this session
	deviceMetrics map[string]map[string]uint64 // Cache metrics per device (device_id -> metric_name -> alias)
	deviceStateMu sync.RWMutex                 // Thread safety for device state

	// NBIRTH synchronization to prevent DBIRTH before NBIRTH
	//
	// CRITICAL SPARKPLUG B SPECIFICATION REQUIREMENT:
	// According to Sparkplug B spec, NBIRTH MUST be published before any device messages.
	// Without this synchronization, there's a race condition where:
	// 1. MQTT client connects → onConnect() callback triggered (async)
	// 2. First message arrives → Write() method called immediately
	// 3. DBIRTH gets published before NBIRTH completes (SPEC VIOLATION)
	//
	// This synchronization ensures proper message ordering:
	// NBIRTH (seq=0) → DBIRTH (seq=1,2,3...) → DDATA (seq=4,5,6...)
	nbirthPublished bool       // Flag to track if NBIRTH has been published
	nbirthMu        sync.Mutex // Mutex to protect NBIRTH publication

	// Core components
	mqttClientBuilder *MQTTClientBuilder

	// Metrics
	messagesPublished *service.MetricCounter
	birthsPublished   *service.MetricCounter
	deathsPublished   *service.MetricCounter
	sequenceWraps     *service.MetricCounter
	publishErrors     *service.MetricCounter

	// Added for type inference
	typeConverter *TypeConverter
}

func newSparkplugOutput(conf *service.ParsedConfig, mgr *service.Resources) (*sparkplugOutput, error) {
	// Parse the idiomatic configuration structure using namespace approach
	var config Config

	// Parse MQTT section using namespace
	mqttConf := conf.Namespace("mqtt")
	urls, err := mqttConf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	config.MQTT.URLs = urls

	config.MQTT.ClientID, err = mqttConf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	// Parse credentials section if present
	if mqttConf.Contains("credentials") {
		credsConf := mqttConf.Namespace("credentials")
		username, err := credsConf.FieldString("username")
		if err == nil {
			config.MQTT.Credentials.Username = username
		}
		password, err := credsConf.FieldString("password")
		if err == nil {
			config.MQTT.Credentials.Password = password
		}
	}

	qosInt, err := mqttConf.FieldInt("qos")
	if err != nil {
		return nil, err
	}
	config.MQTT.QoS = byte(qosInt)

	config.MQTT.KeepAlive, err = mqttConf.FieldDuration("keep_alive")
	if err != nil {
		return nil, err
	}

	config.MQTT.ConnectTimeout, err = mqttConf.FieldDuration("connect_timeout")
	if err != nil {
		return nil, err
	}

	config.MQTT.CleanSession, err = mqttConf.FieldBool("clean_session")
	if err != nil {
		return nil, err
	}

	// Parse identity section using namespace
	identityConf := conf.Namespace("identity")
	config.Identity.GroupID, err = identityConf.FieldString("group_id")
	if err != nil {
		return nil, err
	}

	config.Identity.EdgeNodeID, _ = identityConf.FieldString("edge_node_id")
	if config.Identity.EdgeNodeID == "" {
		return nil, fmt.Errorf("configuration error: edge_node_id is required for Sparkplug B compliance - please set identity.edge_node_id in your configuration")
	}

	config.Identity.LocationPath, _ = identityConf.FieldString("location_path")
	// location_path is optional - used for PARRIS Method conversion to device_id

	config.Identity.DeviceID, _ = identityConf.FieldString("device_id")
	// device_id is optional - if not provided, generated from location_path via PARRIS

	// Output plugin always acts as edge_node (no role configuration needed)
	config.Role = RoleEdgeNode

	// Parse behaviour section using namespace (optional)
	var autoExtractTagName, retainLastValues bool
	if conf.Contains("behaviour") {
		behaviourConf := conf.Namespace("behaviour")
		autoExtractTagName, err = behaviourConf.FieldBool("auto_extract_tag_name")
		if err != nil {
			autoExtractTagName = true // default
		}
		retainLastValues, err = behaviourConf.FieldBool("retain_last_values")
		if err != nil {
			retainLastValues = true // default
		}
	} else {
		// Use defaults
		autoExtractTagName = true
		retainLastValues = true
	}

	// Parse metric configurations (optional)
	var metricObjs []*service.ParsedConfig
	if conf.Contains("metrics") {
		metricObjs, err = conf.FieldObjectList("metrics")
		if err != nil {
			return nil, err
		}
	}

	var metrics []MetricConfig
	metricAliases := make(map[string]uint64)
	metricTypes := make(map[string]string)

	for _, metricObj := range metricObjs {
		name, err := metricObj.FieldString("name")
		if err != nil {
			return nil, fmt.Errorf("metric name is required: %w", err)
		}

		alias, err := metricObj.FieldInt("alias")
		if err != nil {
			return nil, fmt.Errorf("metric alias is required: %w", err)
		}

		metricType, err := metricObj.FieldString("type")
		if err != nil {
			return nil, err
		}

		valueFrom, err := metricObj.FieldString("value_from")
		if err != nil {
			return nil, err
		}

		metricConfig := MetricConfig{
			Name:      name,
			Alias:     uint64(alias),
			Type:      metricType,
			ValueFrom: valueFrom,
		}

		metrics = append(metrics, metricConfig)
		metricAliases[name] = uint64(alias)
		metricTypes[name] = metricType
	}

	// BDSEQ IMPLEMENTATION AND LIMITATIONS:
	//
	// Sparkplug B v3.0 specification (§5.4) requires:
	// 1. Initial value: Can be any 64-bit value
	// 2. Subsequent sessions: MUST increment by exactly +1 for each new MQTT session
	//
	// STATELESS LIMITATION:
	// Benthos components are stateless by design - no persistence across restarts.
	// Therefore, bdSeq behavior is:
	// - Within component lifetime: 0 → 1 → 2 → 3 → ... (spec compliant)
	// - Across component restarts: Resets to 0 (limitation of stateless architecture)
	//
	// This is acceptable for many Sparkplug deployments where Edge Nodes reset
	// bdSeq on restart, but users should be aware of this behavior.
	bdSeq := uint64(0)

	// Calculate next available alias number (start after configured aliases)
	nextAlias := uint64(1) // Start from 1, bdSeq uses alias 0
	for _, alias := range metricAliases {
		if alias >= nextAlias {
			nextAlias = alias + 1
		}
	}

	return &sparkplugOutput{
		config:             config,
		metrics:            metrics,
		autoExtractTagName: autoExtractTagName,
		retainLastValues:   retainLastValues,
		logger:             mgr.Logger(),
		bdSeq:              bdSeq,
		seqCounter:         0,
		metricAliases:      metricAliases,
		metricTypes:        metricTypes,
		lastValues:         make(map[string]interface{}),
		deviceLastValues:   make(map[string]map[string]interface{}),
		nextAlias:          nextAlias,
		rebirthPending:     false,
		rebirthDebounceMs:  5000, // 5 second debounce
		// Device-level PARRIS state initialization
		seenDevices:   make(map[string]bool),
		deviceMetrics: make(map[string]map[string]uint64),

		mqttClientBuilder: NewMQTTClientBuilder(mgr),
		messagesPublished: mgr.Metrics().NewCounter("messages_published"),
		birthsPublished:   mgr.Metrics().NewCounter("births_published"),
		deathsPublished:   mgr.Metrics().NewCounter("deaths_published"),
		sequenceWraps:     mgr.Metrics().NewCounter("sequence_wraps"),
		publishErrors:     mgr.Metrics().NewCounter("publish_errors"),
		typeConverter:     NewTypeConverter(),
	}, nil
}

func (s *sparkplugOutput) Connect(ctx context.Context) error {
	s.logger.Info("Connecting Sparkplug B output as Edge Node")

	// Set up DEATH Last Will Testament
	// Note: Last Will Testament uses static configuration since no message context is available
	deathTopic, deathPayload := s.createDeathMessage(nil)

	mqttConfig := MQTTClientConfig{
		BrokerURLs:       s.config.MQTT.URLs,
		ClientID:         s.config.MQTT.ClientID,
		Username:         s.config.MQTT.Credentials.Username,
		Password:         s.config.MQTT.Credentials.Password,
		KeepAlive:        s.config.MQTT.KeepAlive,
		ConnectTimeout:   s.config.MQTT.ConnectTimeout,
		CleanSession:     s.config.MQTT.CleanSession,
		WillTopic:        deathTopic,
		WillPayload:      deathPayload,
		WillQoS:          s.config.MQTT.QoS,
		WillRetain:       true,
		OnConnect:        s.onConnect,
		OnConnectionLost: s.onConnectionLost,
	}

	// Create MQTT client using Benthos-integrated builder
	client, err := s.mqttClientBuilder.CreateClient(mqttConfig)
	if err != nil {
		return fmt.Errorf("failed to create MQTT client: %w", err)
	}
	s.client = client

	// Connect with Benthos-style retry and monitoring
	s.logger.Infof("Connecting to MQTT brokers: %v", s.config.MQTT.URLs)
	if err := s.mqttClientBuilder.ConnectWithRetry(client, s.config.MQTT.ConnectTimeout); err != nil {
		return err
	}

	s.logger.Info("Successfully connected to Sparkplug MQTT broker")
	return nil
}

func (s *sparkplugOutput) onConnect(client mqtt.Client) {
	s.logger.Info("MQTT client connected, publishing BIRTH message")

	// SEQUENCE COUNTER INITIALIZATION FIX:
	// Reset sequence counter to 255 so that the first increment results in seq=0 for NBIRTH.
	// This is a critical fix because both publishBirthMessage() and publishDBIRTH() now
	// properly increment the sequence counter instead of hardcoding seq=0.
	//
	// Previous bug: Both NBIRTH and DBIRTH were hardcoded to seq=0, violating Sparkplug B spec
	// Fixed sequence progression after connection:
	// seqCounter=255 → increment → seq=0 (NBIRTH) → seq=1 (DBIRTH) → seq=2,3,4... (DDATA)
	s.stateMu.Lock()
	s.seqCounter = 255
	s.stateMu.Unlock()

	// SYNCHRONIZATION PROTOCOL TO PREVENT RACE CONDITION:
	// Reset the NBIRTH flag and only set it to true after successful NBIRTH publication.
	// This prevents the Write() method from processing device messages before NBIRTH.
	//
	// Previous bug: DBIRTH could be published before NBIRTH due to race condition between
	// onConnect() callback and Write() method execution, violating Sparkplug B specification.
	s.nbirthMu.Lock()
	defer s.nbirthMu.Unlock()

	if err := s.publishBirthMessage(); err != nil {
		s.logger.Errorf("Failed to publish BIRTH message: %v", err)
		s.publishErrors.Incr(1)
	} else {
		s.nbirthPublished = true // CRITICAL: Mark NBIRTH as published to allow device messages
		s.birthsPublished.Incr(1)
		s.logger.Info("Successfully published BIRTH message")
	}

	// Subscribe to node rebirth commands (NCMD) 
	// Edge Nodes must listen for rebirth requests from Host applications
	ncmdTopic := fmt.Sprintf("spBv1.0/%s/NCMD/%s", s.config.Identity.GroupID, s.config.Identity.EdgeNodeID)
	s.logger.Infof("Subscribing to node rebirth commands on topic: %s", ncmdTopic)
	
	token := client.Subscribe(ncmdTopic, 1, s.handleRebirthCommand)
	if token.Wait() && token.Error() != nil {
		s.logger.Errorf("Failed to subscribe to rebirth commands: %v", token.Error())
		s.publishErrors.Incr(1)
	} else {
		s.logger.Info("Successfully subscribed to node rebirth commands")
	}
}

func (s *sparkplugOutput) onConnectionLost(client mqtt.Client, err error) {
	s.logger.Errorf("MQTT connection lost: %v", err)

	// Reset NBIRTH flag so it will be republished on reconnection
	s.nbirthMu.Lock()
	s.nbirthPublished = false
	s.nbirthMu.Unlock()
}

// handleRebirthCommand processes incoming NCMD messages with rebirth requests
func (s *sparkplugOutput) handleRebirthCommand(client mqtt.Client, msg mqtt.Message) {
	s.logger.Infof("Received rebirth command on topic: %s", msg.Topic())

	// Debug: Log raw payload bytes for troubleshooting
	s.logger.Infof("Raw NCMD payload size: %d bytes", len(msg.Payload()))

	// Parse the Sparkplug B payload
	var payload sparkplugb.Payload
	if err := proto.Unmarshal(msg.Payload(), &payload); err != nil {
		s.logger.Errorf("Failed to unmarshal rebirth command payload: %v", err)
		payloadLen := len(msg.Payload())
		if payloadLen > 100 {
			payloadLen = 100
		}
		s.logger.Errorf("Raw payload (first %d bytes): %x", payloadLen, msg.Payload()[:payloadLen])
		return
	}

	// Debug: Log payload contents
	s.logger.Infof("Parsed NCMD payload - metrics count: %d, timestamp: %v", len(payload.Metrics), payload.Timestamp)
	for i, metric := range payload.Metrics {
		name := "nil"
		if metric.Name != nil {
			name = *metric.Name
		}
		alias := "nil"
		if metric.Alias != nil {
			alias = fmt.Sprintf("%d", *metric.Alias)
		}
		s.logger.Infof("Metric %d: name='%s', alias=%s, datatype=%v, hasBoolean=%v", i, name, alias, metric.Datatype, metric.GetBooleanValue())
	}

	// Look for the "Node Control/Rebirth" metric
	// Can be either named metric OR alias 0 (per Sparkplug B spec)
	rebirthRequested := false
	for _, metric := range payload.Metrics {
		isRebirthMetric := false
		
		// Check named metric
		if metric.Name != nil && *metric.Name == "Node Control/Rebirth" {
			isRebirthMetric = true
		}
		
		// Check alias-based metric (alias 1 = "Node Control/Rebirth" in Ignition)
		// Note: Different implementations use different aliases for rebirth
		if metric.Alias != nil && (*metric.Alias == 0 || *metric.Alias == 1) {
			isRebirthMetric = true
		}
		
		// If this is a rebirth metric with boolean true value
		if isRebirthMetric && metric.GetBooleanValue() {
			rebirthRequested = true
			s.logger.Infof("Found rebirth request - name: %v, alias: %v, boolean: %v", 
				metric.Name, metric.Alias, metric.GetBooleanValue())
			break
		}
	}

	if !rebirthRequested {
		s.logger.Infof("No valid rebirth request found - looking for 'Node Control/Rebirth' with boolean true")
		return
	}

	s.logger.Info("Processing node rebirth request - republishing BIRTH message")

	// Increment bdSeq for the rebirth
	s.stateMu.Lock()
	s.bdSeq++
	s.stateMu.Unlock()

	// Republish NBIRTH with incremented bdSeq
	if err := s.publishBirthMessage(); err != nil {
		s.logger.Errorf("Failed to republish BIRTH message after rebirth request: %v", err)
		s.publishErrors.Incr(1)
	} else {
		s.birthsPublished.Incr(1)
		s.logger.Info("Successfully republished BIRTH message in response to rebirth request")
	}
}

func (s *sparkplugOutput) Write(ctx context.Context, msg *service.Message) error {
	// Wait for NBIRTH to be published before allowing any device messages
	// This prevents DBIRTH from being published before NBIRTH (Sparkplug B violation)
	s.nbirthMu.Lock()
	nbirthReady := s.nbirthPublished
	s.nbirthMu.Unlock()

	if !nbirthReady {
		s.logger.Debug("Waiting for NBIRTH to be published before processing device messages")
		// Wait for NBIRTH with timeout to prevent infinite blocking
		timeout := time.After(10 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				return fmt.Errorf("timeout waiting for NBIRTH to be published")
			case <-ticker.C:
				s.nbirthMu.Lock()
				if s.nbirthPublished {
					s.nbirthMu.Unlock()
					goto nbirthReady
				}
				s.nbirthMu.Unlock()
			}
		}
	}

nbirthReady:

	// Extract data from message
	data, err := s.extractMessageData(msg)
	if err != nil {
		s.logger.Errorf("Failed to extract message data: %v", err)
		s.publishErrors.Incr(1)
		return err
	}

	if len(data) == 0 {
		// Enhanced debug logging to understand why no metrics were extracted
		structured, _ := msg.AsStructured()
		allMeta := make(map[string]interface{})
		err := msg.MetaWalk(func(key, value string) error {
			allMeta[key] = value
			return nil
		})
		if err != nil {
			s.logger.Debugf("Failed to walk message metadata: %v", err)
		}

		s.logger.Debugf("No metrics to publish in message - Debug info: "+
			"Configured metrics count: %d, "+
			"Auto extract tag_name: %t, "+
			"Message payload: %+v, "+
			"Message metadata: %+v",
			len(s.metrics), s.autoExtractTagName, structured, allMeta)
		return nil
	}

	// Phase 3: Device-level PARRIS implementation
	deviceID := s.getParrisDeviceID(msg)

	// Check if we need to publish DBIRTH (first time OR new metrics discovered)
	newMetricsFound := s.updateDeviceMetrics(deviceID, data)

	if s.isFirstTimeDevice(deviceID) || newMetricsFound {
		if s.isFirstTimeDevice(deviceID) {
			s.logger.Infof("First message for device '%s', publishing DBIRTH", deviceID)
		} else {
			s.logger.Infof("New metrics discovered for device '%s', publishing updated DBIRTH", deviceID)
		}

		// Get all known metrics for this device for DBIRTH
		allDeviceMetrics := s.getAllDeviceMetrics(deviceID, data)

		if err := s.publishDBIRTH(deviceID, allDeviceMetrics); err != nil {
			s.logger.Errorf("Failed to publish DBIRTH for device '%s': %v", deviceID, err)
			s.publishErrors.Incr(1)
			return err
		}
		s.markDeviceSeen(deviceID)
		s.birthsPublished.Incr(1)
	}

	// Publish DATA message (DDATA if device, NDATA if node-level)
	if err := s.publishDataMessage(data, msg); err != nil {
		s.logger.Errorf("Failed to publish DATA message: %v", err)
		s.publishErrors.Incr(1)
		return err
	}

	// Store device values after successful DATA publication for future DBIRTH messages
	s.storeDeviceLastValues(deviceID, data)

	s.messagesPublished.Incr(1)
	return nil
}

func (s *sparkplugOutput) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil && s.client.IsConnected() {
		// Publish DEATH message before disconnecting gracefully
		// Note: DEATH messages use static configuration since no message context is available
		// DEATH messages must be retained to clear the retained BIRTH message
		deathTopic, deathPayload := s.createDeathMessage(nil)
		token := s.client.Publish(deathTopic, s.config.MQTT.QoS, true, deathPayload)
		token.WaitTimeout(5 * time.Second)

		if token.Error() == nil {
			s.deathsPublished.Incr(1)
			s.logger.Info("Published retained DEATH message before disconnect")
		}

		s.client.Disconnect(1000)
	}

	s.logger.Info("Sparkplug output closed")
	return nil
}

// getParrisDeviceID resolves the Device ID using the PARRIS Method at device level.
// This implements the NEW device-level PARRIS approach for Sparkplug B compliance.
//
// Priority logic:
// 1. Static Override: If device_id is configured, use it (ignores metadata)
// 2. Dynamic Generation: If location_path metadata exists, convert using PARRIS Method
//   - Converts UMH dot notation to Sparkplug colon notation
//   - Example: "enterprise.plant1.line3.station5" → "enterprise:plant1:line3:station5"
//
// 3. Config LocationPath: If location_path is configured statically, use it
// 4. Empty Device ID: Return empty string for node-level messages (no device)
func (s *sparkplugOutput) getParrisDeviceID(msg *service.Message) string {
	// Priority 1: Static device_id override
	if s.config.Identity.DeviceID != "" {
		return s.config.Identity.DeviceID
	}

	// Priority 2: Dynamic from message metadata
	if msg != nil {
		if locationPath, exists := msg.MetaGet("location_path"); exists && locationPath != "" {
			return strings.ReplaceAll(locationPath, ".", ":")
		}
	}

	// Priority 3: Static location_path from config
	if s.config.Identity.LocationPath != "" {
		return strings.ReplaceAll(s.config.Identity.LocationPath, ".", ":")
	}

	// Priority 4: Empty device ID (node-level messages)
	return ""
}

// isFirstTimeDevice checks if this is the first time we're seeing this device ID
func (s *sparkplugOutput) isFirstTimeDevice(deviceID string) bool {
	if deviceID == "" {
		return false // Node-level messages don't need DBIRTH
	}

	s.deviceStateMu.RLock()
	defer s.deviceStateMu.RUnlock()

	return !s.seenDevices[deviceID]
}

// markDeviceSeen marks a device as seen to prevent duplicate DBIRTH messages
func (s *sparkplugOutput) markDeviceSeen(deviceID string) {
	if deviceID == "" {
		return
	}

	s.deviceStateMu.Lock()
	defer s.deviceStateMu.Unlock()

	s.seenDevices[deviceID] = true
}

// updateDeviceMetrics updates the metric cache for a device and returns true if new metrics were found.
// This implements the Device-Level PARRIS requirement that DBIRTH must contain ALL metrics
// before they can be used in DDATA messages. This function ensures we track all unique
// metrics per device across multiple incoming messages.
//
// Returns true when new metrics are discovered, triggering a fresh DBIRTH publication
// to maintain Sparkplug B compliance (all aliases must be defined before use).
func (s *sparkplugOutput) updateDeviceMetrics(deviceID string, data map[string]interface{}) bool {
	if deviceID == "" || len(data) == 0 {
		return false
	}

	s.deviceStateMu.Lock()
	defer s.deviceStateMu.Unlock()

	// Initialize device metrics map if not exists
	if s.deviceMetrics[deviceID] == nil {
		s.deviceMetrics[deviceID] = make(map[string]uint64)
	}

	deviceCache := s.deviceMetrics[deviceID]
	newMetricsFound := false

	// Check for new metrics
	for metricName := range data {
		if _, exists := deviceCache[metricName]; !exists {
			newMetricsFound = true
			// We'll assign the actual alias later in assignDynamicAliases
			deviceCache[metricName] = 0 // Placeholder
		}
	}

	return newMetricsFound
}

// getAllDeviceMetrics returns all known metrics for a device (cached + current).
// Essential for Device-Level PARRIS: DBIRTH must declare ALL metrics that will appear
// in DDATA messages. This function combines current message metrics with previously
// cached metrics to ensure complete DBIRTH declarations.
//
// Behavior:
// - Current message metrics use actual values
// - Previously seen metrics (not in current message) use their last known values
// - Metrics with no known values use null values
// - Ensures Sparkplug B compliance: no metric can appear in DDATA without prior DBIRTH definition
func (s *sparkplugOutput) getAllDeviceMetrics(deviceID string, currentData map[string]interface{}) map[string]interface{} {
	if deviceID == "" {
		return currentData
	}

	s.deviceStateMu.RLock()
	deviceCache := s.deviceMetrics[deviceID]
	deviceLastValues := s.deviceLastValues[deviceID]
	s.deviceStateMu.RUnlock()

	// Start with current data (these are the most recent values)
	allMetrics := make(map[string]interface{})
	for k, v := range currentData {
		allMetrics[k] = v
	}

	// Add cached metrics with their last known values (preserves state for DBIRTH)
	if deviceCache != nil {
		for metricName := range deviceCache {
			if _, exists := allMetrics[metricName]; !exists {
				// Use last known value for this device if available
				if deviceLastValues != nil {
					if lastValue, hasLastValue := deviceLastValues[metricName]; hasLastValue {
						allMetrics[metricName] = lastValue
					} else {
						// No last value known, use null (DBIRTH spec compliance)
						allMetrics[metricName] = nil
					}
				} else {
					// No last values stored for this device, use null
					allMetrics[metricName] = nil
				}
			}
		}
	}

	return allMetrics
}

// storeDeviceLastValues stores the current values for a device to be used in future DBIRTH messages.
// This ensures that when a device publishes DBIRTH with multiple metrics, previously seen metrics
// retain their last known values instead of being set to null.
func (s *sparkplugOutput) storeDeviceLastValues(deviceID string, data map[string]interface{}) {
	if deviceID == "" || len(data) == 0 {
		return
	}

	s.deviceStateMu.Lock()
	defer s.deviceStateMu.Unlock()

	// Initialize device last values if not exists
	if s.deviceLastValues[deviceID] == nil {
		s.deviceLastValues[deviceID] = make(map[string]interface{})
	}

	// Store current values for this device
	for metricName, value := range data {
		s.deviceLastValues[deviceID][metricName] = value
	}
}

// getStaticEdgeNodeID returns the static Edge Node ID for Sparkplug B compliance.
// Edge Node ID must be static throughout the session per Sparkplug B v3.0 specification.
func (s *sparkplugOutput) getStaticEdgeNodeID() string {
	// Edge Node ID must be configured and static for Sparkplug B compliance
	if s.config.Identity.EdgeNodeID != "" {
		return s.config.Identity.EdgeNodeID
	}

	// This is a critical configuration error - Edge Node ID is required for Sparkplug B compliance
	// Failing fast prevents publishing non-compliant messages
	panic("Critical configuration error: edge_node_id is required for Sparkplug B compliance - please set identity.edge_node_id in your configuration")
}

// getEONNodeID resolves the Edge of Network (EON) Node ID using the Parris Method.
// This method implements dynamic EON Node ID generation from UMH location_path metadata
// while maintaining backward compatibility with static configuration.
//
// Priority logic:
// 1. Dynamic Generation (Recommended): If location_path metadata exists, convert using Parris Method
//   - Converts UMH dot notation to Sparkplug colon notation
//   - Example: "enterprise.plant1.line3.station5" → "enterprise:plant1:line3:station5"
//
// 2. Static Override: If edge_node_id is configured, use it (ignores metadata)
// 3. Default Fallback: Use "default_node" with warning (should be avoided in production)
//
// The Parris Method enables ISA-95 hierarchical structures within Sparkplug topic namespace
// by encoding the organizational hierarchy directly into the EON Node ID using colon delimiters.
//
// Parameters:
//   - msg: The Benthos message containing potential location_path metadata
//
// Returns:

// Private methods for the rest of the implementation...
func (s *sparkplugOutput) createDeathMessage(msg *service.Message) (string, []byte) {
	// Phase 3: Always use static Edge Node ID for Sparkplug B compliance
	// This ensures DEATH messages use the same Edge Node ID as BIRTH/DATA messages
	eonNodeID := s.getStaticEdgeNodeID()

	var topic string
	if s.config.Identity.DeviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", s.config.Identity.GroupID, eonNodeID, s.config.Identity.DeviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NDEATH/%s", s.config.Identity.GroupID, eonNodeID)
	}

	bdSeqMetric := &sparkplugb.Payload_Metric{
		Name: func() *string { s := "bdSeq"; return &s }(),
		Value: &sparkplugb.Payload_Metric_LongValue{
			LongValue: s.bdSeq,
		},
		Datatype: func() *uint32 { d := uint32(4); return &d }(),
	}

	deathPayload := &sparkplugb.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(0); return &s }(), // NDEATH must have seq=0 per Sparkplug spec
		Metrics:   []*sparkplugb.Payload_Metric{bdSeqMetric},
	}

	payloadBytes, err := proto.Marshal(deathPayload)
	if err != nil {
		s.logger.Errorf("Failed to marshal DEATH payload: %v", err)
		return topic, []byte{}
	}

	return topic, payloadBytes
}

// publishDBIRTH publishes a Device BIRTH message for the specified device ID.
// This is the cornerstone of the Device-Level PARRIS method: DBIRTH defines ALL metrics
// that will be used in subsequent DDATA messages. Unlike NBIRTH (node-level), DBIRTH
// focuses exclusively on device-specific metrics with proper alias definitions.
//
// Key behaviors:
// - Accumulates all known metrics for the device (current + previously seen)
// - Assigns unique aliases for efficient DDATA transmission
// - Sets retained flag per Sparkplug B specification
// - Called on first device message OR when new metrics are discovered
func (s *sparkplugOutput) publishDBIRTH(deviceID string, data map[string]interface{}) error {
	if deviceID == "" {
		return fmt.Errorf("device ID cannot be empty for DBIRTH")
	}

	eonNodeID := s.getStaticEdgeNodeID()
	topic := fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", s.config.Identity.GroupID, eonNodeID, deviceID)

	var metrics []*sparkplugb.Payload_Metric

	// Add configured metrics for this device
	s.stateMu.RLock()
	for _, metricConfig := range s.metrics {
		metric := &sparkplugb.Payload_Metric{
			Name:     func() *string { s := metricConfig.Name; return &s }(),
			Alias:    &metricConfig.Alias,
			Datatype: s.getSparkplugDataType(metricConfig.Type),
		}

		if s.retainLastValues {
			if lastValue, exists := s.lastValues[metricConfig.Name]; exists {
				s.setMetricValue(metric, lastValue, metricConfig.Type)
			} else {
				metric.IsNull = func() *bool { b := true; return &b }()
			}
		} else {
			metric.IsNull = func() *bool { b := true; return &b }()
		}
		
		s.setDBirthMetricTimestamp(metric)
		metrics = append(metrics, metric)
	}

	s.stateMu.RUnlock()

	// Add dynamic metrics from current data (after releasing the read lock to avoid deadlock)
	for metricName, value := range data {
		// Skip if already added as configured metric
		found := false
		s.stateMu.RLock()
		for _, metricConfig := range s.metrics {
			if metricConfig.Name == metricName {
				found = true
				break
			}
		}
		s.stateMu.RUnlock()

		if found {
			continue
		}

		// Assign dynamic alias if not already assigned (this may acquire write locks)
		s.stateMu.RLock()
		_, exists := s.metricAliases[metricName]
		s.stateMu.RUnlock()

		if !exists {
			s.assignDynamicAliases([]string{metricName}, data)
		}

		s.stateMu.RLock()
		alias := s.metricAliases[metricName]
		metricType := s.metricTypes[metricName]
		s.stateMu.RUnlock()

		metric := &sparkplugb.Payload_Metric{
			Name:     func() *string { s := metricName; return &s }(),
			Alias:    &alias,
			Datatype: s.getSparkplugDataType(metricType),
		}

		s.setMetricValue(metric, value, metricType)
		s.setDBirthMetricTimestamp(metric)
		metrics = append(metrics, metric)
	}

	// SEQUENCE COUNTER FIX FOR DBIRTH:
	// DBIRTH must use incremented sequence counter from Edge Node level, NOT reset to 0.
	// Previous bug: DBIRTH was hardcoded to seq=0, same as NBIRTH, violating Sparkplug B spec.
	//
	// Sparkplug B specification requires sequence counter to increment across ALL message types:
	// - NBIRTH (seq=0)     ← First message after connection
	// - DBIRTH (seq=1,2,3) ← Device births increment sequence counter
	// - DDATA  (seq=4,5,6) ← Data messages continue incrementing
	// - Only DEATH messages should reset to seq=0
	s.stateMu.Lock()
	s.seqCounter++
	if s.seqCounter == 0 {
		s.sequenceWraps.Incr(1)
	}
	currentSeq := s.seqCounter
	s.stateMu.Unlock()

	birthPayload := &sparkplugb.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(currentSeq); return &s }(),
		Metrics:   metrics,
	}

	payloadBytes, err := proto.Marshal(birthPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal DBIRTH payload: %w", err)
	}

	// DBIRTH messages MUST be retained per Sparkplug B specification
	token := s.client.Publish(topic, s.config.MQTT.QoS, true, payloadBytes)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish DBIRTH message: %w", token.Error())
	}

	s.logger.Infof("Published retained DBIRTH message on topic: %s", topic)
	return nil
}

func (s *sparkplugOutput) publishBirthMessage() error {
	// Phase 3: Use static Edge Node ID for Sparkplug B compliance
	// This ensures BIRTH and DATA messages use the same Edge Node ID for proper alias resolution
	eonNodeID := s.getStaticEdgeNodeID()

	var topic string
	if s.config.Identity.DeviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", s.config.Identity.GroupID, eonNodeID, s.config.Identity.DeviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", s.config.Identity.GroupID, eonNodeID)
	}

	var metrics []*sparkplugb.Payload_Metric

	// Add bdSeq metric (required by Sparkplug spec)
	bdSeqMetric := &sparkplugb.Payload_Metric{
		Name:  func() *string { s := "bdSeq"; return &s }(),
		Alias: func() *uint64 { a := uint64(0); return &a }(),
		Value: &sparkplugb.Payload_Metric_LongValue{
			LongValue: s.bdSeq,
		},
		Datatype: func() *uint32 { d := uint32(4); return &d }(),
	}
	metrics = append(metrics, bdSeqMetric)

	// Add Node Control/Rebirth metric (required by Sparkplug spec for Edge Nodes)
	nodeControlMetric := &sparkplugb.Payload_Metric{
		Name:  func() *string { s := "Node Control/Rebirth"; return &s }(),
		Alias: func() *uint64 { a := uint64(1); return &a }(),
		Value: &sparkplugb.Payload_Metric_BooleanValue{
			BooleanValue: false, // Always false in NBIRTH
		},
		Datatype: func() *uint32 { d := uint32(11); return &d }(), // Boolean type
	}
	metrics = append(metrics, nodeControlMetric)

	// Add configured metrics
	s.stateMu.RLock()
	for _, metricConfig := range s.metrics {
		metric := &sparkplugb.Payload_Metric{
			Name:     func() *string { s := metricConfig.Name; return &s }(),
			Alias:    &metricConfig.Alias,
			Datatype: s.getSparkplugDataType(metricConfig.Type),
		}

		if s.retainLastValues {
			if lastValue, exists := s.lastValues[metricConfig.Name]; exists {
				s.setMetricValue(metric, lastValue, metricConfig.Type)
			} else {
				metric.IsNull = func() *bool { b := true; return &b }()
			}
		} else {
			metric.IsNull = func() *bool { b := true; return &b }()
		}
		
		s.setDBirthMetricTimestamp(metric)
		metrics = append(metrics, metric)
	}
	s.stateMu.RUnlock()

	// SEQUENCE COUNTER FIX FOR NBIRTH:
	// NBIRTH must use properly incremented sequence counter, NOT hardcoded to 0.
	// Previous bug: NBIRTH was hardcoded to seq=0, ignoring sequence progression.
	//
	// Fixed behavior:
	// - Initial connection: seqCounter=255 → increment → seq=0 (NBIRTH)
	// - Rebirth scenarios: seqCounter=X → increment → seq=X+1 (NBIRTH)
	// This ensures NBIRTH participates in the global Edge Node sequence progression.
	s.stateMu.Lock()
	s.seqCounter++
	if s.seqCounter == 0 {
		s.sequenceWraps.Incr(1)
	}
	currentSeq := s.seqCounter
	s.stateMu.Unlock()

	birthPayload := &sparkplugb.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(currentSeq); return &s }(),
		Metrics:   metrics,
	}

	payloadBytes, err := proto.Marshal(birthPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal BIRTH payload: %w", err)
	}

	// BIRTH messages MUST be retained per Sparkplug B specification
	// This allows Primary Hosts to receive current state when they connect
	token := s.client.Publish(topic, s.config.MQTT.QoS, true, payloadBytes)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish BIRTH message: %w", token.Error())
	}

	s.logger.Infof("Published retained BIRTH message on topic: %s", topic)
	return nil
}

func (s *sparkplugOutput) extractMessageData(msg *service.Message) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured message data: %w", err)
	}

	s.logger.Debugf("extractMessageData: Starting extraction - "+
		"autoExtractTagName: %t, configured metrics: %d, structured payload: %+v",
		s.autoExtractTagName, len(s.metrics), structured)

	if s.autoExtractTagName {
		if tagName, exists := msg.MetaGet("tag_name"); exists {
			s.logger.Debugf("extractMessageData: Found tag_name metadata: %s", tagName)

			// Try to match with configured metrics first
			for _, metricConfig := range s.metrics {
				if metricConfig.Name == tagName {
					s.logger.Debugf("extractMessageData: Matched tag_name %s with configured metric, extracting from path: %s", tagName, metricConfig.ValueFrom)
					value, err := s.extractValueFromPath(structured, metricConfig.ValueFrom)
					if err != nil {
						s.logger.Debugf("Failed to extract value for metric %s: %v", tagName, err)
						continue
					}
					data[tagName] = value
					break
				}
			}

			// If no configured metrics matched, try to generate metric name from virtual_path:tag_name
			if len(data) == 0 {
				if virtualPath, hasVirtualPath := msg.MetaGet("virtual_path"); hasVirtualPath {
					// convert virtual_path "." to ":"
					virtualPath = strings.ReplaceAll(virtualPath, ".", ":")
					// Generate metric name using virtual_path:tag_name format
					metricName := virtualPath + ":" + tagName
					s.logger.Debugf("extractMessageData: No matching configured metrics, generating metric name: %s", metricName)

					// Extract value from the tag_name field in the payload
					if value, err := s.extractValueFromPath(structured, tagName); err == nil {
						data[metricName] = value
						s.logger.Debugf("extractMessageData: Successfully extracted value for generated metric %s: %v", metricName, value)
					} else {
						s.logger.Debugf("extractMessageData: Failed to extract value for generated metric %s from path %s: %v", metricName, tagName, err)
					}
				} else {
					s.logger.Debugf("extractMessageData: tag_name %s found but no virtual_path metadata for metric generation", tagName)
				}
			}
		} else {
			s.logger.Debugf("extractMessageData: No tag_name metadata found")
		}
	}

	// Process only static configured metrics (from config file)
	// Dynamic metrics are handled above via tag_name metadata extraction
	for _, metricConfig := range s.metrics {
		if _, exists := data[metricConfig.Name]; exists {
			continue // Already extracted via tag_name metadata
		}

		s.logger.Debugf("extractMessageData: Processing static configured metric %s with value_from: %s", metricConfig.Name, metricConfig.ValueFrom)
		value, err := s.extractValueFromPath(structured, metricConfig.ValueFrom)
		if err != nil {
			s.logger.Debugf("Failed to extract value for static metric %s: %v", metricConfig.Name, err)
			continue
		}
		data[metricConfig.Name] = value
	}

	s.logger.Debugf("extractMessageData: Extraction complete - extracted %d metrics: %+v", len(data), data)
	return data, nil
}

func (s *sparkplugOutput) extractValueFromPath(structured interface{}, path string) (interface{}, error) {
	if structMap, ok := structured.(map[string]interface{}); ok {
		if value, exists := structMap[path]; exists {
			return value, nil
		}
	}
	return nil, fmt.Errorf("field %s not found", path)
}

func (s *sparkplugOutput) publishDataMessage(data map[string]interface{}, msg *service.Message) error {
	// P5 Dynamic Alias Implementation: Check for new metrics
	newMetrics := s.detectNewMetrics(data)
	if len(newMetrics) > 0 {
		s.logger.Infof("Detected %d new metrics: %v", len(newMetrics), newMetrics)

		if s.shouldTriggerRebirth() {
			// Assign dynamic aliases to new metrics
			s.assignDynamicAliases(newMetrics, data)

			// Trigger rebirth sequence
			if err := s.triggerRebirth(); err != nil {
				return fmt.Errorf("failed to trigger rebirth for new metrics: %w", err)
			}

			// Skip this DATA message - rebirth will announce the new metrics
			// Next DATA messages will include the new metrics normally
			s.logger.Debug("Skipping DATA message during rebirth sequence")
			return nil
		} else {
			s.logger.Debug("Rebirth debounced or already pending, skipping new metrics for now")
			// Continue with existing metrics only
		}
	}

	// Check if rebirth is pending - if so, skip DATA messages
	s.dynamicMu.RLock()
	if s.rebirthPending {
		s.dynamicMu.RUnlock()
		s.logger.Debug("Rebirth pending, skipping DATA message")
		return nil
	}
	s.dynamicMu.RUnlock()

	// Phase 3: Use static Edge Node ID and device-level PARRIS
	eonNodeID := s.getStaticEdgeNodeID()
	deviceID := s.getParrisDeviceID(msg)

	var topic string
	if deviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DDATA/%s/%s", s.config.Identity.GroupID, eonNodeID, deviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NDATA/%s", s.config.Identity.GroupID, eonNodeID)
	}

	s.stateMu.Lock()
	s.seqCounter++
	if s.seqCounter == 0 {
		s.sequenceWraps.Incr(1)
	}
	currentSeq := s.seqCounter
	s.stateMu.Unlock()

	var metrics []*sparkplugb.Payload_Metric

	s.logger.Debugf("Starting metrics creation loop - data map has %d entries: %v", len(data), data)
	for metricName, value := range data {
		s.stateMu.RLock()
		alias, hasAlias := s.metricAliases[metricName]
		metricType, hasType := s.metricTypes[metricName]
		s.stateMu.RUnlock()

		if !hasAlias || !hasType {
			// P5: New metrics are now handled above, this should only happen during debounce
			s.logger.Debugf("Metric %s not configured (may be new metric during debounce), skipping", metricName)
			continue
		}

		metric := &sparkplugb.Payload_Metric{
			Alias:    &alias,
			Datatype: s.getSparkplugDataType(metricType),
		}

		s.setMetricValue(metric, value, metricType)
		s.logger.Debugf("About to call setMetricTimestamp for metric: %s", metricName)
		s.setMetricTimestamp(metric, msg)
		metrics = append(metrics, metric)

		if s.retainLastValues {
			s.stateMu.Lock()
			s.lastValues[metricName] = value
			s.stateMu.Unlock()
		}
	}

	if len(metrics) == 0 {
		s.logger.Debug("No configured metrics to publish in this DATA message")
		return nil // Don't error on empty messages - may happen during dynamic alias assignment
	}

	dataPayload := &sparkplugb.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(currentSeq); return &s }(),
		Metrics:   metrics,
	}

	payloadBytes, err := proto.Marshal(dataPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal DATA payload: %w", err)
	}

	token := s.client.Publish(topic, s.config.MQTT.QoS, false, payloadBytes)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish DATA message: %w", token.Error())
	}

	s.logger.Debugf("Published DATA message with %d metrics on topic: %s", len(metrics), topic)
	return nil
}

func (s *sparkplugOutput) getSparkplugDataType(typeStr string) *uint32 {
	typeMap := map[string]uint32{
		"int8": 1, "int16": 2, "int32": 3, "int64": 4,
		"uint8": 5, "uint16": 6, "uint32": 7, "uint64": 8,
		"float": 9, "double": 10, "boolean": 11, "string": 12,
	}

	if dataType, exists := typeMap[strings.ToLower(typeStr)]; exists {
		return &dataType
	}

	defaultType := uint32(10) // Default to double
	return &defaultType
}

func (s *sparkplugOutput) setMetricValue(metric *sparkplugb.Payload_Metric, value interface{}, metricType string) {
	switch strings.ToLower(metricType) {
	case "int8", "int16", "int32":
		if intVal, ok := s.convertToUint32(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_IntValue{IntValue: intVal}
		}
	case "int64":
		if longVal, ok := s.convertToInt64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_LongValue{LongValue: longVal}
		}
	case "float":
		if floatVal, ok := s.convertToFloat32(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_FloatValue{FloatValue: floatVal}
		}
	case "double":
		if doubleVal, ok := s.convertToFloat64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: doubleVal}
		}
	case "boolean":
		if boolVal, ok := s.convertToBool(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: boolVal}
		}
	case "string":
		if strVal, ok := s.convertToString(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_StringValue{StringValue: strVal}
		}
	}
}

// setMetricTimestamp sets the timestamp field on a Sparkplug B metric using direct field assignment
// Now uses the complete official protobuf definition that includes timestamp field support
func (s *sparkplugOutput) setMetricTimestamp(metric *sparkplugb.Payload_Metric, msg *service.Message) {
	// Get current timestamp in milliseconds
	timestamp := uint64(time.Now().UnixMilli())
	
	// Try to extract timestamp from message metadata first
	if metaTS, exists := msg.MetaGet("timestamp_ms"); exists && metaTS != "" {
		if parsedTS, err := strconv.ParseUint(metaTS, 10, 64); err == nil {
			timestamp = parsedTS
		}
	}
	
	// Direct field assignment using the complete protobuf definition
	metric.Timestamp = &timestamp
	s.logger.Debugf("Set metric timestamp: %d", timestamp)
}

// setDBirthMetricTimestamp sets the timestamp field on a DBIRTH metric using current time
func (s *sparkplugOutput) setDBirthMetricTimestamp(metric *sparkplugb.Payload_Metric) {
	timestamp := uint64(time.Now().UnixMilli())
	
	// Direct field assignment using the complete protobuf definition
	metric.Timestamp = &timestamp
}

func (s *sparkplugOutput) convertToUint32(value interface{}) (uint32, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			if i < 0 {
				return 0, false
			}
			return uint32(i), true
		}
	}
	return 0, false
}

func (s *sparkplugOutput) convertToInt64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false // Consistent with ConvertToInt32 behavior
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			if i < 0 {
				return 0, false
			}
			return uint64(i), true
		}
	}
	return 0, false
}

func (s *sparkplugOutput) convertToFloat32(value interface{}) (float32, bool) {
	switch v := value.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case int:
		return float32(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return float32(f), true
		}
	}
	return 0, false
}

func (s *sparkplugOutput) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (s *sparkplugOutput) convertToBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		return strings.ToLower(v) == "true", true
	case int:
		return v != 0, true
	case float64:
		return v != 0, true
	}
	return false, false
}

func (s *sparkplugOutput) convertToString(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return string(v), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// P5 Dynamic Alias Implementation - Helper Methods

// detectNewMetrics identifies metrics that don't have aliases and need dynamic assignment
func (s *sparkplugOutput) detectNewMetrics(data map[string]interface{}) []string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	var newMetrics []string
	for metricName := range data {
		if _, hasAlias := s.metricAliases[metricName]; !hasAlias {
			newMetrics = append(newMetrics, metricName)
		}
	}
	return newMetrics
}

// shouldTriggerRebirth determines if a rebirth should be triggered based on debouncing
func (s *sparkplugOutput) shouldTriggerRebirth() bool {
	s.dynamicMu.RLock()
	defer s.dynamicMu.RUnlock()

	// Check if rebirth is already pending
	if s.rebirthPending {
		return false
	}

	// Check debounce period
	timeSinceLastRebirth := time.Since(s.lastRebirthTime).Milliseconds()
	return timeSinceLastRebirth >= s.rebirthDebounceMs
}

// assignDynamicAliases assigns aliases and types to new metrics
func (s *sparkplugOutput) assignDynamicAliases(newMetrics []string, data map[string]interface{}) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	for _, metricName := range newMetrics {
		// Assign alias
		s.metricAliases[metricName] = s.nextAlias
		s.nextAlias++

		// Infer type from value
		value := data[metricName]
		metricType := s.typeConverter.InferMetricType(value)
		s.metricTypes[metricName] = metricType

		s.logger.Infof("Assigned dynamic alias %d to metric '%s' (type: %s)",
			s.metricAliases[metricName], metricName, metricType)
	}

	// NOTE: We do NOT append to s.metrics here to avoid global accumulation bug
	// Dynamic metrics are tracked via s.metricAliases and s.metricTypes maps
	// s.metrics should only contain static configuration from config file
}

// triggerRebirth initiates a rebirth sequence with new metrics
func (s *sparkplugOutput) triggerRebirth() error {
	s.dynamicMu.Lock()
	s.rebirthPending = true
	s.lastRebirthTime = time.Now()
	s.dynamicMu.Unlock()

	s.logger.Info("Triggering rebirth sequence due to new metrics")

	// Increment bdSeq for rebirth
	s.stateMu.Lock()
	s.bdSeq++
	s.stateMu.Unlock()

	// Publish new BIRTH message with all metrics (existing + new)
	if err := s.publishBirthMessage(); err != nil {
		s.dynamicMu.Lock()
		s.rebirthPending = false
		s.dynamicMu.Unlock()
		return fmt.Errorf("failed to publish rebirth message: %w", err)
	}

	s.birthsPublished.Incr(1)
	s.logger.Info("Successfully published rebirth message")

	// Clear rebirth pending flag
	s.dynamicMu.Lock()
	s.rebirthPending = false
	s.dynamicMu.Unlock()

	return nil
}
