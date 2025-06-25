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

package sparkplug_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

func init() {
	inputSpec := service.NewConfigSpec().
		Version("2.0.0").
		Summary("Sparkplug B MQTT input with idiomatic configuration").
		Description(`A Sparkplug B input that supports multiple roles and configuration styles:

ROLES:
- primary_host: Acts as SCADA/Primary Application, subscribes to all groups (spBv1.0/+/#)
- edge_node: Acts as Edge Node, subscribes only to its own group (spBv1.0/{group}/#)  
- hybrid: Combines both behaviors (rare, but useful for gateways)

CONFIGURATION STYLES:
- Idiomatic: Uses mqtt/identity/role/behaviour sections for clean organization
- Legacy: Supports existing flat configuration for backward compatibility

Key features:
- Clean configuration structure with mqtt/identity/role/behaviour sections
- Automatic STATE topic management with LWT
- Role-based subscription behavior (all groups vs single group)
- Sequence number validation and rebirth coordination
- Alias resolution using BIRTH message metadata
- Configurable message processing (splitting, extraction, filtering)
- Comprehensive metrics and monitoring`).
		// MQTT Transport Configuration
		Field(service.NewObjectField("mqtt",
			service.NewStringListField("urls").
				Description("List of MQTT broker URLs to connect to").
				Example([]string{"tcp://localhost:1883", "ssl://broker.hivemq.com:8883"}).
				Default([]string{"tcp://localhost:1883"}),
			service.NewStringField("client_id").
				Description("MQTT client ID for this input plugin").
				Default("benthos-sparkplug-input"),
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
				Description("QoS level for MQTT operations (0, 1, or 2)").
				Default(1).
				Examples(0, 1, 2),
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
				Description("Edge Node ID within the group (e.g., 'Line3')").
				Example("Line3").
				Optional(),
			service.NewStringField("device_id").
				Description("Device ID under the edge node (optional, if not specified acts as node-level)").
				Default("").
				Optional()).
			Description("Sparkplug identity configuration")).
		// Role Configuration
		Field(service.NewStringField("role").
			Description("Sparkplug role: 'primary_host' (subscribe to all groups), 'edge_node' (own group only), or 'hybrid' (both)").
			Default("primary_host")).
		// Subscription Configuration
		Field(service.NewObjectField("subscription",
			service.NewStringListField("groups").
				Description("Specific groups to subscribe to for primary_host and hybrid roles. Empty means all groups (+)").
				Example([]string{"benthos", "factory1", "test"}).
				Default([]string{}).
				Optional()).
			Description("Subscription filtering configuration for primary_host role").
			Optional()).
		// Behaviour Configuration
		Field(service.NewObjectField("behaviour",
			service.NewBoolField("auto_split_metrics").
				Description("Split multi-metric messages into individual metric messages").
				Default(true),
			service.NewBoolField("data_messages_only").
				Description("Only process DATA messages (drop BIRTH/DEATH after processing)").
				Default(false),
			service.NewBoolField("data_only").
				Description("Skip publishing metrics from birth messages, only emit DATA messages").
				Default(false),
			service.NewBoolField("enable_rebirth_req").
				Description("Send rebirth requests on sequence gaps").
				Default(true),
			service.NewBoolField("drop_birth_messages").
				Description("Drop BIRTH messages after alias extraction").
				Default(false),
			service.NewBoolField("strict_topic_validation").
				Description("Strictly validate Sparkplug topic format").
				Default(false),
			service.NewBoolField("auto_extract_values").
				Description("Extract metric values as message payload").
				Default(true)).
			Description("Processing behavior configuration").
			Optional())

	err := service.RegisterBatchInput(
		"sparkplug_b",
		inputSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newSparkplugInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type sparkplugInput struct {
	config Config
	logger *service.Logger

	// MQTT client and state
	client   mqtt.Client
	messages chan mqttMessage
	mu       sync.RWMutex
	closed   bool

	// Sparkplug state management using core components
	nodeStates map[string]*nodeState // deviceKey -> state

	// Core components for shared functionality
	aliasCache        *AliasCache
	topicParser       *TopicParser
	messageProcessor  *MessageProcessor
	typeConverter     *TypeConverter
	mqttClientBuilder *MQTTClientBuilder

	// Legacy alias cache for backward compatibility during transition
	legacyAliasCache map[string]map[uint64]string // deviceKey -> (alias -> metric name)
	stateMu          sync.RWMutex

	// Metrics
	messagesReceived  *service.MetricCounter
	messagesProcessed *service.MetricCounter
	messagesDropped   *service.MetricCounter
	messagesErrored   *service.MetricCounter
	birthsProcessed   *service.MetricCounter
	deathsProcessed   *service.MetricCounter
	rebirthsRequested *service.MetricCounter
	sequenceErrors    *service.MetricCounter
	aliasResolutions  *service.MetricCounter
}

type mqttMessage struct {
	topic   string
	payload []byte
}

type nodeState struct {
	lastSeen time.Time
	lastSeq  uint8
	bdSeq    uint64
	isOnline bool
}

func newSparkplugInput(conf *service.ParsedConfig, mgr *service.Resources) (*sparkplugInput, error) {
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

	qosInt, err := mqttConf.FieldInt("qos")
	if err != nil {
		return nil, err
	}
	if qosInt < 0 || qosInt > 2 {
		return nil, fmt.Errorf("QoS must be 0, 1, or 2, got %d", qosInt)
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

	// Parse credentials sub-section if present
	credsConf := mqttConf.Namespace("credentials")
	config.MQTT.Credentials.Username, _ = credsConf.FieldString("username")
	config.MQTT.Credentials.Password, _ = credsConf.FieldString("password")

	// Parse identity section using namespace
	identityConf := conf.Namespace("identity")
	config.Identity.GroupID, err = identityConf.FieldString("group_id")
	if err != nil {
		return nil, err
	}

	config.Identity.EdgeNodeID, _ = identityConf.FieldString("edge_node_id")
	// edge_node_id is optional for input plugin

	config.Identity.DeviceID, _ = identityConf.FieldString("device_id")

	// Parse role
	roleStr, err := conf.FieldString("role")
	if err != nil {
		return nil, fmt.Errorf("failed to parse role: %w", err)
	}
	config.Role = Role(roleStr)

	// Parse subscription section using namespace (optional)
	if conf.Contains("subscription") {
		subscriptionConf := conf.Namespace("subscription")
		groups, err := subscriptionConf.FieldStringList("groups")
		if err == nil {
			config.Subscription.Groups = groups
		}
	}

	// Parse behaviour section using namespace
	behaviourConf := conf.Namespace("behaviour")
	config.Behaviour.AutoSplitMetrics, _ = behaviourConf.FieldBool("auto_split_metrics")
	config.Behaviour.DataMessagesOnly, _ = behaviourConf.FieldBool("data_messages_only")
	config.Behaviour.DataOnly, _ = behaviourConf.FieldBool("data_only")
	config.Behaviour.EnableRebirthReq, _ = behaviourConf.FieldBool("enable_rebirth_req")
	config.Behaviour.DropBirthMessages, _ = behaviourConf.FieldBool("drop_birth_messages")
	config.Behaviour.StrictTopicValidation, _ = behaviourConf.FieldBool("strict_topic_validation")
	config.Behaviour.AutoExtractValues, _ = behaviourConf.FieldBool("auto_extract_values")

	si := &sparkplugInput{
		config:            config,
		logger:            mgr.Logger(),
		messages:          make(chan mqttMessage, 1000),
		nodeStates:        make(map[string]*nodeState),
		legacyAliasCache:  make(map[string]map[uint64]string),
		aliasCache:        NewAliasCache(),
		topicParser:       NewTopicParser(),
		messageProcessor:  NewMessageProcessor(mgr.Logger()),
		typeConverter:     NewTypeConverter(),
		mqttClientBuilder: NewMQTTClientBuilder(mgr),
		messagesReceived:  mgr.Metrics().NewCounter("messages_received"),
		messagesProcessed: mgr.Metrics().NewCounter("messages_processed"),
		messagesDropped:   mgr.Metrics().NewCounter("messages_dropped"),
		messagesErrored:   mgr.Metrics().NewCounter("messages_errored"),
		birthsProcessed:   mgr.Metrics().NewCounter("births_processed"),
		deathsProcessed:   mgr.Metrics().NewCounter("deaths_processed"),
		rebirthsRequested: mgr.Metrics().NewCounter("rebirths_requested"),
		sequenceErrors:    mgr.Metrics().NewCounter("sequence_errors"),
		aliasResolutions:  mgr.Metrics().NewCounter("alias_resolutions"),
	}

	return si, nil
}

func (s *sparkplugInput) Connect(ctx context.Context) error {
	s.logger.Infof("Connecting Sparkplug B input (role: %s)", s.config.Role)

	// Prepare MQTT client configuration
	stateTopic := s.config.GetStateTopic()
	statePayload := []byte("OFFLINE")

	mqttConfig := MQTTClientConfig{
		BrokerURLs:       s.config.MQTT.URLs,
		ClientID:         s.config.MQTT.ClientID,
		Username:         s.config.MQTT.Credentials.Username,
		Password:         s.config.MQTT.Credentials.Password,
		KeepAlive:        s.config.MQTT.KeepAlive,
		ConnectTimeout:   s.config.MQTT.ConnectTimeout,
		CleanSession:     s.config.MQTT.CleanSession,
		WillTopic:        stateTopic,
		WillPayload:      statePayload,
		WillQoS:          s.config.MQTT.QoS,
		WillRetain:       true,
		OnConnect:        s.onConnect,
		OnConnectionLost: s.onConnectionLost,
		MessageHandler: func(client mqtt.Client, msg mqtt.Message) {
			s.logger.Warnf("Received message on unhandled topic: %s", msg.Topic())
		},
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

func (s *sparkplugInput) onConnect(client mqtt.Client) {
	s.logger.Info("MQTT client connected, setting up Sparkplug subscriptions")

	// Get subscription topics based on role
	topics := s.config.GetSubscriptionTopics()

	s.logger.Infof("Operating as %s - subscribing to: %v", s.config.Role, topics)

	// Subscribe to all required topics
	for _, topic := range topics {
		err := s.mqttClientBuilder.SubscribeWithMetrics(client, topic, s.config.MQTT.QoS, s.messageHandler)
		if err != nil {
			s.logger.Errorf("Failed to subscribe to topic %s: %v", topic, err)
			return
		}
		s.logger.Infof("Subscribed to Sparkplug topic: %s", topic)
	}

	// Publish STATE ONLINE for primary host and hybrid roles
	if s.config.Role == RolePrimaryHost || s.config.Role == RoleHybrid {
		stateTopic := s.config.GetStateTopic()
		err := s.mqttClientBuilder.PublishWithMetrics(client, stateTopic, s.config.MQTT.QoS, false, "ONLINE")
		if err != nil {
			s.logger.Errorf("Failed to publish STATE ONLINE: %v", err)
			return
		}
		s.logger.Infof("Published STATE ONLINE on topic: %s", stateTopic)
	}
}

func (s *sparkplugInput) onConnectionLost(client mqtt.Client, err error) {
	s.logger.Errorf("MQTT connection lost: %v", err)
}

func (s *sparkplugInput) messageHandler(client mqtt.Client, msg mqtt.Message) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return
	}

	// DEBUG: Log entry point as recommended in the plan
	s.logger.Debugf("📥 messageHandler: received message on topic %s, payload length %d",
		msg.Topic(), len(msg.Payload()))

	s.messagesReceived.Incr(1)

	// Non-blocking send to message channel
	select {
	case s.messages <- mqttMessage{topic: msg.Topic(), payload: msg.Payload()}:
		s.logger.Debugf("✅ messageHandler: queued message for processing")
	default:
		s.logger.Warn("Message buffer full, dropping message")
		s.messagesDropped.Incr(1)
	}
}

func (s *sparkplugInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case mqttMsg := <-s.messages:
		s.logger.Debugf("🔍 ReadBatch: processing message from topic %s", mqttMsg.topic)
		batch, err := s.processSparkplugMessage(mqttMsg)
		if err != nil {
			s.logger.Errorf("❌ ReadBatch: failed to process message: %v", err)
			s.messagesErrored.Incr(1)
			return nil, nil, err
		}
		if batch == nil || len(batch) == 0 {
			s.logger.Debugf("⚠️ ReadBatch: no batch produced for message")
			return nil, func(ctx context.Context, err error) error { return nil }, nil
		}
		s.logger.Debugf("✅ ReadBatch: produced batch with %d messages", len(batch))
		return batch, func(ctx context.Context, err error) error { return nil }, err
	}
}

func (s *sparkplugInput) processSparkplugMessage(mqttMsg mqttMessage) (service.MessageBatch, error) {
	// DEBUG: Log processing entry as recommended in the plan
	s.logger.Debugf("🔄 processSparkplugMessage: starting to process topic %s", mqttMsg.topic)

	// Parse topic to extract Sparkplug components
	msgType, deviceKey, topicInfo := s.parseSparkplugTopicDetailed(mqttMsg.topic)
	if msgType == "" {
		s.logger.Debugf("Ignoring non-Sparkplug topic: %s", mqttMsg.topic)
		return nil, nil
	}

	s.logger.Debugf("📊 processSparkplugMessage: parsed topic - msgType=%s, deviceKey=%s", msgType, deviceKey)

	// **FIX: Filter STATE messages from protobuf parsing**
	// STATE messages contain plain text "ONLINE"/"OFFLINE", not protobuf payloads
	if msgType == "STATE" {
		s.logger.Debugf("🏛️ processSparkplugMessage: processing STATE message (payload: %s)", string(mqttMsg.payload))
		return s.processStateMessage(deviceKey, msgType, topicInfo, mqttMsg.topic, string(mqttMsg.payload))
	}

	// DEBUG: Log before protobuf unmarshal as recommended in the plan
	s.logger.Debugf("🔍 processSparkplugMessage: attempting to unmarshal %d bytes as Sparkplug payload", len(mqttMsg.payload))

	// Decode Sparkplug payload
	var payload sproto.Payload
	if err := proto.Unmarshal(mqttMsg.payload, &payload); err != nil {
		s.logger.Errorf("Failed to unmarshal Sparkplug payload from topic %s: %v", mqttMsg.topic, err)
		s.messagesErrored.Incr(1)
		return nil, nil
	}

	// DEBUG: Log after successful protobuf unmarshal
	s.logger.Debugf("✅ processSparkplugMessage: successfully unmarshaled payload with %d metrics", len(payload.Metrics))

	isBirthMessage := strings.Contains(msgType, "BIRTH")
	isDataMessage := strings.Contains(msgType, "DATA")
	isDeathMessage := strings.Contains(msgType, "DEATH")
	isCommandMessage := strings.Contains(msgType, "CMD")

	s.logger.Debugf("🏷️ processSparkplugMessage: message type classification - birth=%v, data=%v, death=%v, command=%v",
		isBirthMessage, isDataMessage, isDeathMessage, isCommandMessage)

	var batch service.MessageBatch

	if isBirthMessage {
		s.logger.Debugf("🎂 processSparkplugMessage: processing BIRTH message")
		s.processBirthMessage(deviceKey, msgType, &payload)
		s.birthsProcessed.Incr(1)

		// Implement data_only filter - skip emitting birth metrics if enabled
		if s.config.Behaviour.DataOnly {
			s.logger.Debugf("🚫 processSparkplugMessage: data_only=true, skipping birth metric emission")
			// Still process birth for alias table but don't emit metrics
			return nil, nil
		}

		if s.config.Behaviour.AutoSplitMetrics {
			batch = s.createSplitMessages(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		} else {
			batch = s.createSingleMessage(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		}
	} else if isDataMessage {
		s.logger.Debugf("📈 processSparkplugMessage: processing DATA message")
		s.processDataMessage(deviceKey, msgType, &payload)

		if s.config.Behaviour.AutoSplitMetrics {
			batch = s.createSplitMessages(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		} else {
			batch = s.createSingleMessage(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		}
	} else if isDeathMessage {
		s.logger.Debugf("💀 processSparkplugMessage: processing DEATH message")
		s.processDeathMessage(deviceKey, msgType, &payload)
		s.deathsProcessed.Incr(1)

		// Create status event message for death
		batch = s.createDeathEventMessage(msgType, deviceKey, topicInfo, mqttMsg.topic)
	} else if isCommandMessage {
		s.logger.Debugf("⚡ processSparkplugMessage: processing COMMAND message")
		batch = s.processCommandMessage(deviceKey, msgType, &payload, topicInfo, mqttMsg.topic)
	}

	// DEBUG: Log when pushing to Benthos pipeline as recommended in the plan
	if batch != nil && len(batch) > 0 {
		s.logger.Debugf("🚀 processSparkplugMessage: created batch with %d messages for Benthos pipeline", len(batch))
	} else {
		s.logger.Debugf("⚠️ processSparkplugMessage: no batch created - this might be the issue!")
	}

	return batch, nil
}

// processBirthMessage handles both NBIRTH and DBIRTH messages.
// In Device-Level PARRIS architecture, DBIRTH messages are the primary mechanism
// for establishing alias mappings. NBIRTH messages handle node-level metrics but
// are not used in pure device-level deployments.
//
// Key behavior: Caches alias → metric name mappings from BIRTH certificates
// for use in subsequent DATA message resolution.
func (s *sparkplugInput) processBirthMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Update node state
	if state, exists := s.nodeStates[deviceKey]; exists {
		state.isOnline = true
		state.lastSeen = time.Now()
		if payload.Seq != nil {
			state.lastSeq = uint8(*payload.Seq)
		}
		if payload.Timestamp != nil {
			// Extract bdSeq from metrics if present
			for _, metric := range payload.Metrics {
				if metric.Name != nil && *metric.Name == "bdSeq" {
					if metric.GetLongValue() != 0 {
						state.bdSeq = metric.GetLongValue()
					}
				}
			}
		}
	} else {
		state := &nodeState{
			isOnline: true,
			lastSeen: time.Now(),
		}
		if payload.Seq != nil {
			state.lastSeq = uint8(*payload.Seq)
		}
		s.nodeStates[deviceKey] = state
	}

	// Cache aliases from birth message
	s.cacheAliases(deviceKey, payload.Metrics)

	s.logger.Debugf("Processed %s for device %s", msgType, deviceKey)
}

func (s *sparkplugInput) processDataMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Check sequence numbers for out-of-order detection (Sparkplug B spec compliance)
	if state, exists := s.nodeStates[deviceKey]; exists && payload.Seq != nil {
		currentSeq := uint8(*payload.Seq)
		expectedSeq := uint8((int(state.lastSeq) + 1) % 256)

		// Validate sequence according to Sparkplug B specification
		isValidSequence := s.validateSequenceNumber(state.lastSeq, currentSeq)

		if !isValidSequence {
			s.logger.Warnf("Sequence gap detected for device %s: expected %d, got %d",
				deviceKey, expectedSeq, currentSeq)
			s.sequenceErrors.Incr(1)

			// Mark node as stale until rebirth (Sparkplug spec requirement)
			state.isOnline = false

			// Send rebirth request if enabled
			if s.config.Behaviour.EnableRebirthReq {
				s.sendRebirthRequest(deviceKey)
			}
		}

		state.lastSeq = currentSeq
		state.lastSeen = time.Now()
	}

	// Resolve aliases in data message
	s.resolveAliases(deviceKey, payload.Metrics)
}

func (s *sparkplugInput) processDeathMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if state, exists := s.nodeStates[deviceKey]; exists {
		state.isOnline = false
		state.lastSeen = time.Now()
	}

	s.logger.Debugf("Processed %s for device %s", msgType, deviceKey)
}

func (s *sparkplugInput) processCommandMessage(deviceKey, msgType string, payload *sproto.Payload, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	s.logger.Debugf("⚡ processCommandMessage: processing %s for device %s with %d metrics", msgType, deviceKey, len(payload.Metrics))

	// Update node state timestamp for activity tracking
	s.stateMu.Lock()
	if state, exists := s.nodeStates[deviceKey]; exists {
		state.lastSeen = time.Now()
	} else {
		s.nodeStates[deviceKey] = &nodeState{
			lastSeen: time.Now(),
			isOnline: true, // Assume online if receiving commands
		}
	}
	s.stateMu.Unlock()

	// Check for rebirth command
	for _, metric := range payload.Metrics {
		if metric.Name != nil && *metric.Name == "Node Control/Rebirth" {
			if metric.GetBooleanValue() {
				s.logger.Infof("🔄 Rebirth request received for device %s", deviceKey)
				// Handle rebirth logic here if needed for edge nodes
				// For primary hosts, this is typically just logged
			}
		}
	}

	// Create batch from command metrics - commands should be processed like any other Sparkplug message
	var batch service.MessageBatch
	if s.config.Behaviour.AutoSplitMetrics {
		batch = s.createSplitMessages(payload, msgType, deviceKey, topicInfo, originalTopic)
	} else {
		batch = s.createSingleMessage(payload, msgType, deviceKey, topicInfo, originalTopic)
	}

	s.logger.Debugf("✅ processCommandMessage: created batch with %d messages for %s", len(batch), msgType)
	return batch
}

func (s *sparkplugInput) processStateMessage(deviceKey, msgType string, topicInfo *TopicInfo, originalTopic string, statePayload string) (service.MessageBatch, error) {
	s.logger.Debugf("🏛️ processStateMessage: processing STATE message for device %s, state: %s", deviceKey, statePayload)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Update node state based on STATE message content
	isOnline := statePayload == "ONLINE"
	if state, exists := s.nodeStates[deviceKey]; exists {
		state.isOnline = isOnline
		state.lastSeen = time.Now()
	} else {
		s.nodeStates[deviceKey] = &nodeState{
			isOnline: isOnline,
			lastSeen: time.Now(),
		}
	}

	// Create a status event message for STATE changes
	event := map[string]interface{}{
		"event":        "StateChange",
		"device_key":   deviceKey,
		"group_id":     topicInfo.Group,
		"edge_node_id": topicInfo.EdgeNode,
		"state":        statePayload,
		"timestamp_ms": time.Now().UnixMilli(),
	}

	if topicInfo.Device != "" {
		event["device_id"] = topicInfo.Device
	}

	jsonBytes, err := json.Marshal(event)
	if err != nil {
		s.logger.Errorf("Failed to marshal STATE event: %v", err)
		return nil, nil
	}

	msg := service.NewMessage(jsonBytes)
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}
	msg.MetaSet("event_type", "state_change")
	msg.MetaSet("node_state", statePayload)

	s.logger.Debugf("✅ processStateMessage: created STATE event message for device %s: %s", deviceKey, statePayload)

	return service.MessageBatch{msg}, nil
}

func (s *sparkplugInput) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	if s.client != nil && s.client.IsConnected() {
		// Publish STATE OFFLINE before disconnecting (for primary host and hybrid roles)
		if s.config.Role == RolePrimaryHost || s.config.Role == RoleHybrid {
			stateTopic := s.config.GetStateTopic()
			token := s.client.Publish(stateTopic, s.config.MQTT.QoS, false, "OFFLINE")
			token.WaitTimeout(5 * time.Second)
		}

		s.client.Disconnect(1000)
	}

	close(s.messages)

	s.logger.Info("Sparkplug input closed")
	return nil
}

// Helper methods that delegate to the existing processor logic where possible
func (s *sparkplugInput) cacheAliases(deviceKey string, metrics []*sproto.Payload_Metric) {
	// DEBUG: Log before alias caching as recommended in the plan
	s.logger.Debugf("🗃️ cacheAliases: starting to cache aliases for deviceKey=%s, %d metrics", deviceKey, len(metrics))

	// Use core component instead of processor
	count := s.aliasCache.CacheAliases(deviceKey, metrics)
	if count > 0 {
		s.logger.Debugf("✅ cacheAliases: cached %d aliases for device %s", count, deviceKey)

		// DEBUG: Log the actual aliases cached (helpful for debugging)
		for _, metric := range metrics {
			if metric.Name != nil && metric.Alias != nil {
				s.logger.Debugf("   🔗 cached alias %d -> '%s'", *metric.Alias, *metric.Name)
			}
		}
	} else {
		s.logger.Debugf("⚠️ cacheAliases: no aliases cached for device %s", deviceKey)
	}
}

// resolveAliases converts numeric aliases back to metric names using cached BIRTH certificates.
// This is the complement to DBIRTH processing: while DBIRTH establishes the alias → name mappings,
// resolveAliases applies those mappings to DDATA messages for efficient processing.
//
// Critical for Device-Level PARRIS: DDATA messages contain only aliases (for efficiency),
// but downstream processing needs the original metric names from the DBIRTH certificate.
func (s *sparkplugInput) resolveAliases(deviceKey string, metrics []*sproto.Payload_Metric) {
	// DEBUG: Log before alias resolution as recommended in the plan
	s.logger.Debugf("🔍 resolveAliases: starting to resolve aliases for deviceKey=%s, %d metrics", deviceKey, len(metrics))

	// Use core component instead of processor
	count := s.aliasCache.ResolveAliases(deviceKey, metrics)
	if count > 0 {
		s.aliasResolutions.Incr(int64(count))
		s.logger.Debugf("✅ resolveAliases: resolved %d aliases for device %s", count, deviceKey)

		// DEBUG: Log the actual resolutions (critical for debugging)
		for _, metric := range metrics {
			if metric.Name != nil && metric.Alias != nil {
				s.logger.Debugf("   🎯 resolved alias %d -> '%s'", *metric.Alias, *metric.Name)
			} else if metric.Alias != nil && metric.Name == nil {
				s.logger.Debugf("   ❌ FAILED to resolve alias %d (no name found)", *metric.Alias)
			}
		}
	} else {
		s.logger.Debugf("⚠️ resolveAliases: no aliases resolved for device %s - this could be the issue!", deviceKey)
	}
}

func (s *sparkplugInput) parseSparkplugTopicDetailed(topic string) (string, string, *TopicInfo) {
	// Use core component instead of processor
	return s.topicParser.ParseSparkplugTopicDetailed(topic)
}

// Message creation methods
func (s *sparkplugInput) createSplitMessages(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	var batch service.MessageBatch

	for _, metric := range payload.Metrics {
		if metric == nil {
			continue
		}

		msg := s.createMessageFromMetric(metric, payload, msgType, deviceKey, topicInfo, originalTopic)
		if msg != nil {
			batch = append(batch, msg)
		}
	}

	return batch
}

func (s *sparkplugInput) createSingleMessage(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	msg := s.createMessageFromPayload(payload, msgType, deviceKey, topicInfo, originalTopic)
	if msg == nil {
		return nil
	}
	return service.MessageBatch{msg}
}

func (s *sparkplugInput) createMessageFromMetric(metric *sproto.Payload_Metric, payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) *service.Message {
	// Extract metric value as JSON
	value := s.extractMetricValue(metric)
	if value == nil {
		return nil
	}

	msg := service.NewMessage(value)

	// Set Sparkplug metadata
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}

	// Enhanced metadata enrichment as per expert specification
	msg.MetaSet("spb_group", topicInfo.Group)
	msg.MetaSet("spb_edge_node", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device", topicInfo.Device)
	}

	// Set metric name as tag
	tagName := "unknown_metric"
	if metric.Name != nil && *metric.Name != "" {
		tagName = *metric.Name
	} else if metric.Alias != nil {
		tagName = fmt.Sprintf("alias_%d", *metric.Alias)
	}
	msg.MetaSet("tag_name", tagName)

	// Enhanced sequence and timing metadata
	if payload.Seq != nil {
		msg.MetaSet("sparkplug_seq", fmt.Sprintf("%d", *payload.Seq))
		msg.MetaSet("spb_seq", fmt.Sprintf("%d", *payload.Seq))
	}

	if payload.Timestamp != nil {
		msg.MetaSet("timestamp_ms", fmt.Sprintf("%d", *payload.Timestamp))
		msg.MetaSet("spb_timestamp", fmt.Sprintf("%d", *payload.Timestamp))
	}

	// Add metric-specific metadata
	if metric.Alias != nil {
		msg.MetaSet("spb_alias", fmt.Sprintf("%d", *metric.Alias))
	}

	if metric.Datatype != nil {
		msg.MetaSet("spb_datatype", s.getDataTypeName(*metric.Datatype))
	}

	if metric.IsHistorical != nil {
		msg.MetaSet("spb_is_historical", fmt.Sprintf("%t", *metric.IsHistorical))
	}

	// Add birth-death sequence if available from node state
	s.stateMu.RLock()
	if state, exists := s.nodeStates[deviceKey]; exists {
		msg.MetaSet("spb_bdseq", fmt.Sprintf("%d", state.bdSeq))
	}
	s.stateMu.RUnlock()

	return msg
}

func (s *sparkplugInput) createMessageFromPayload(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) *service.Message {
	// Convert entire payload to JSON
	jsonData := s.payloadToJSON(payload)
	if jsonData == nil {
		return nil
	}

	msg := service.NewMessage(jsonData)

	// Set metadata
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}

	return msg
}

func (s *sparkplugInput) createDeathEventMessage(msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	event := map[string]interface{}{
		"event":        "DeviceOffline",
		"device_key":   deviceKey,
		"group_id":     topicInfo.Group,
		"edge_node_id": topicInfo.EdgeNode,
		"timestamp_ms": time.Now().UnixMilli(),
	}

	if topicInfo.Device != "" {
		event["device_id"] = topicInfo.Device
	}

	jsonBytes, err := json.Marshal(event)
	if err != nil {
		s.logger.Errorf("Failed to marshal death event: %v", err)
		return nil
	}

	msg := service.NewMessage(jsonBytes)
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("event_type", "device_offline")

	return service.MessageBatch{msg}
}

func (s *sparkplugInput) extractMetricValue(metric *sproto.Payload_Metric) []byte {
	// Create a simple JSON structure with the metric value
	result := make(map[string]interface{})

	// Add metric name if available
	if metric.Name != nil && *metric.Name != "" {
		result["name"] = *metric.Name
	}

	// Add alias if available
	if metric.Alias != nil && *metric.Alias != 0 {
		result["alias"] = *metric.Alias
	}

	// Extract value based on type
	if metric.IsNull != nil && *metric.IsNull {
		result["value"] = nil
	} else if value := metric.GetValue(); value != nil {
		switch v := value.(type) {
		case *sproto.Payload_Metric_IntValue:
			result["value"] = v.IntValue
		case *sproto.Payload_Metric_LongValue:
			result["value"] = v.LongValue
		case *sproto.Payload_Metric_FloatValue:
			result["value"] = v.FloatValue
		case *sproto.Payload_Metric_DoubleValue:
			result["value"] = v.DoubleValue
		case *sproto.Payload_Metric_BooleanValue:
			result["value"] = v.BooleanValue
		case *sproto.Payload_Metric_StringValue:
			result["value"] = v.StringValue
		default:
			result["value"] = nil
		}
	}

	// Note: Individual metrics don't have timestamps in Sparkplug B
	// Timestamp is at the payload level

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		s.logger.Errorf("Failed to marshal metric value: %v", err)
		return nil
	}

	return jsonBytes
}

// getDataTypeName converts Sparkplug data type ID to human-readable string
func (s *sparkplugInput) getDataTypeName(datatype uint32) string {
	switch datatype {
	case 1:
		return "Int8"
	case 2:
		return "Int16"
	case 3:
		return "Int32"
	case 4:
		return "Int64"
	case 5:
		return "UInt8"
	case 6:
		return "UInt16"
	case 7:
		return "UInt32"
	case 8:
		return "UInt64"
	case 9:
		return "Float"
	case 10:
		return "Double"
	case 11:
		return "Boolean"
	case 12:
		return "String"
	case 13:
		return "DateTime"
	case 14:
		return "Text"
	default:
		return "Unknown"
	}
}

func (s *sparkplugInput) payloadToJSON(payload *sproto.Payload) []byte {
	// Create JSON representation of entire payload
	result := make(map[string]interface{})

	if payload.Timestamp != nil {
		result["timestamp_ms"] = *payload.Timestamp
	}

	if payload.Seq != nil {
		result["seq"] = *payload.Seq
	}

	if len(payload.Metrics) > 0 {
		metrics := make([]map[string]interface{}, 0, len(payload.Metrics))
		for _, metric := range payload.Metrics {
			if metric == nil {
				continue
			}

			metricJSON := make(map[string]interface{})
			if metric.Name != nil {
				metricJSON["name"] = *metric.Name
			}
			if metric.Alias != nil {
				metricJSON["alias"] = *metric.Alias
			}

			// Add value
			if metric.IsNull != nil && *metric.IsNull {
				metricJSON["value"] = nil
			} else if value := metric.GetValue(); value != nil {
				switch v := value.(type) {
				case *sproto.Payload_Metric_IntValue:
					metricJSON["value"] = v.IntValue
				case *sproto.Payload_Metric_LongValue:
					metricJSON["value"] = v.LongValue
				case *sproto.Payload_Metric_FloatValue:
					metricJSON["value"] = v.FloatValue
				case *sproto.Payload_Metric_DoubleValue:
					metricJSON["value"] = v.DoubleValue
				case *sproto.Payload_Metric_BooleanValue:
					metricJSON["value"] = v.BooleanValue
				case *sproto.Payload_Metric_StringValue:
					metricJSON["value"] = v.StringValue
				}
			}

			metrics = append(metrics, metricJSON)
		}
		result["metrics"] = metrics
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		s.logger.Errorf("Failed to marshal payload to JSON: %v", err)
		return nil
	}

	return jsonBytes
}

func (s *sparkplugInput) sendRebirthRequest(deviceKey string) {
	if s.client == nil || !s.client.IsConnected() {
		return
	}

	// Parse device key to get topic components
	parts := strings.Split(deviceKey, "/")
	if len(parts) < 2 {
		return
	}

	var topic string
	if len(parts) == 2 {
		// Node level rebirth
		topic = fmt.Sprintf("spBv1.0/%s/NCMD/%s", parts[0], parts[1])
	} else {
		// Device level rebirth
		topic = fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", parts[0], parts[1], parts[2])
	}

	// Create rebirth command payload
	rebirthMetric := &sproto.Payload_Metric{
		Name: func() *string { s := "Node Control/Rebirth"; return &s }(),
		Value: &sproto.Payload_Metric_BooleanValue{
			BooleanValue: true,
		},
		Datatype: func() *uint32 { d := uint32(11); return &d }(), // Boolean type
	}

	cmdPayload := &sproto.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Metrics:   []*sproto.Payload_Metric{rebirthMetric},
	}

	payloadBytes, err := proto.Marshal(cmdPayload)
	if err != nil {
		s.logger.Errorf("Failed to marshal rebirth command: %v", err)
		return
	}

	token := s.client.Publish(topic, s.config.MQTT.QoS, false, payloadBytes)
	if token.Wait() && token.Error() != nil {
		s.logger.Errorf("Failed to publish rebirth command: %v", token.Error())
		return
	}

	s.rebirthsRequested.Incr(1)
	s.logger.Infof("Sent rebirth request to %s on topic %s", deviceKey, topic)
}

// validateSequenceNumber checks if a received sequence number is valid according to Sparkplug B spec
func (s *sparkplugInput) validateSequenceNumber(lastSeq, currentSeq uint8) bool {
	// Calculate expected next sequence with wraparound
	expectedNext := uint8((int(lastSeq) + 1) % 256)

	// Direct match is valid
	if currentSeq == expectedNext {
		return true
	}

	// Handle wraparound case (255 -> 0)
	if lastSeq == 255 && currentSeq == 0 {
		return true
	}

	// Allow small gaps based on default tolerance (5)
	maxGap := uint8(5) // Default gap tolerance per Sparkplug spec

	// Calculate gap size
	var gap uint8
	if currentSeq > lastSeq {
		gap = currentSeq - lastSeq - 1
	} else {
		// Handle wraparound gap calculation
		gap = uint8((256 - int(lastSeq) - 1) + int(currentSeq))
	}

	// Allow small gaps within tolerance
	return gap <= maxGap
}
