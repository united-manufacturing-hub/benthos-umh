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
	"google.golang.org/protobuf/proto"

	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// Sparkplug B Data Type Constants
const (
	SparkplugDataTypeInt8     = uint32(1)
	SparkplugDataTypeInt16    = uint32(2)
	SparkplugDataTypeInt32    = uint32(3)
	SparkplugDataTypeInt64    = uint32(4)
	SparkplugDataTypeUInt8    = uint32(5)
	SparkplugDataTypeUInt16   = uint32(6)
	SparkplugDataTypeUInt32   = uint32(7)
	SparkplugDataTypeUInt64   = uint32(8)
	SparkplugDataTypeFloat    = uint32(9)
	SparkplugDataTypeDouble   = uint32(10)
	SparkplugDataTypeBoolean  = uint32(11)
	SparkplugDataTypeString   = uint32(12)
	SparkplugDataTypeDateTime = uint32(13)
	SparkplugDataTypeText     = uint32(14)
)

func init() {
	inputSpec := service.NewConfigSpec().
		Version("2.0.0").
		Summary("Sparkplug B MQTT input with idiomatic configuration").
		Description(`A Sparkplug B input plugin with three Host roles:

SPARKPLUG B HOST ROLES:
- secondary_passive (default): Read-only consumer, no rebirth commands, safe for brownfield
- secondary_active: Active consumer, sends rebirth commands, no STATE publishing
- primary: Full Primary Host with STATE publishing and session management

Key features:
- Three-role system for different deployment scenarios
- Safe default role prevents rebirth storms
- Automatic STATE topic management with LWT (Primary role only)
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
				Description("Sparkplug Group ID (e.g., 'FactoryA'). Serves two purposes: it is the publishing identity, and it is the default subscription filter (subscribes to spBv1.0/<group_id>/#). Set subscription.groups to override the filter.").
				Example("FactoryA"),
			service.NewStringField("edge_node_id").
				Description("For the 'primary' role this field is required and is used as the Sparkplug v3.0 host_id in the STATE topic (spBv1.0/STATE/<host_id>). For the 'secondary_passive' and 'secondary_active' roles it is optional and used only for identification.").
				Example("PrimaryHost").
				Optional(),
			service.NewStringField("device_id").
				Description("Device ID under the edge node (optional, if not specified acts as node-level)").
				Default("").
				Optional()).
			Description("Sparkplug identity configuration")).
		// Role Configuration
		Field(service.NewStringField("role").
			Description("Sparkplug Host role: 'secondary_passive' (default), 'secondary_active', or 'primary'").
			Default("secondary_passive")).
		// Discovery REBIRTH Configuration
		Field(service.NewBoolField("request_birth_on_connect").
			Description("Send REBIRTH when DATA arrives from a node with no prior BIRTH on this bridge. Typical after a bridge restart with no retained NBIRTH/DBIRTH on the broker. Ignored under `secondary_passive`. Controls only the discovery path; sequence-gap and unresolved-aliases recovery always run.").
			Default(true).
			Optional()).
		Field(service.NewDurationField("birth_request_throttle").
			Description("Minimum time between REBIRTH commands to the same node, applied across every rebirth reason. Prevents the alias-recovery storm where DATA arriving in the BIRTH round-trip window fires another rebirth. Set to `0` to disable throttling.").
			Default("1s").
			Optional()).
		Field(service.NewBoolField("include_edge_node_in_location").
			Description("Nest device-level data under its Sparkplug edge node, so location_path becomes <edge_node>.<device> (e.g. 'Line1.IO_Controller'). Enable for native/brownfield Sparkplug where the edge node is a real hierarchy level and devices on different nodes may share a name. Leave disabled (default) for UMH Parris-encoded publishers where device_id already carries the full location path. Node-level data (no device) is unaffected.").
			Default(false).
			Examples(true, false).
			Advanced()).
		// Extension Decoding (opt-in, ENG-5229)
		Field(service.NewObjectField("decode_extensions",
			service.NewStringField("extensions").
				Description("Inline proto2 schema declaring extensions of the Sparkplug `Payload.MetaData` and/or `Payload.MetricValueExtension` messages. Write only `package` + `extend` blocks, plus any custom message types or well-known-type imports; the plugin compiles it as proto2 and adds the Sparkplug import (do not write a `syntax` line or import the Sparkplug schema). Scalar extensions are attached per carrying metric as `spb_ext_<field>`; the full decoded metric (including message-typed extensions) as `spb_metric_decoded` JSON. Metrics without an extension are unaffected.").
				Default("").
				Optional()).
			Description("Opt-in decoding of proto2 Sparkplug extension fields that the standard decode retains but ignores.").
			Advanced().
			Optional()).
		// Subscription Configuration
		Field(service.NewObjectField("subscription",
			service.NewStringListField("groups").
				Description("Groups to subscribe to. When empty (the default), the plugin filters to identity.group_id. Set multiple group IDs to listen to several groups, or [\"+\"] to subscribe to every group via the MQTT wildcard.").
				Example([]string{"benthos", "factory1", "test"}).
				Default([]string{}).
				Optional()).
			Description("Subscription filtering configuration. Defaults to filtering by `identity.group_id`. Set `subscription.groups` to listen to multiple groups or to `[\"+\"]` to subscribe to every group.").
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
	done     chan struct{} // Signal for graceful shutdown
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

	stateMu sync.RWMutex

	// Per-node REBIRTH throttle. Shared across every reason sendRebirthRequest
	// dispatches (discovery, sequence gap, unresolved aliases) so concurrent
	// reasons on the same node collapse into a single NCMD per throttle window.
	// Unbounded: grows once per unique nodeKey ever observed by this process.
	// For long-running bridges seeing GUID-per-session edges (AGVs, CI), a
	// follow-up TTL eviction may be warranted.
	//
	// Lock ordering: callers must release stateMu before acquiring
	// birthRequestMu. The pattern in processDataMessage demonstrates this.
	birthRequested map[string]time.Time // nodeKey -> last REBIRTH request time
	birthRequestMu sync.RWMutex         // Protects birthRequested

	// Metrics
	messagesReceived        *service.MetricCounter
	messagesProcessed       *service.MetricCounter
	messagesDropped         *service.MetricCounter
	messagesErrored         *service.MetricCounter
	birthsProcessed         *service.MetricCounter
	deathsProcessed         *service.MetricCounter
	rebirthsRequested       *service.MetricCounter
	rebirthsSuppressed      *service.MetricCounter
	sequenceErrors          *service.MetricCounter
	aliasResolutions        *service.MetricCounter
	aliasResolutionFailures *service.MetricCounter // Per-metric: alias present in DATA but not in cache
	discoveryRebirths       *service.MetricCounter // REBIRTH requests sent for discovery
	sequenceGapRebirths     *service.MetricCounter // REBIRTH requests sent because of a sequence number gap
	aliasRebirths           Counter                // REBIRTH requests sent because DATA aliases were unresolved

	// Extension decoding (ENG-5229); nil when decode_extensions is unset.
	extDecoder            *extensionDecoder
	extensionDecodeErrors *service.MetricCounter
	extDecodeLogMu        sync.Mutex
	extDecodeLogLast      time.Time
}

type mqttMessage struct {
	topic   string
	payload []byte
}

type nodeState = NodeState // Type alias for backward compatibility

// StateAction represents the required actions after processing a state update.
// This struct enables clean separation between state mutation and I/O operations.
// Exported for testing to verify state transition logic in isolation.
type StateAction struct {
	IsNewNode    bool // True if node was newly discovered (requires BIRTH request)
	NeedsRebirth bool // True if sequence gap detected (requires rebirth command)
}

// Counter interface for metric counters (for testing)
type Counter interface {
	Incr(delta int64, labelValues ...string)
}

// sameDispatchThreshold separates "two reasons firing on the same DATA message"
// (co-fire, logs at debug) from "a genuine retry inside the throttle window"
// (logs at info). 100ms is far below the default 1s throttle but well above the
// microsecond gap between sequential calls inside one processDataMessage.
const sameDispatchThreshold = 100 * time.Millisecond

// rebirthReason tags why sendRebirthRequest was invoked. It picks the right
// metric counter, decides whether the RequestBirthOnConnect feature flag
// applies, and stamps the log line so dashboards can attribute each NCMD
// to the signal that triggered it.
//
// String-backed (matching MessageType in sparkplug_b_core.go) so the zero
// value is the empty string rather than a real reason; a future caller
// forgetting the reason argument fails the switch instead of silently taking
// the discovery path.
type rebirthReason string

const (
	rebirthReasonDiscovery         rebirthReason = "discovery"
	rebirthReasonSequenceGap       rebirthReason = "sequence-gap"
	rebirthReasonUnresolvedAliases rebirthReason = "unresolved-aliases"
)

// UpdateNodeState is a pure function that updates node state and determines required actions.
// This function encapsulates all state transition logic for DATA message processing,
// enabling deterministic testing without I/O operations.
//
// Behavior:
// - New nodes: Creates initial state, returns IsNewNode=true
// - Valid sequence: Updates state, returns no action
// - Sequence gap: Marks offline, returns NeedsRebirth=true
// - Wraparound (255→0): Treated as valid sequence
//
// Exported for testing to verify state transition logic in isolation.
func UpdateNodeState(nodeStates map[string]*NodeState, deviceKey string, currentSeq uint8) StateAction {
	state, exists := nodeStates[deviceKey]

	if !exists {
		// NEW NODE DISCOVERED - create initial state
		nodeStates[deviceKey] = &NodeState{
			IsOnline: true,
			LastSeen: time.Now(),
			LastSeq:  currentSeq,
		}
		return StateAction{
			IsNewNode:    true,
			NeedsRebirth: false,
		}
	}

	// EXISTING NODE - validate sequence number
	isValidSequence := ValidateSequenceNumber(state.LastSeq, currentSeq)

	// Update all state regardless of sequence validity
	state.LastSeq = currentSeq
	state.LastSeen = time.Now()
	state.IsOnline = isValidSequence // Mark offline if sequence gap detected

	return StateAction{
		IsNewNode:    false,
		NeedsRebirth: !isValidSequence, // Request rebirth if sequence gap detected
	}
}

// LogSequenceError logs a sequence gap error with consistent formatting and increments the error counter.
// This function centralizes sequence error logging to ensure uniform error messages across the codebase.
//
// Behavior:
// - Calculates expected sequence with wraparound (255→0)
// - Logs warning with expected/actual sequence numbers
// - Increments the provided error counter
//
// Exported for testing to verify error logging behavior.
func LogSequenceError(logger *service.Logger, counter Counter, deviceKey string, lastSeq uint8, currentSeq uint8) {
	// Calculate expected sequence with wraparound
	expectedSeq := uint8((int(lastSeq) + 1) % 256)

	// Log warning if logger is provided (may be nil in tests)
	if logger != nil {
		logger.Warnf("Sequence gap detected for device %s: expected %d, got %d",
			deviceKey, expectedSeq, currentSeq)
	}

	// Increment error counter
	if counter != nil {
		counter.Incr(1)
	}
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

	// Parse role (Host-only for input plugin)
	roleStr, err := conf.FieldString("role")
	if err != nil {
		return nil, fmt.Errorf("failed to parse role: %w", err)
	}

	// Parse role into three-role system
	switch roleStr {
	case "secondary_passive":
		config.Role = RoleSecondaryPassive
	case "secondary_active":
		config.Role = RoleSecondaryActive
	case "primary":
		config.Role = RolePrimaryHost
	default:
		return nil, fmt.Errorf("invalid role '%s': must be 'secondary_passive' (default), 'secondary_active', or 'primary'", roleStr)
	}

	// Parse discovery REBIRTH configuration
	config.RequestBirthOnConnect, _ = conf.FieldBool("request_birth_on_connect")
	config.BirthRequestThrottle, _ = conf.FieldDuration("birth_request_throttle")

	config.IncludeEdgeNodeInLocation, _ = conf.FieldBool("include_edge_node_in_location")

	// Parse extension decoding (optional)
	if conf.Contains("decode_extensions") {
		config.DecodeExtensions, _ = conf.Namespace("decode_extensions").FieldString("extensions")
	}

	// Parse subscription section using namespace (optional)
	if conf.Contains("subscription") {
		subscriptionConf := conf.Namespace("subscription")
		var groups []string
		groups, err = subscriptionConf.FieldStringList("groups")
		if err == nil {
			config.Subscription.Groups = groups
		}
	}

	// Behavior is now hardcoded for simplicity:
	// - AutoSplitMetrics: true (required for UMH-Core format)
	// - DataOnly: false (birth messages contain valuable state)
	// - EnableRebirthReq: true (required for Sparkplug B compliance)
	// - AutoExtractValues: true (required for UMH-Core format)

	// Validate configuration (this will auto-detect the role)
	if err = config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Compile the extension schema once at startup (fail fast on a bad snippet).
	var extDecoder *extensionDecoder
	if strings.TrimSpace(config.DecodeExtensions) != "" {
		extDecoder, err = newExtensionDecoder(config.DecodeExtensions)
		if err != nil {
			return nil, fmt.Errorf("decode_extensions: %w", err)
		}
	}

	si := &sparkplugInput{
		config:                  config,
		logger:                  mgr.Logger(),
		messages:                make(chan mqttMessage, 1000),
		done:                    make(chan struct{}),
		nodeStates:              make(map[string]*nodeState),
		birthRequested:          make(map[string]time.Time), // Per-node REBIRTH throttle
		aliasCache:              NewAliasCache(),
		topicParser:             NewTopicParser(),
		messageProcessor:        NewMessageProcessor(mgr.Logger()),
		typeConverter:           NewTypeConverter(),
		mqttClientBuilder:       NewMQTTClientBuilder(mgr),
		messagesReceived:        mgr.Metrics().NewCounter("messages_received"),
		messagesProcessed:       mgr.Metrics().NewCounter("messages_processed"),
		messagesDropped:         mgr.Metrics().NewCounter("messages_dropped"),
		messagesErrored:         mgr.Metrics().NewCounter("messages_errored"),
		birthsProcessed:         mgr.Metrics().NewCounter("births_processed"),
		deathsProcessed:         mgr.Metrics().NewCounter("deaths_processed"),
		rebirthsRequested:       mgr.Metrics().NewCounter("rebirths_requested"),
		rebirthsSuppressed:      mgr.Metrics().NewCounter("rebirths_suppressed"),
		sequenceErrors:          mgr.Metrics().NewCounter("sequence_errors"),
		aliasResolutions:        mgr.Metrics().NewCounter("alias_resolutions"),
		aliasResolutionFailures: mgr.Metrics().NewCounter("alias_resolution_failures"),
		discoveryRebirths:       mgr.Metrics().NewCounter("discovery_rebirths"),
		sequenceGapRebirths:     mgr.Metrics().NewCounter("sequence_gap_rebirths"),
		aliasRebirths:           mgr.Metrics().NewCounter("alias_rebirths"),
		extDecoder:              extDecoder,
		extensionDecodeErrors:   mgr.Metrics().NewCounter("extension_decode_errors"),
	}

	return si, nil
}

func (s *sparkplugInput) Connect(_ context.Context) error {
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
		MessageHandler: func(_ mqtt.Client, msg mqtt.Message) {
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

	// Publish STATE ONLINE for primary host role
	if s.config.Role == RolePrimaryHost {
		stateTopic := s.config.GetStateTopic()
		s.logger.Infof("Primary Host: using identity.edge_node_id='%s' as Sparkplug v3.0 host_id for STATE topic %s",
			s.config.Identity.EdgeNodeID, stateTopic)
		err := s.mqttClientBuilder.PublishWithMetrics(client, stateTopic, s.config.MQTT.QoS, false, "ONLINE")
		if err != nil {
			s.logger.Errorf("Failed to publish STATE ONLINE: %v", err)
			return
		}
		s.logger.Infof("Published STATE ONLINE on topic: %s", stateTopic)
	}
}

func (s *sparkplugInput) onConnectionLost(_ mqtt.Client, err error) {
	s.logger.Errorf("MQTT connection lost: %v", err)
}

func (s *sparkplugInput) messageHandler(_ mqtt.Client, msg mqtt.Message) {
	// Check if we're shutting down
	select {
	case <-s.done:
		// Shutting down, don't process
		return
	default:
		// Continue processing
	}

	// DEBUG: Log entry point as recommended in the plan
	s.logger.Debugf("📥 messageHandler: received message on topic %s, payload length %d",
		msg.Topic(), len(msg.Payload()))

	s.messagesReceived.Incr(1)

	// Check if messages channel is closed before attempting to send
	// This prevents race condition where Close() closes the channel
	// after done check but before the send operation
	// Use RLock since we're only reading the closed flag
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()

	// Non-blocking send to message channel with shutdown check
	select {
	case s.messages <- mqttMessage{topic: msg.Topic(), payload: msg.Payload()}:
		s.logger.Debugf("✅ messageHandler: queued message for processing")
	case <-s.done:
		// Shutting down, drop message silently
		return
	default:
		s.logger.Warn("Message buffer full, dropping message")
		s.messagesDropped.Incr(1)
	}
}

func (s *sparkplugInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.done:
		return nil, nil, service.ErrEndOfInput
	case mqttMsg := <-s.messages:
		s.logger.Debugf("🔍 ReadBatch: processing message from topic %s", mqttMsg.topic)
		batch, err := s.processSparkplugMessage(mqttMsg)
		if err != nil {
			s.logger.Errorf("❌ ReadBatch: failed to process message: %v", err)
			s.messagesErrored.Incr(1)
			return nil, nil, err
		}
		if len(batch) == 0 {
			s.logger.Debugf("⚠️ ReadBatch: no batch produced for message")
			return nil, func(_ context.Context, _ error) error { return nil }, nil
		}
		s.logger.Debugf("✅ ReadBatch: produced batch with %d messages", len(batch))
		return batch, func(_ context.Context, _ error) error { return nil }, err
	}
}

func (s *sparkplugInput) processSparkplugMessage(mqttMsg mqttMessage) (service.MessageBatch, error) {
	// DEBUG: Log processing entry as recommended in the plan
	s.logger.Debugf("🔄 processSparkplugMessage: starting to process topic %s", mqttMsg.topic)

	// Parse topic to extract Sparkplug components
	msgType, topicInfo := s.parseSparkplugTopicDetailed(mqttMsg.topic)
	if !msgType.IsValid() {
		s.logger.Debugf("Ignoring non-Sparkplug topic: %s", mqttMsg.topic)
		return nil, nil
	}

	s.logger.Debugf("📊 processSparkplugMessage: parsed topic - msgType=%s, deviceKey=%s", msgType, topicInfo.DeviceKey())

	// **FIX: Filter STATE messages from protobuf parsing**
	// STATE messages contain plain text "ONLINE"/"OFFLINE", not protobuf payloads
	if msgType.IsState() {
		s.logger.Debugf("🏛️ processSparkplugMessage: processing STATE message (payload: %s)", string(mqttMsg.payload))
		return s.processStateMessage(msgType, topicInfo, mqttMsg.topic, string(mqttMsg.payload))
	}

	// DEBUG: Log before protobuf unmarshal as recommended in the plan
	s.logger.Debugf("🔍 processSparkplugMessage: attempting to unmarshal %d bytes as Sparkplug payload", len(mqttMsg.payload))

	// Decode Sparkplug payload
	var payload sparkplugb.Payload
	if err := proto.Unmarshal(mqttMsg.payload, &payload); err != nil {
		s.logger.Errorf("Failed to unmarshal Sparkplug payload from topic %s: %v", mqttMsg.topic, err)
		s.messagesErrored.Incr(1)
		return nil, nil
	}

	// DEBUG: Log after successful protobuf unmarshal
	s.logger.Debugf("✅ processSparkplugMessage: successfully unmarshaled payload with %d metrics", len(payload.Metrics))

	isBirthMessage := msgType.IsBirth()
	isDataMessage := msgType.IsData()
	isDeathMessage := msgType.IsDeath()
	isCommandMessage := msgType.IsCommand()

	s.logger.Debugf("🏷️ processSparkplugMessage: message type classification - birth=%v, data=%v, death=%v, command=%v",
		isBirthMessage, isDataMessage, isDeathMessage, isCommandMessage)

	var batch service.MessageBatch

	if isBirthMessage {
		s.logger.Debugf("🎂 processSparkplugMessage: processing BIRTH message")
		s.processBirthMessage(msgType, &payload, topicInfo)
		s.birthsProcessed.Incr(1)

		// Always process birth messages (they contain valuable current state)
		// Always split metrics for UMH-Core format (one metric per message)
		batch = s.createSplitMessages(&payload, msgType, topicInfo, mqttMsg.topic)
	} else if isDataMessage {
		s.logger.Debugf("📈 processSparkplugMessage: processing DATA message")
		s.processDataMessage(msgType, &payload, topicInfo)

		// Always split metrics for UMH-Core format (one metric per message)
		batch = s.createSplitMessages(&payload, msgType, topicInfo, mqttMsg.topic)
	} else if isDeathMessage {
		s.logger.Debugf("💀 processSparkplugMessage: processing DEATH message")
		s.processDeathMessage(msgType, &payload, topicInfo)
		s.deathsProcessed.Incr(1)

		// Create status event message for death
		batch = s.createDeathEventMessage(msgType, topicInfo, mqttMsg.topic)
	} else if isCommandMessage {
		s.logger.Debugf("⚡ processSparkplugMessage: processing COMMAND message")
		batch = s.processCommandMessage(msgType, &payload, topicInfo, mqttMsg.topic)
	}

	// DEBUG: Log when pushing to Benthos pipeline as recommended in the plan
	if len(batch) > 0 {
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
func (s *sparkplugInput) processBirthMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// ENG-4031: Use node-level key for sequence tracking.
	// Per Sparkplug B spec, sequence is tracked at NODE scope, not device scope.
	nodeKey := topicInfo.NodeKey()

	// Update node state using nodeKey for sequence tracking
	if state, exists := s.nodeStates[nodeKey]; exists {
		state.IsOnline = true
		state.LastSeen = time.Now()
		state.LastSeq = GetSequenceNumber(payload)
		if payload.Timestamp != nil {
			// Extract bdSeq from metrics if present
			for _, metric := range payload.Metrics {
				if metric.Name != nil && *metric.Name == "bdSeq" {
					if metric.GetLongValue() != 0 {
						state.BdSeq = metric.GetLongValue()
					}
				}
			}
		}
	} else {
		state := &nodeState{
			IsOnline: true,
			LastSeen: time.Now(),
			LastSeq:  GetSequenceNumber(payload),
		}
		s.nodeStates[nodeKey] = state
	}

	// Cache aliases from birth message
	// NOTE: Alias caching uses deviceKey - aliases ARE per-device from DBIRTH
	s.cacheAliases(topicInfo.DeviceKey(), payload.Metrics)

	s.logger.Debugf("Processed %s for device %s (node: %s)", msgType, topicInfo.DeviceKey(), nodeKey)
}

// processDataMessage handles NDATA/DDATA messages. State mutation
// (sequence-number tracking, alias resolution) runs under stateMu; logging
// and MQTT I/O run after the lock is released. Three rebirth reasons may fire
// independently on the same message: discovery (new node), sequence-gap
// (UpdateNodeState detected a missing seq), and unresolved-aliases (DATA
// referenced aliases not in the cache).
func (s *sparkplugInput) processDataMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	s.stateMu.Lock()

	currentSeq := GetSequenceNumber(payload)

	// ENG-4031: Use node-level key for sequence tracking.
	// Per Sparkplug B spec, sequence is tracked at NODE scope, not device scope.
	// All message types from a node (NBIRTH, NDATA, DBIRTH, DDATA) share one counter.
	nodeKey := topicInfo.NodeKey()

	// Capture previous sequence before UpdateNodeState modifies it
	var prevSeq uint8
	if state, exists := s.nodeStates[nodeKey]; exists {
		prevSeq = state.LastSeq
	}

	action := UpdateNodeState(s.nodeStates, nodeKey, currentSeq)

	// Resolve aliases while holding lock (safe operation)
	// NOTE: Alias resolution uses deviceKey - aliases ARE per-device from DBIRTH
	unresolvedAliases := s.resolveAliases(topicInfo.DeviceKey(), payload.Metrics)

	s.stateMu.Unlock()

	// Logging after lock release to minimize lock hold time
	if action.IsNewNode {
		s.logger.Infof("Discovered new node from %s message: %s (node: %s)", msgType, topicInfo.DeviceKey(), nodeKey)
	}

	if action.NeedsRebirth {
		LogSequenceError(s.logger, s.sequenceErrors, nodeKey, prevSeq, currentSeq)
	}

	// Three independent conditions can each trigger a rebirth on the same DATA
	// message. The call sites stay flat (no in-code precedence) because the
	// shared throttle inside sendRebirthRequest collapses concurrent reasons
	// into at most one broker command per BirthRequestThrottle window, and
	// each reason ticks its own per-reason counter for dashboard attribution.
	// Collapsing into an ordered chain would lose that per-reason metric
	// breakdown without affecting NCMD output.
	//
	// A bridge restart with no retained BIRTH (IsNewNode=true AND
	// unresolvedAliases>0) still recovers when RequestBirthOnConnect is off,
	// because the alias-recovery reason bypasses that feature flag.
	if action.IsNewNode {
		s.sendRebirthRequest(nodeKey, rebirthReasonDiscovery)
	}
	if action.NeedsRebirth {
		s.sendRebirthRequest(nodeKey, rebirthReasonSequenceGap)
	}
	if unresolvedAliases > 0 {
		s.sendRebirthRequest(nodeKey, rebirthReasonUnresolvedAliases)
	}
}

func (s *sparkplugInput) processDeathMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// ENG-4031: Use node-level key for state tracking consistency
	nodeKey := topicInfo.NodeKey()

	state, exists := s.nodeStates[nodeKey]
	if !exists {
		// No state for this node - create minimal state
		s.nodeStates[nodeKey] = &nodeState{
			IsOnline: false,
			LastSeen: time.Now(),
		}
		s.logger.Debugf("Processed %s for unknown device %s (node: %s, created state)", msgType, topicInfo.DeviceKey(), nodeKey)
		return
	}

	// For NDEATH messages, validate bdSeq from payload
	if msgType == MessageTypeNDEATH {
		// Extract bdSeq from payload metrics
		var payloadBdSeq uint64
		var foundBdSeq bool
		for _, metric := range payload.Metrics {
			if metric.Name != nil && *metric.Name == "bdSeq" {
				payloadBdSeq = metric.GetLongValue()
				foundBdSeq = true
				break
			}
		}

		if foundBdSeq {
			// Validate bdSeq matches the stored value from NBIRTH
			if payloadBdSeq != state.BdSeq {
				// Stale NDEATH from old session - ignore it
				s.logger.Warnf("Ignoring stale NDEATH for node %s: bdSeq mismatch (payload=%d, stored=%d)",
					nodeKey, payloadBdSeq, state.BdSeq)
				return
			}
			s.logger.Debugf("NDEATH bdSeq validated for node %s (bdSeq=%d)", nodeKey, payloadBdSeq)
		} else {
			// No bdSeq in payload - log warning but still process
			// Some older edge nodes might not include bdSeq
			s.logger.Warnf("NDEATH for node %s missing bdSeq metric - processing anyway", nodeKey)
		}
	}

	// Update state
	state.IsOnline = false
	state.LastSeen = time.Now()

	s.logger.Debugf("Processed %s for device %s (node: %s)", msgType, topicInfo.DeviceKey(), nodeKey)
}

func (s *sparkplugInput) processCommandMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	s.logger.Debugf("⚡ processCommandMessage: processing %s for device %s with %d metrics", msgType, topicInfo.DeviceKey(), len(payload.Metrics))

	// ENG-4031: Use node-level key for state tracking consistency
	nodeKey := topicInfo.NodeKey()

	// Update node state timestamp for activity tracking
	s.stateMu.Lock()
	if state, exists := s.nodeStates[nodeKey]; exists {
		state.LastSeen = time.Now()
	} else {
		s.nodeStates[nodeKey] = &nodeState{
			LastSeen: time.Now(),
			IsOnline: true, // Assume online if receiving commands
		}
	}
	s.stateMu.Unlock()

	// Check for rebirth command
	for _, metric := range payload.Metrics {
		if metric.Name != nil && *metric.Name == "Node Control/Rebirth" {
			if metric.GetBooleanValue() {
				s.logger.Infof("🔄 Rebirth request received for device %s (node: %s)", topicInfo.DeviceKey(), nodeKey)
				// Handle rebirth logic here if needed for edge nodes
				// For primary hosts, this is typically just logged
			}
		}
	}

	// Resolve aliases for side effects (cache lookup + per-metric failure counter).
	// Return is discarded: CMD messages don't trigger rebirth. The spec lists DATA
	// without BIRTH as the rebirth condition, not CMD.
	_ = s.resolveAliases(topicInfo.DeviceKey(), payload.Metrics)

	// Create batch from command metrics - always split for UMH-Core format
	batch := s.createSplitMessages(payload, msgType, topicInfo, originalTopic)

	s.logger.Debugf("✅ processCommandMessage: created batch with %d messages for %s", len(batch), msgType)
	return batch
}

func (s *sparkplugInput) processStateMessage(msgType MessageType, topicInfo *TopicInfo, originalTopic string, statePayload string) (service.MessageBatch, error) {
	// ENG-4031: Use node-level key for state tracking consistency
	nodeKey := topicInfo.NodeKey()
	s.logger.Debugf("🏛️ processStateMessage: processing STATE message for node %s, state: %s", nodeKey, statePayload)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Update node state based on STATE message content
	isOnline := statePayload == "ONLINE"
	if state, exists := s.nodeStates[nodeKey]; exists {
		state.IsOnline = isOnline
		state.LastSeen = time.Now()
	} else {
		s.nodeStates[nodeKey] = &nodeState{
			IsOnline: isOnline,
			LastSeen: time.Now(),
		}
	}

	// Create a status event message for STATE changes
	event := map[string]interface{}{
		"event":        "StateChange",
		"node_key":     nodeKey,
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

	// Set Sparkplug B standard metadata for state messages
	msg.MetaSet("spb_node_key", nodeKey)
	msg.MetaSet("spb_message_type", msgType.String())
	msg.MetaSet("spb_topic", originalTopic)
	msg.MetaSet("spb_group_id", topicInfo.Group)
	msg.MetaSet("spb_edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id", topicInfo.Device)
	}

	// Add pre-sanitized versions for state messages
	msg.MetaSet("spb_group_id_sanitized", s.sanitizeForTopic(topicInfo.Group))
	msg.MetaSet("spb_edge_node_id_sanitized", s.sanitizeForTopic(topicInfo.EdgeNode))
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id_sanitized", s.sanitizeForTopic(topicInfo.Device))
	}
	msg.MetaSet("spb_node_key_sanitized", s.sanitizeForTopic(nodeKey))
	msg.MetaSet("event_type", "state_change")
	msg.MetaSet("spb_state", statePayload)

	s.logger.Debugf("✅ processStateMessage: created STATE event message for node %s: %s", nodeKey, statePayload)

	return service.MessageBatch{msg}, nil
}

func (s *sparkplugInput) Close(_ context.Context) error {
	// Signal shutdown to all goroutines first
	close(s.done)

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	if s.client != nil && s.client.IsConnected() {
		// Publish STATE OFFLINE before disconnecting (for primary host role)
		if s.config.Role == RolePrimaryHost {
			stateTopic := s.config.GetStateTopic()
			token := s.client.Publish(stateTopic, s.config.MQTT.QoS, false, "OFFLINE")
			token.WaitTimeout(5 * time.Second)
		}

		s.client.Disconnect(1000)
	}

	// Do not close s.messages; s.done gates producers and ReadBatch.
	// Closing the channel could cause a panic if messageHandler attempts to send
	// after the done check but before the channel send completes.

	s.logger.Info("Sparkplug input closed")
	return nil
}

// Helper methods that delegate to the existing processor logic where possible
func (s *sparkplugInput) cacheAliases(deviceKey string, metrics []*sparkplugb.Payload_Metric) {
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
//
// Returns the number of metrics that carried a real alias but could not be resolved. A non-zero
// return signals that BIRTH context is missing for this device; DATA callers use this to trigger
// a recovery rebirth.
func (s *sparkplugInput) resolveAliases(deviceKey string, metrics []*sparkplugb.Payload_Metric) int {
	s.logger.Debugf("🔍 resolveAliases: starting to resolve aliases for deviceKey=%s, %d metrics", deviceKey, len(metrics))

	count := s.aliasCache.ResolveAliases(deviceKey, metrics)

	// Scan metrics once, applying the same "what counts as a real alias" filter as
	// AliasCache (see sparkplug_b_core.go): alias must be non-nil AND non-zero, since
	// alias 0 is a Sparkplug-reserved sentinel that never appears in the cache. A
	// metric is "resolved" iff its Name is set and non-empty after the cache lookup.
	var unresolved int
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		if metric.Alias == nil || *metric.Alias == 0 {
			continue
		}
		if metric.Name != nil && *metric.Name != "" {
			s.logger.Debugf("   🎯 resolved alias %d -> '%s'", *metric.Alias, *metric.Name)
			continue
		}
		s.logger.Debugf("   ❌ FAILED to resolve alias %d (no name found)", *metric.Alias)
		s.aliasResolutionFailures.Incr(1)
		unresolved++
	}

	if count > 0 {
		s.aliasResolutions.Incr(int64(count))
		s.logger.Debugf("✅ resolveAliases: resolved %d aliases for device %s", count, deviceKey)
	}
	if unresolved > 0 {
		s.logger.Debugf("⚠️ resolveAliases: %d alias(es) unresolved for device %s — BIRTH context missing", unresolved, deviceKey)
	}

	return unresolved
}

func (s *sparkplugInput) parseSparkplugTopicDetailed(topic string) (MessageType, *TopicInfo) {
	// Use core component instead of processor
	return s.topicParser.ParseSparkplugTopicDetailed(topic)
}

// Message creation methods
func (s *sparkplugInput) createSplitMessages(payload *sparkplugb.Payload, msgType MessageType, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	var batch service.MessageBatch

	for i, metric := range payload.Metrics {
		if metric == nil {
			continue
		}

		msg := s.createMessageFromMetric(metric, payload, msgType, topicInfo, originalTopic, i, len(payload.Metrics))
		if msg != nil {
			batch = append(batch, msg)
		}
	}

	return batch
}

func (s *sparkplugInput) createMessageFromMetric(metric *sparkplugb.Payload_Metric, payload *sparkplugb.Payload, msgType MessageType, topicInfo *TopicInfo, originalTopic string, metricIndex int, totalMetrics int) *service.Message {
	// Extract metric value as JSON (always preserve Sparkplug B format)
	value := s.extractMetricValue(metric)
	if value == nil {
		return nil
	}

	msg := service.NewMessage(value)

	// Set Sparkplug B standard metadata (always available)
	msg.MetaSet("spb_group_id", topicInfo.Group)
	msg.MetaSet("spb_edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id", topicInfo.Device)
	}
	msg.MetaSet("spb_message_type", msgType.String())
	msg.MetaSet("spb_device_key", topicInfo.DeviceKey())
	msg.MetaSet("spb_topic", originalTopic)

	// Add pre-sanitized versions for easier processing
	msg.MetaSet("spb_group_id_sanitized", s.sanitizeForTopic(topicInfo.Group))
	msg.MetaSet("spb_edge_node_id_sanitized", s.sanitizeForTopic(topicInfo.EdgeNode))
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id_sanitized", s.sanitizeForTopic(topicInfo.Device))
	}
	msg.MetaSet("spb_device_key_sanitized", s.sanitizeForTopic(topicInfo.DeviceKey()))

	// Set Sparkplug B metric name
	metricName := "unknown_metric"
	if metric.Name != nil && *metric.Name != "" {
		metricName = *metric.Name
	} else if metric.Alias != nil {
		metricName = fmt.Sprintf("alias_%d", *metric.Alias)
	}
	msg.MetaSet("spb_metric_name", metricName)
	msg.MetaSet("spb_metric_name_sanitized", s.sanitizeForTopic(metricName))

	// Set sequence and timing metadata
	// Note: spb_sequence is the MQTT-level sequence number (shared by all metrics in this NDATA message)
	// See ENG-3720 and CS-13 for context on why we add metric_index for unique identification
	seq := GetSequenceNumber(payload)
	msg.MetaSet("spb_sequence", fmt.Sprintf("%d", seq))

	// Add Dual-Sequence metadata for split message identification (Fix for ENG-3720)
	// When NDATA messages are split into individual metrics, all metrics share the same spb_sequence.
	// These fields enable unique identification: composite key = (spb_sequence, spb_metric_index)
	msg.MetaSet("spb_metric_index", fmt.Sprintf("%d", metricIndex))
	msg.MetaSet("spb_metrics_in_payload", fmt.Sprintf("%d", totalMetrics))

	if payload.Timestamp != nil {
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
	// ENG-4031: Use nodeKey for state lookup - nodeStates is keyed by node, not device
	s.stateMu.RLock()
	if state, exists := s.nodeStates[topicInfo.NodeKey()]; exists {
		msg.MetaSet("spb_bdseq", fmt.Sprintf("%d", state.BdSeq))
	}
	s.stateMu.RUnlock()

	// Try to add UMH conversion metadata (optional, non-failing)
	s.tryAddUMHMetadata(msg, metric, payload, topicInfo)

	// Decode proto2 extension fields (opt-in). Additive only: sets new metadata, never the
	// value or existing keys, and never fails the metric.
	if s.extDecoder != nil {
		if flat, decoded, present, err := s.extDecoder.decode(metric); err != nil {
			s.noteExtDecodeError(metric.GetName(), err)
		} else if present {
			for leaf, val := range flat {
				msg.MetaSet("spb_ext_"+leaf, val)
			}
			msg.MetaSet("spb_metric_decoded", decoded)
		}
	}

	return msg
}

// noteExtDecodeError counts the failure and warns at most once per 30s, so a systematic
// failure (e.g. a mis-declared field type) can't flood the logs.
func (s *sparkplugInput) noteExtDecodeError(metricName string, err error) {
	s.extensionDecodeErrors.Incr(1)
	s.extDecodeLogMu.Lock()
	now := time.Now()
	if !s.extDecodeLogLast.IsZero() && now.Sub(s.extDecodeLogLast) < 30*time.Second {
		s.extDecodeLogMu.Unlock()
		return
	}
	s.extDecodeLogLast = now
	s.extDecodeLogMu.Unlock()
	s.logger.Warnf("extension decode failed for metric %q: %v (metric emitted without extension fields; further warnings throttled)", metricName, err)
}

func (s *sparkplugInput) createDeathEventMessage(msgType MessageType, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	event := map[string]interface{}{
		"event":        "DeviceOffline",
		"device_key":   topicInfo.DeviceKey(),
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

	// Set Sparkplug B standard metadata for death events
	msg.MetaSet("spb_message_type", msgType.String())
	msg.MetaSet("spb_device_key", topicInfo.DeviceKey())
	msg.MetaSet("spb_topic", originalTopic)
	msg.MetaSet("spb_group_id", topicInfo.Group)
	msg.MetaSet("spb_edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id", topicInfo.Device)
	}

	// Add pre-sanitized versions for death events
	msg.MetaSet("spb_group_id_sanitized", s.sanitizeForTopic(topicInfo.Group))
	msg.MetaSet("spb_edge_node_id_sanitized", s.sanitizeForTopic(topicInfo.EdgeNode))
	if topicInfo.Device != "" {
		msg.MetaSet("spb_device_id_sanitized", s.sanitizeForTopic(topicInfo.Device))
	}
	msg.MetaSet("spb_device_key_sanitized", s.sanitizeForTopic(topicInfo.DeviceKey()))
	msg.MetaSet("event_type", "device_offline")

	return service.MessageBatch{msg}
}

func (s *sparkplugInput) extractMetricValue(metric *sparkplugb.Payload_Metric) []byte {
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
		case *sparkplugb.Payload_Metric_IntValue:
			result["value"] = decodeIntValue(metric.Datatype, v.IntValue)
		case *sparkplugb.Payload_Metric_LongValue:
			result["value"] = decodeLongValue(metric.Datatype, v.LongValue)
		case *sparkplugb.Payload_Metric_FloatValue:
			result["value"] = v.FloatValue
		case *sparkplugb.Payload_Metric_DoubleValue:
			result["value"] = v.DoubleValue
		case *sparkplugb.Payload_Metric_BooleanValue:
			result["value"] = v.BooleanValue
		case *sparkplugb.Payload_Metric_StringValue:
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

// sanitizeForTopic sanitizes strings for use in UMH topic paths: every rune that is not an
// ASCII letter, digit, or dot becomes '_'. Dots are kept because they separate topic segments.
func (s *sparkplugInput) sanitizeForTopic(input string) string {
	return sanitizeRunes(input, func(r rune) bool { return isASCIIAlnum(r) || r == '.' })
}

// isASCIIAlnum reports whether r is an ASCII letter or digit.
func isASCIIAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// sanitizeRunes replaces every rune for which keep returns false with '_'.
func sanitizeRunes(s string, keep func(rune) bool) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if keep(r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// getDataTypeName converts Sparkplug data type ID to human-readable string
func (s *sparkplugInput) getDataTypeName(datatype uint32) string {
	switch datatype {
	case SparkplugDataTypeInt8:
		return "Int8"
	case SparkplugDataTypeInt16:
		return "Int16"
	case SparkplugDataTypeInt32:
		return "Int32"
	case SparkplugDataTypeInt64:
		return "Int64"
	case SparkplugDataTypeUInt8:
		return "UInt8"
	case SparkplugDataTypeUInt16:
		return "UInt16"
	case SparkplugDataTypeUInt32:
		return "UInt32"
	case SparkplugDataTypeUInt64:
		return "UInt64"
	case SparkplugDataTypeFloat:
		return "Float"
	case SparkplugDataTypeDouble:
		return "Double"
	case SparkplugDataTypeBoolean:
		return "Boolean"
	case SparkplugDataTypeString:
		return "String"
	case SparkplugDataTypeDateTime:
		return "DateTime"
	case SparkplugDataTypeText:
		return "Text"
	default:
		return "Unknown"
	}
}

// stampOrThrottle atomically checks the throttle map for `key` and, if no
// recent entry is found, stamps the current time. Returns throttled=true with
// the elapsed time since the previous stamp when suppression is required.
//
// Holds birthRequestMu only for the read-write window; callers must not hold
// any sparkplugInput lock when invoking it.
func (s *sparkplugInput) stampOrThrottle(key string) (bool, time.Duration) {
	s.birthRequestMu.Lock()
	defer s.birthRequestMu.Unlock()
	if last, exists := s.birthRequested[key]; exists {
		elapsed := time.Since(last)
		if elapsed < s.config.BirthRequestThrottle {
			return true, elapsed
		}
	}
	s.birthRequested[key] = time.Now()
	return false, 0
}

// sendRebirthRequest publishes an NCMD/DCMD Rebirth for the given key. The reason
// drives which feature flags apply and which metric counter ticks.
//
// Gates applied in order: role (RoleSecondaryPassive never publishes), feature
// flag (RequestBirthOnConnect applies only to the discovery reason), shared
// throttle (birthRequested keyed by `key`, default 1s window).
//
// Counter semantics: reason-specific counters (discoveryRebirths,
// sequenceGapRebirths, aliasRebirths) tick after the throttle gate passes,
// regardless of whether MQTT publish succeeds; they measure "policy decided
// to send", not "broker received". rebirthsRequested ticks only on successful
// publish. rebirthsSuppressed ticks on every gate that returns early.
//
// `key` must be a node-level key (group/node) for the spec-mandated
// NCMD/Rebirth path. A 3-part device key (group/node/device) is also accepted
// for DCMD/Rebirth, though current callers in processDataMessage always pass
// nodeKey.
func (s *sparkplugInput) sendRebirthRequest(key string, reason rebirthReason) {
	if s.config.Role == RoleSecondaryPassive {
		s.logger.Debugf("Rebirth suppressed for %s (secondary_passive role, reason: %s)", key, reason)
		s.rebirthsSuppressed.Incr(1)
		return
	}

	if reason == rebirthReasonDiscovery && !s.config.RequestBirthOnConnect {
		s.logger.Debugf("Discovery rebirth suppressed for %s (request_birth_on_connect=false)", key)
		s.rebirthsSuppressed.Incr(1)
		return
	}

	throttled, elapsed := s.stampOrThrottle(key)
	if throttled {
		s.rebirthsSuppressed.Incr(1)
		if elapsed < sameDispatchThreshold {
			s.logger.Debugf("Suppressed %s rebirth for %s (same-dispatch co-fire, %v after stamp)",
				reason, key, elapsed)
		} else {
			s.logger.Infof("Suppressed %s rebirth for %s (throttled, last request %v ago)",
				reason, key, elapsed)
		}
		return
	}

	switch reason {
	case rebirthReasonDiscovery:
		s.discoveryRebirths.Incr(1)
	case rebirthReasonSequenceGap:
		s.sequenceGapRebirths.Incr(1)
	case rebirthReasonUnresolvedAliases:
		s.aliasRebirths.Incr(1)
	}

	s.logger.Infof("Sending %s rebirth for %s", reason, key)

	if s.client == nil || !s.client.IsConnected() {
		return
	}

	parts := strings.Split(key, "/")

	if len(parts) < 2 || len(parts) > 3 {
		s.logger.Warnf("Invalid rebirth key format: expected 2-3 parts (group/node or group/node/device), got %d: %s",
			len(parts), key)
		return
	}

	for i, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			s.logger.Warnf("Invalid rebirth key: empty or whitespace-only part at index %d: %s - rebirth command will not be sent",
				i, key)
			return
		}
		if trimmed != part {
			s.logger.Warnf("Invalid rebirth key: leading/trailing whitespace in part %d: %s - rebirth command will not be sent",
				i, key)
			return
		}
		if strings.Contains(part, " ") {
			s.logger.Warnf("Invalid rebirth key: embedded whitespace in part %d: %s - rebirth command will not be sent",
				i, key)
			return
		}
	}

	var topic string
	var controlMetricName string
	if len(parts) == 2 {
		topic = fmt.Sprintf("spBv1.0/%s/NCMD/%s", parts[0], parts[1])
		controlMetricName = "Node Control/Rebirth"
	} else { // len(parts) == 3
		topic = fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", parts[0], parts[1], parts[2])
		controlMetricName = "Device Control/Rebirth"
	}

	rebirthMetric := &sparkplugb.Payload_Metric{
		Name: func() *string { s := controlMetricName; return &s }(),
		Value: &sparkplugb.Payload_Metric_BooleanValue{
			BooleanValue: true,
		},
		Datatype: func() *uint32 { d := SparkplugDataTypeBoolean; return &d }(),
	}

	cmdPayload := &sparkplugb.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Metrics:   []*sparkplugb.Payload_Metric{rebirthMetric},
	}

	payloadBytes, err := proto.Marshal(cmdPayload)
	if err != nil {
		s.logger.Errorf("Failed to marshal rebirth command: %v", err)
		return
	}

	token := s.client.Publish(topic, s.config.MQTT.QoS, false, payloadBytes)
	// Match Close()'s pattern (line ~964): bound the publish so a TCP-black-holed
	// broker can't stall the input pipeline. processDataMessage runs synchronously
	// from ReadBatch; an unbounded Wait() here means a frozen broker freezes reads.
	if !token.WaitTimeout(5 * time.Second) {
		s.logger.Warnf("Timed out publishing %s rebirth for %s after 5s", reason, key)
		return
	}
	if token.Error() != nil {
		s.logger.Errorf("Failed to publish rebirth command: %v", token.Error())
		return
	}

	s.rebirthsRequested.Incr(1)
	s.logger.Infof("Sent rebirth request to %s on topic %s", key, topic)
}

// GetSequenceNumber extracts sequence number from payload, treating nil as 0 (implied)
// According to Sparkplug B spec updates, seq=0 can be omitted in BIRTH messages for backwards compatibility
// Exported for testing purposes to ensure backwards compatibility with older devices
func GetSequenceNumber(payload *sparkplugb.Payload) uint8 {
	if payload.Seq == nil {
		return 0 // Implied seq=0 for backwards compatibility
	}
	return uint8(*payload.Seq)
}

// ValidateSequenceNumber checks if a received sequence number is valid according to Sparkplug B spec
// Exported for testing purposes to ensure sequence validation logic is properly tested
//
// According to the Sparkplug B specification (https://github.com/eclipse-sparkplug/sparkplug/blob/master/specification/src/main/asciidoc/chapters/Sparkplug_5_Operational_Behavior.adoc):
// - Sequence numbers must arrive in sequential order (0, 1, 2, ... 255, 0, 1, ...)
// - ANY gap in sequence numbers should trigger a rebirth request after a configurable timeout
// - This function only validates strict sequential order; timeout-based reordering is handled elsewhere
func ValidateSequenceNumber(lastSeq uint8, currentSeq uint8) bool {
	// Calculate expected next sequence with wraparound (0-255)
	expectedNext := uint8((int(lastSeq) + 1) % 256)

	// Only accept the exact next sequence number or valid wraparound
	return currentSeq == expectedNext
}

func composeLocationDeviceID(edgeNode string, device string, includeEdgeNode bool) string {
	if device == "" {
		return edgeNode
	}
	if includeEdgeNode {
		return edgeNode + ":" + device
	}
	return device
}

// tryAddUMHMetadata attempts to convert Sparkplug B data to UMH format and add UMH metadata.
// This is a non-failing operation - if conversion fails, it adds status flags and continues.
func (s *sparkplugInput) tryAddUMHMetadata(msg *service.Message, metric *sparkplugb.Payload_Metric, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	// Get message type from metadata (already set by createMessageFromMetric)
	msgType, _ := msg.MetaGet("spb_message_type")

	// Only attempt conversion if we have necessary data
	// For NDATA messages: device ID is optional (node-level data), use edge_node_id as device identifier
	// For DDATA messages: device ID is required (device-level data)
	if metric == nil {
		msg.MetaSet("umh_conversion_status", "skipped_insufficient_data")
		s.logger.Debugf("Skipping UMH conversion: metric is nil")
		return
	}

	// For DDATA messages, device ID is required
	if msgType == "DDATA" && topicInfo.Device == "" {
		msg.MetaSet("umh_conversion_status", "skipped_insufficient_data")
		s.logger.Debugf("Skipping UMH conversion for DDATA: device ID required but missing")
		return
	}

	// For NDATA messages, device ID is optional - we'll use edge_node_id as device identifier

	// Try to use the format converter
	converter := NewFormatConverter()

	// Extract raw value for conversion
	rawValue := s.extractMetricValueRaw(metric)
	if rawValue == nil {
		msg.MetaSet("umh_conversion_status", "failed_no_value")
		s.logger.Debugf("UMH conversion failed: no extractable value from metric")
		return
	}

	// Create SparkplugMessage struct for conversion
	var dataType string
	if metric.Datatype != nil {
		dataType = s.convertSparkplugDataTypeToString(*metric.Datatype)
	} else {
		dataType = "unknown"
	}

	// For NDATA messages (node-level data), use EdgeNode as DeviceID when Device is empty
	if topicInfo.Device == "" {
		// Set spb_device_id metadata for consistency (used by Topic Browser and other downstream processors)
		// Even though this is NDATA (node-level), we're treating EdgeNode as the device identifier
		msg.MetaSet("spb_device_id", topicInfo.EdgeNode)
		msg.MetaSet("spb_device_id_sanitized", s.sanitizeForTopic(topicInfo.EdgeNode))
	}

	locationDeviceID := composeLocationDeviceID(topicInfo.EdgeNode, topicInfo.Device, s.config.IncludeEdgeNodeInLocation)

	sparkplugMsg := &SparkplugMessage{
		GroupID:    topicInfo.Group,
		EdgeNodeID: topicInfo.EdgeNode,
		DeviceID:   locationDeviceID,
		Value:      rawValue,
		DataType:   dataType,
		Timestamp:  time.Now(), // Will be overridden below if payload has timestamp
	}

	// Set metric name from name or alias
	if metric.Name != nil && *metric.Name != "" {
		sparkplugMsg.MetricName = *metric.Name
	} else if metric.Alias != nil {
		sparkplugMsg.MetricName = fmt.Sprintf("alias_%d", *metric.Alias)
	} else {
		sparkplugMsg.MetricName = "unknown_metric"
	}

	// Set timestamp from payload if available
	if payload.Timestamp != nil {
		sparkplugMsg.Timestamp = time.UnixMilli(int64(*payload.Timestamp))
	}

	// Store original values before any sanitization
	originalMetricName := sparkplugMsg.MetricName
	originalDeviceID := sparkplugMsg.DeviceID

	// Try UMH conversion - the converter will handle any necessary sanitization
	umhMsg, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_raw")
	if err != nil {
		msg.MetaSet("umh_conversion_status", "failed")
		msg.MetaSet("umh_conversion_error", err.Error())
		s.logger.Debugf("UMH conversion failed for metric %s: %v", sparkplugMsg.MetricName, err)

		// Provide original values as fallback metadata
		msg.MetaSet("spb_device_id", originalDeviceID)
		msg.MetaSet("spb_metric_name", originalMetricName)
		return
	}

	// Conversion successful - add UMH metadata
	msg.MetaSet("umh_conversion_status", "success")

	// Build location path without trailing dots when LocationSublevels is empty
	locationPath := umhMsg.TopicInfo.Level0
	if len(umhMsg.TopicInfo.LocationSublevels) > 0 {
		locationPath = locationPath + "." + strings.Join(umhMsg.TopicInfo.LocationSublevels, ".")
	}

	// The converter has already sanitized all fields, so we can use them directly
	msg.MetaSet("umh_location_path", locationPath)
	msg.MetaSet("umh_tag_name", umhMsg.TopicInfo.Name)
	msg.MetaSet("umh_data_contract", umhMsg.TopicInfo.DataContract)

	// Add virtual path if present
	// Note: Benthos metadata cannot store empty strings (they become unset)
	// YAML configs must use .or("") to handle missing virtual_path metadata
	if umhMsg.TopicInfo.VirtualPath != nil {
		msg.MetaSet("umh_virtual_path", *umhMsg.TopicInfo.VirtualPath)
	}

	// Add debug metadata for traceability
	if originalMetricName != sparkplugMsg.MetricName {
		msg.MetaSet("spb_original_metric_name", originalMetricName)
	}
	if originalDeviceID != "" && originalDeviceID != sparkplugMsg.DeviceID {
		msg.MetaSet("spb_original_device_id", originalDeviceID)
	}

	s.logger.Debugf("Successfully added UMH metadata for metric %s -> %s", sparkplugMsg.MetricName, umhMsg.Topic.String())
}

// extractMetricValueRaw extracts the raw value from a Sparkplug metric without JSON wrapping
func (s *sparkplugInput) extractMetricValueRaw(metric *sparkplugb.Payload_Metric) interface{} {
	// Check for null value
	if metric.IsNull != nil && *metric.IsNull {
		return nil
	}

	// Extract value based on type
	if value := metric.GetValue(); value != nil {
		switch v := value.(type) {
		case *sparkplugb.Payload_Metric_IntValue:
			return decodeIntValue(metric.Datatype, v.IntValue)
		case *sparkplugb.Payload_Metric_LongValue:
			return decodeLongValue(metric.Datatype, v.LongValue)
		case *sparkplugb.Payload_Metric_FloatValue:
			return v.FloatValue
		case *sparkplugb.Payload_Metric_DoubleValue:
			return v.DoubleValue
		case *sparkplugb.Payload_Metric_BooleanValue:
			return v.BooleanValue
		case *sparkplugb.Payload_Metric_StringValue:
			return v.StringValue
		default:
			return nil
		}
	}

	return nil
}

// decodeIntValue reinterprets the unsigned int_value wire field according to the
// metric datatype. Sparkplug B packs signed integers (Int8/Int16/Int32) as
// two's-complement into int_value (ENG-5126); without a signed datatype the raw
// uint32 is returned unchanged (UInt8/UInt16/UInt32, or datatype unknown).
func decodeIntValue(datatype *uint32, raw uint32) any {
	if datatype == nil {
		return raw
	}
	switch *datatype {
	case SparkplugDataTypeInt8:
		return int8(uint8(raw))
	case SparkplugDataTypeInt16:
		return int16(uint16(raw))
	case SparkplugDataTypeInt32:
		return int32(raw)
	default:
		return raw
	}
}

// decodeLongValue reinterprets the unsigned long_value wire field according to
// the metric datatype: Int64 is two's-complement (ENG-5126); UInt64 and
// DateTime pass through unchanged.
func decodeLongValue(datatype *uint32, raw uint64) any {
	if datatype != nil && *datatype == SparkplugDataTypeInt64 {
		return int64(raw)
	}
	return raw
}

// convertSparkplugDataTypeToString converts Sparkplug data type ID to format converter expected string
func (s *sparkplugInput) convertSparkplugDataTypeToString(datatype uint32) string {
	switch datatype {
	case SparkplugDataTypeInt8:
		return "int8"
	case SparkplugDataTypeInt16:
		return "int16"
	case SparkplugDataTypeInt32:
		return "int32"
	case SparkplugDataTypeInt64:
		return "int64"
	case SparkplugDataTypeUInt8:
		return "uint8"
	case SparkplugDataTypeUInt16:
		return "uint16"
	case SparkplugDataTypeUInt32:
		return "uint32"
	case SparkplugDataTypeUInt64:
		return "uint64"
	case SparkplugDataTypeFloat:
		return "float"
	case SparkplugDataTypeDouble:
		return "double"
	case SparkplugDataTypeBoolean:
		return "boolean"
	case SparkplugDataTypeString:
		return "string"
	case SparkplugDataTypeDateTime:
		return "string" // Treat DateTime as string for now
	case SparkplugDataTypeText:
		return "string" // Treat Text as string
	default:
		return "string" // Default to string for unknown types
	}
}
