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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
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
		Field(service.NewStringListField("broker_urls").
			Description("List of MQTT broker URLs to connect to").
			Example([]string{"tcp://localhost:1883", "ssl://broker.hivemq.com:8883"}).
			Default([]string{"tcp://localhost:1883"})).
		Field(service.NewStringField("client_id").
			Description("MQTT client ID for this edge node").
			Default("benthos-sparkplug-node")).
		Field(service.NewStringField("username").
			Description("MQTT username for authentication").
			Default("").
			Optional()).
		Field(service.NewStringField("password").
			Description("MQTT password for authentication").
			Default("").
			Secret().
			Optional()).
		Field(service.NewStringField("group_id").
			Description("Sparkplug Group ID (e.g., 'FactoryA')").
			Example("FactoryA")).
		Field(service.NewStringField("edge_node_id").
			Description("Edge Node ID within the group (e.g., 'Line3')").
			Example("Line3")).
		Field(service.NewStringField("device_id").
			Description("Device ID under the edge node (optional, if not specified publishes as node-level)").
			Default("").
			Optional()).
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
			Description("Metric definitions for BIRTH messages and alias mapping")).
		Field(service.NewIntField("qos").
			Description("QoS level for MQTT publishing (0, 1, or 2)").
			Default(1)).
		Field(service.NewDurationField("keep_alive").
			Description("MQTT keep alive interval").
			Default("30s")).
		Field(service.NewDurationField("connect_timeout").
			Description("MQTT connection timeout").
			Default("10s")).
		Field(service.NewBoolField("clean_session").
			Description("MQTT clean session flag").
			Default(true)).
		Field(service.NewBoolField("auto_extract_tag_name").
			Description("Whether to automatically extract tag_name from message metadata").
			Default(true)).
		Field(service.NewBoolField("retain_last_values").
			Description("Whether to retain last known values for BIRTH messages after reconnection").
			Default(true))

	err := service.RegisterOutput(
		"sparkplug_output",
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
	brokerURLs         []string
	clientID           string
	username           string
	password           string
	groupID            string
	edgeNodeID         string
	deviceID           string
	metrics            []MetricConfig
	qos                byte
	keepAlive          time.Duration
	connectTimeout     time.Duration
	cleanSession       bool
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
	lastValues    map[string]interface{} // metric name -> last value
	stateMu       sync.RWMutex

	// Metrics
	messagesPublished *service.MetricCounter
	birthsPublished   *service.MetricCounter
	deathsPublished   *service.MetricCounter
	sequenceWraps     *service.MetricCounter
	publishErrors     *service.MetricCounter
}

func newSparkplugOutput(conf *service.ParsedConfig, mgr *service.Resources) (*sparkplugOutput, error) {
	brokerURLs, err := conf.FieldStringList("broker_urls")
	if err != nil {
		return nil, err
	}

	clientID, err := conf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	username, _ := conf.FieldString("username")
	password, _ := conf.FieldString("password")

	groupID, err := conf.FieldString("group_id")
	if err != nil {
		return nil, err
	}

	edgeNodeID, err := conf.FieldString("edge_node_id")
	if err != nil {
		return nil, err
	}

	deviceID, _ := conf.FieldString("device_id")

	qosInt, err := conf.FieldInt("qos")
	if err != nil {
		return nil, err
	}
	qos := byte(qosInt)

	keepAlive, err := conf.FieldDuration("keep_alive")
	if err != nil {
		return nil, err
	}

	connectTimeout, err := conf.FieldDuration("connect_timeout")
	if err != nil {
		return nil, err
	}

	cleanSession, err := conf.FieldBool("clean_session")
	if err != nil {
		return nil, err
	}

	autoExtractTagName, err := conf.FieldBool("auto_extract_tag_name")
	if err != nil {
		return nil, err
	}

	retainLastValues, err := conf.FieldBool("retain_last_values")
	if err != nil {
		return nil, err
	}

	// Parse metric configurations
	metricObjs, err := conf.FieldObjectList("metrics")
	if err != nil {
		return nil, err
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

	// Generate random bdSeq
	bdSeqBig, err := rand.Int(rand.Reader, big.NewInt(65536))
	if err != nil {
		return nil, fmt.Errorf("failed to generate bdSeq: %w", err)
	}
	bdSeq := bdSeqBig.Uint64()

	return &sparkplugOutput{
		brokerURLs:         brokerURLs,
		clientID:           clientID,
		username:           username,
		password:           password,
		groupID:            groupID,
		edgeNodeID:         edgeNodeID,
		deviceID:           deviceID,
		metrics:            metrics,
		qos:                qos,
		keepAlive:          keepAlive,
		connectTimeout:     connectTimeout,
		cleanSession:       cleanSession,
		autoExtractTagName: autoExtractTagName,
		retainLastValues:   retainLastValues,
		logger:             mgr.Logger(),
		bdSeq:              bdSeq,
		seqCounter:         0,
		metricAliases:      metricAliases,
		metricTypes:        metricTypes,
		lastValues:         make(map[string]interface{}),
		messagesPublished:  mgr.Metrics().NewCounter("messages_published"),
		birthsPublished:    mgr.Metrics().NewCounter("births_published"),
		deathsPublished:    mgr.Metrics().NewCounter("deaths_published"),
		sequenceWraps:      mgr.Metrics().NewCounter("sequence_wraps"),
		publishErrors:      mgr.Metrics().NewCounter("publish_errors"),
	}, nil
}

func (s *sparkplugOutput) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions()

	// Add broker URLs
	for _, url := range s.brokerURLs {
		opts.AddBroker(url)
	}

	opts.SetClientID(s.clientID)
	opts.SetKeepAlive(s.keepAlive)
	opts.SetCleanSession(s.cleanSession)
	opts.SetConnectTimeout(s.connectTimeout)

	if s.username != "" {
		opts.SetUsername(s.username)
		if s.password != "" {
			opts.SetPassword(s.password)
		}
	}

	// Set up DEATH Last Will Testament
	deathTopic, deathPayload := s.createDeathMessage()
	opts.SetWill(deathTopic, string(deathPayload), s.qos, false)

	// Set connection handlers
	opts.SetOnConnectHandler(s.onConnect)
	opts.SetConnectionLostHandler(s.onConnectionLost)

	s.client = mqtt.NewClient(opts)

	s.logger.Infof("Connecting to Sparkplug MQTT brokers: %v", s.brokerURLs)
	token := s.client.Connect()
	if !token.WaitTimeout(s.connectTimeout) {
		return fmt.Errorf("connection timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", err)
	}

	s.logger.Info("Successfully connected to Sparkplug MQTT broker")
	return nil
}

func (s *sparkplugOutput) onConnect(client mqtt.Client) {
	s.logger.Info("MQTT client connected, publishing BIRTH message")

	// Reset sequence counter on new connection
	s.stateMu.Lock()
	s.seqCounter = 0
	s.stateMu.Unlock()

	// Publish BIRTH message
	if err := s.publishBirthMessage(); err != nil {
		s.logger.Errorf("Failed to publish BIRTH message: %v", err)
		s.publishErrors.Incr(1)
	} else {
		s.birthsPublished.Incr(1)
		s.logger.Info("Successfully published BIRTH message")
	}
}

func (s *sparkplugOutput) onConnectionLost(client mqtt.Client, err error) {
	s.logger.Errorf("MQTT connection lost: %v", err)
}

func (s *sparkplugOutput) Write(ctx context.Context, msg *service.Message) error {
	// Extract data from message
	data, err := s.extractMessageData(msg)
	if err != nil {
		s.logger.Errorf("Failed to extract message data: %v", err)
		s.publishErrors.Incr(1)
		return err
	}

	if len(data) == 0 {
		s.logger.Debug("No metrics to publish in message")
		return nil
	}

	// Create DATA message
	if err := s.publishDataMessage(data); err != nil {
		s.logger.Errorf("Failed to publish DATA message: %v", err)
		s.publishErrors.Incr(1)
		return err
	}

	s.messagesPublished.Incr(1)
	return nil
}

func (s *sparkplugOutput) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil && s.client.IsConnected() {
		// Publish DEATH message before disconnecting gracefully
		deathTopic, deathPayload := s.createDeathMessage()
		token := s.client.Publish(deathTopic, s.qos, false, deathPayload)
		token.WaitTimeout(5 * time.Second)

		if token.Error() == nil {
			s.deathsPublished.Incr(1)
			s.logger.Info("Published DEATH message before disconnect")
		}

		s.client.Disconnect(1000)
	}

	s.logger.Info("Sparkplug output closed")
	return nil
}

// Private methods for the rest of the implementation...
func (s *sparkplugOutput) createDeathMessage() (string, []byte) {
	var topic string
	if s.deviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", s.groupID, s.edgeNodeID, s.deviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NDEATH/%s", s.groupID, s.edgeNodeID)
	}

	bdSeqMetric := &sproto.Payload_Metric{
		Name: func() *string { s := "bdSeq"; return &s }(),
		Value: &sproto.Payload_Metric_LongValue{
			LongValue: s.bdSeq,
		},
		Datatype: func() *uint32 { d := uint32(4); return &d }(),
	}

	deathPayload := &sproto.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Metrics:   []*sproto.Payload_Metric{bdSeqMetric},
	}

	payloadBytes, err := proto.Marshal(deathPayload)
	if err != nil {
		s.logger.Errorf("Failed to marshal DEATH payload: %v", err)
		return topic, []byte{}
	}

	return topic, payloadBytes
}

func (s *sparkplugOutput) publishBirthMessage() error {
	var topic string
	if s.deviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", s.groupID, s.edgeNodeID, s.deviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", s.groupID, s.edgeNodeID)
	}

	var metrics []*sproto.Payload_Metric

	// Add bdSeq metric
	bdSeqMetric := &sproto.Payload_Metric{
		Name:  func() *string { s := "bdSeq"; return &s }(),
		Alias: func() *uint64 { a := uint64(0); return &a }(),
		Value: &sproto.Payload_Metric_LongValue{
			LongValue: s.bdSeq,
		},
		Datatype: func() *uint32 { d := uint32(4); return &d }(),
	}
	metrics = append(metrics, bdSeqMetric)

	// Add configured metrics
	s.stateMu.RLock()
	for _, metricConfig := range s.metrics {
		metric := &sproto.Payload_Metric{
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

		metrics = append(metrics, metric)
	}
	s.stateMu.RUnlock()

	birthPayload := &sproto.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(0); return &s }(),
		Metrics:   metrics,
	}

	payloadBytes, err := proto.Marshal(birthPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal BIRTH payload: %w", err)
	}

	token := s.client.Publish(topic, s.qos, false, payloadBytes)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish BIRTH message: %w", token.Error())
	}

	s.logger.Infof("Published BIRTH message on topic: %s", topic)
	return nil
}

func (s *sparkplugOutput) extractMessageData(msg *service.Message) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured message data: %w", err)
	}

	if s.autoExtractTagName {
		if tagName, exists := msg.MetaGet("tag_name"); exists {
			for _, metricConfig := range s.metrics {
				if metricConfig.Name == tagName {
					value, err := s.extractValueFromPath(structured, metricConfig.ValueFrom)
					if err != nil {
						s.logger.Debugf("Failed to extract value for metric %s: %v", tagName, err)
						continue
					}
					data[tagName] = value
					break
				}
			}
		}
	}

	for _, metricConfig := range s.metrics {
		if _, exists := data[metricConfig.Name]; exists {
			continue
		}

		value, err := s.extractValueFromPath(structured, metricConfig.ValueFrom)
		if err != nil {
			s.logger.Debugf("Failed to extract value for metric %s: %v", metricConfig.Name, err)
			continue
		}
		data[metricConfig.Name] = value
	}

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

func (s *sparkplugOutput) publishDataMessage(data map[string]interface{}) error {
	var topic string
	if s.deviceID != "" {
		topic = fmt.Sprintf("spBv1.0/%s/DDATA/%s/%s", s.groupID, s.edgeNodeID, s.deviceID)
	} else {
		topic = fmt.Sprintf("spBv1.0/%s/NDATA/%s", s.groupID, s.edgeNodeID)
	}

	s.stateMu.Lock()
	s.seqCounter++
	if s.seqCounter == 0 {
		s.sequenceWraps.Incr(1)
	}
	currentSeq := s.seqCounter
	s.stateMu.Unlock()

	var metrics []*sproto.Payload_Metric

	for metricName, value := range data {
		alias, hasAlias := s.metricAliases[metricName]
		metricType, hasType := s.metricTypes[metricName]

		if !hasAlias || !hasType {
			s.logger.Debugf("Metric %s not configured, skipping", metricName)
			continue
		}

		metric := &sproto.Payload_Metric{
			Alias:    &alias,
			Datatype: s.getSparkplugDataType(metricType),
		}

		s.setMetricValue(metric, value, metricType)
		metrics = append(metrics, metric)

		if s.retainLastValues {
			s.stateMu.Lock()
			s.lastValues[metricName] = value
			s.stateMu.Unlock()
		}
	}

	if len(metrics) == 0 {
		return fmt.Errorf("no valid metrics to publish")
	}

	dataPayload := &sproto.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Seq:       func() *uint64 { s := uint64(currentSeq); return &s }(),
		Metrics:   metrics,
	}

	payloadBytes, err := proto.Marshal(dataPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal DATA payload: %w", err)
	}

	token := s.client.Publish(topic, s.qos, false, payloadBytes)
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

func (s *sparkplugOutput) setMetricValue(metric *sproto.Payload_Metric, value interface{}, metricType string) {
	switch strings.ToLower(metricType) {
	case "int8", "int16", "int32":
		if intVal, ok := s.convertToInt32(value); ok {
			metric.Value = &sproto.Payload_Metric_IntValue{IntValue: intVal}
		}
	case "int64":
		if longVal, ok := s.convertToInt64(value); ok {
			metric.Value = &sproto.Payload_Metric_LongValue{LongValue: longVal}
		}
	case "float":
		if floatVal, ok := s.convertToFloat32(value); ok {
			metric.Value = &sproto.Payload_Metric_FloatValue{FloatValue: floatVal}
		}
	case "double":
		if doubleVal, ok := s.convertToFloat64(value); ok {
			metric.Value = &sproto.Payload_Metric_DoubleValue{DoubleValue: doubleVal}
		}
	case "boolean":
		if boolVal, ok := s.convertToBool(value); ok {
			metric.Value = &sproto.Payload_Metric_BooleanValue{BooleanValue: boolVal}
		}
	case "string":
		if strVal, ok := s.convertToString(value); ok {
			metric.Value = &sproto.Payload_Metric_StringValue{StringValue: strVal}
		}
	}
}

func (s *sparkplugOutput) convertToInt32(value interface{}) (uint32, bool) {
	switch v := value.(type) {
	case int:
		return uint32(v), true
	case int32:
		return uint32(v), true
	case int64:
		return uint32(v), true
	case float64:
		return uint32(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return uint32(i), true
		}
	}
	return 0, false
}

func (s *sparkplugOutput) convertToInt64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case int:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case float64:
		return uint64(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
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
