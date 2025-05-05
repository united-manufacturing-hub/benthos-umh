package umhstreamplugin

import (
	"context"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultOutputTopic   = "umh.v1.messages"
	defaultBrokerAddress = "localhost:9092"
	defaultClientID      = "umh_core"
)

func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes to the local kafka topic, setting each message's key based on the given topic variable").
		Description("Sends messages to the umh core kafka topic with specified key").
		Field(service.NewInterpolatedStringField("topic").
			Description("key to use when sending messages to the output topic. Supports interpolation.").
			Example("${! meta(\"topic\") }").
			Example("enterprise.site.area_historian"))
}

type umhStreamOutput struct {
	topic  *service.InterpolatedString
	client *kgo.Client
	log    *service.Logger
}

// Close closes the underlying kafka client
func (o *umhStreamOutput) Close(ctx context.Context) error {
	o.log.Infof("Attempting to close the umh_stream kafka client")
	if o.client != nil {
		o.client.Close()
	}
	o.log.Infof("umh_stream kafka client closed successfully")
	return nil
}

// Connect initializes the kafka client
func (o *umhStreamOutput) Connect(ctx context.Context) error {
	o.log.Infof("Connecting to umh core stream kafka broker: %v", defaultBrokerAddress)

	// Create the kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(defaultBrokerAddress),       // bootstrap broker addresses
		kgo.AllowAutoTopicCreation(),                // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),               // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(1*time.Hour),            // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(5*time.Second),              // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.DefaultProduceTopic(defaultOutputTopic), // topic to produce messages to. The plugin writes messages only to a single default topic
		kgo.RequiredAcks(kgo.LeaderAck()),           // Partition leader has to send acknowledment on successful write
		kgo.MaxBufferedRecords(1000),                // Max amount of records hte client will buffer
		kgo.ProduceRequestTimeout(5*time.Second),    // Produce Request Timeout
		kgo.ProducerLinger(1*time.Second),           // Duration on how long individual partitons will linger waiting for more records before triggering a Produce request
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client: %v", err)
	}

	o.client = client
	o.log.Infof("Connection to the kafka broker %s is successful", defaultBrokerAddress)

	return nil
}

// WriteBatch implements service.BatchOutput.
func (o *umhStreamOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {

	records := make([]*kgo.Record, len(msgs))
	for _, msg := range msgs {
		key, err := o.topic.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve topic field: %v", err)
		}

		o.log.Tracef("sending message with key: %s", key)

		msgAsBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		record := &kgo.Record{
			Topic: defaultOutputTopic,
			Key:   []byte(key),
			Value: msgAsBytes,
		}

		records = append(records, record)

	}
	if err := o.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("error while writing batch output to kafka: %v", err)
	}
	return nil
}

// Write writes the message to the output topic
func (o *umhStreamOutput) Write(ctx context.Context, msg *service.Message) error {
	key, err := o.topic.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to resolve topic field: %v", err)
	}

	o.log.Tracef("sending message with key: %s", key)

	msgAsBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: defaultOutputTopic,
		Key:   []byte(key),
		Value: msgAsBytes,
	}

	if err := o.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	return nil
}

func newUMHStreamOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	topic, err := conf.FieldInterpolatedString("topic")
	if err != nil {
		return nil, batchPolicy, 0, fmt.Errorf("error while parsing topic string from the config: %v", err)
	}

	return &umhStreamOutput{
		topic: topic,
		log:   mgr.Logger(),
	}, batchPolicy, maxInFlight, nil

}
