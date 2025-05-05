package umhstreamplugin

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/internal/message"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultOutputTopic   = "umh.v1.messages"
	defaultBrokerAddress = "localhost:9092"
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
		kgo.SeedBrokers(defaultBrokerAddress),
		kgo.AllowAutoTopicCreation(), // Allow creating the defaultOutputTopic if it doesn't exists
		// Todo : More configs for optimisation
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client: %v", err)
	}

	o.client = client
	o.log.Infof("Connection to the kafka broker %s is successful", defaultBrokerAddress)

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

func newUMHStreamOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
	topic, err := conf.FieldInterpolatedString("topic")
	if err != nil {
		return nil, 0, fmt.Errorf("error while parsing topic string from the config: %v", err)
	}

	return &umhStreamOutput{
		topic: topic,
		log:   mgr.Logger(),
	}, 0, nil

}
