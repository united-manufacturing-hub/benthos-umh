package umhstreamplugin

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Record struct {
	Topic string
	Key   []byte
	Value []byte
}

type Producer interface {
	ProduceSync(context.Context, []Record) error
}

type Streamer interface {
	Connect(...kgo.Opt) error
	Close() error
	Producer
}

// Client is a wrapper for franz-go kafka client
type Client struct {
	client *kgo.Client
}

// NewClient initializes the franz-go client
func NewClient() Streamer {
	return &Client{}
}

// Connect connect to the seedbroker with the given kafka client options
func (k *Client) Connect(opts ...kgo.Opt) error {
	var err error
	k.client, err = kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the underlying franz-go kafka client
func (k *Client) Close() error {
	if k.client != nil {
		// franz-go client.Close() never returns an error
		k.client.Close()
	}
	return nil
}

// ProduceSync produces a message batch to kafka
func (k *Client) ProduceSync(ctx context.Context, records []Record) error {

	if k.client == nil {
		return errors.New("attempt to produce using a nil kafka client")
	}

	kgoRecords := make([]*kgo.Record, len(records))
	for _, r := range records {
		kgoRecord := &kgo.Record{
			Topic: r.Topic,
			Key:   r.Key,
			Value: r.Value,
		}
		kgoRecords = append(kgoRecords, kgoRecord)
	}

	return k.client.ProduceSync(ctx, kgoRecords...).FirstErr()
}
