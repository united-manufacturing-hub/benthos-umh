package plugin

import (
	"context"
	"math/rand"

	"github.com/benthosdev/benthos/v4/public/service"
)

var OPCUAConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from OPC-UA servers.").
	Field(service.NewStringField("endpoint").Default("opc.tcp://localhost:4840").Description("The OPC-UA endpoint to connect to.")).
	Field(service.NewStringField("nodeID").Default("").Description("The OPC-UA node ID to start the browsing."))

func newOPCUAInput(conf *service.ParsedConfig) (service.Input, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, err
	}

	nodeID, err := conf.FieldString("nodeID")
	if err != nil {
		return nil, err
	}
	return service.AutoRetryNacks(&OPCUAInput{endpoint, nodeID}), nil
}

func init() {
	err := service.RegisterInput(
		"opcua", OPCUAConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newOPCUAInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type OPCUAInput struct {
	endpoint string
	nodeID   string
}

func (g *OPCUAInput) Connect(ctx context.Context) error {
	return nil
}

func (g *OPCUAInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	b := make([]byte, 100)
	for k := range b {
		b[k] = byte((rand.Int() % 94) + 32)
	}
	return service.NewMessage(b), func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *OPCUAInput) Close(ctx context.Context) error {
	return nil
}
