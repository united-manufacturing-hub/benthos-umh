package main

import (
	"context"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
	"github.com/benthosdev/benthos/v4/public/components/aws"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/plugin"
)

func main() {
	aws.RunLambda(context.Background())
}
