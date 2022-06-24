package main

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/components/aws"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
	_ "github.com/makenew/benthos-plugin/v2/plugin"
)

func main() {
	aws.RunLambda(context.Background())
}
