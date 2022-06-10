package main

import (
	"github.com/benthosdev/benthos/v4/internal/serverless/lambda"
	_ "github.com/makenew/benthos-plugin/v2/plugin"
)

func main() {
	lambda.Run()
}
