package main

import (
	"github.com/Jeffail/benthos/v3/lib/serverless/lambda"
	_ "github.com/makenew/benthos-plugin/v1/plugin"
)

func main() {
	lambda.Run()
}
