package umhstreamplugin

import "github.com/redpanda-data/benthos/v4/public/service"

func main() {
	service.RegisterBatchOutput("umh_stream", outputConfig(), newUMHStreamOutput)
}
