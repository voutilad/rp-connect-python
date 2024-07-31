package main

import (
	"context"
	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"
	_ "github.com/voutilad/rp-connect-python/processor"
)

func main() {
	service.RunCLI(context.Background())
}
