package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"
	_ "github.com/voutilad/rp-connect-python/input"
	_ "github.com/voutilad/rp-connect-python/output"
	_ "github.com/voutilad/rp-connect-python/processor"
)

func main() {
	/*
		f, _ := os.Create("cpu.prof")
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	*/
	service.RunCLI(context.Background())
}
