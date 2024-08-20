package output

import (
	"context"
	"github.com/voutilad/rp-connect-python/processor"
	"runtime"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
)

var configSpec = service.NewConfigSpec().
	Summary("Post-process data with Python.").
	Field(service.NewStringField("script").
		Description("Python code to execute.")).
	Field(service.NewStringField("exe").
		Description("Path to a Python executable.").
		Default("python3")).
	Field(service.NewStringField("mode").
		Description("Toggle different Python runtime modes: 'multi', 'single', and 'legacy' (the default)").
		Default(string(python.LegacyMode)))

type pythonOutput struct {
	logger    *service.Logger
	processor service.Processor
}

func init() {
	err := service.RegisterOutput("python",
		configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			// Extract our configuration.
			exe, err := conf.FieldString("exe")
			if err != nil {
				panic(err)
			}
			script, err := conf.FieldString("script")
			if err != nil {
				return nil, -1, err
			}
			modeString, err := conf.FieldString("mode")
			if err != nil {
				return nil, -1, err
			}

			p, err := processor.NewPythonProcessor(exe, script, python.StringAsMode(modeString), mgr.Logger())
			return &pythonOutput{
				logger:    mgr.Logger(),
				processor: p,
			}, runtime.NumCPU(), nil
		})

	if err != nil {
		panic(err)
	}
}

func (p *pythonOutput) Connect(_ context.Context) error {
	// noop
	return nil
}

func (p *pythonOutput) Write(ctx context.Context, m *service.Message) error {
	// TODO: for now we hack and just pass it to a PythonProcessor and ignore
	//       if it returns a batch.
	p.logger.Debug("CALLED!")
	_, err := p.processor.Process(ctx, m)
	return err
}

func (p *pythonOutput) Close(ctx context.Context) error {
	return p.processor.Close(ctx)
}
