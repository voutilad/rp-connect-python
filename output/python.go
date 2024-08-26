package output

import (
	"context"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"github.com/voutilad/rp-connect-python/processor"
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
	processor service.BatchProcessor
}

func init() {
	err := service.RegisterBatchOutput("python",
		configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			policy := service.BatchPolicy{}
			// Extract our configuration.
			exe, err := conf.FieldString("exe")
			if err != nil {
				panic(err)
			}
			script, err := conf.FieldString("script")
			if err != nil {
				return nil, policy, 0, err
			}
			modeString, err := conf.FieldString("mode")
			if err != nil {
				return nil, policy, 0, err
			}

			p, err := processor.NewPythonProcessor(exe, script, 1, python.StringAsMode(modeString), mgr.Logger())
			return &pythonOutput{
				logger:    mgr.Logger(),
				processor: p,
			}, policy, 1, nil
		})

	if err != nil {
		panic(err)
	}
}

func (p *pythonOutput) Connect(_ context.Context) error {
	// noop
	return nil
}

func (p *pythonOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	// TODO: for now we hack and just pass it to a PythonProcessor and ignore
	//       if it returns a batch.
	_, err := p.processor.ProcessBatch(ctx, batch)
	return err
}

func (p *pythonOutput) Close(ctx context.Context) error {
	return p.processor.Close(ctx)
}
