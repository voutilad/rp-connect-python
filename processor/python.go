package processor

import (
	"context"
	"errors"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

type pythonProcessor struct {
	logger    *service.Logger
	config    py.PyConfig_3_12
	mainState py.PyThreadStatePtr
	subState  py.PyThreadStatePtr
	script    string
}

func init() {
	configSpec := service.
		NewConfigSpec().
		Summary("Process data with Python").
		Field(service.NewStringField("script"))

	ctor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newPythonProcessor(mgr.Logger(), conf)
	}

	err := service.RegisterProcessor("python", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func newPythonProcessor(logger *service.Logger, conf *service.ParsedConfig) (*pythonProcessor, error) {
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	py.Load_library()

	// Pre-configure the Python Interpreter
	preConfig := py.PyPreConfig{}
	py.PyPreConfig_InitIsolatedConfig(&preConfig)
	//preConfig.Allocator = 3
	status := py.Py_PreInitialize(&preConfig)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to preinitialize python: %s\n", errmsg)
		return nil, errors.New(errmsg)
	}

	/*
	 * Configure our Paths. We need to approximate an isolated config from regular
	 * because Python will ignore our modifying some values if we initialize an
	 * isolated config. Annoying.
	 */
	config := py.PyConfig_3_12{}
	py.PyConfig_InitPythonConfig(&config)
	config.ParseArgv = 0
	config.SafePath = 1
	config.UserSiteDirectory = 0
	config.InstallSignalHandlers = 0

	// TODO: parameterize home dir
	home := "/Users/dv/src/gogopython/venv"
	status = py.PyConfig_SetBytesString(&config, &config.Home, home)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to set home: %s\n", errmsg)
		return nil, errors.New(errmsg)
	}

	// TODO: find paths or provide them in config
	path := strings.Join([]string{
		"/opt/homebrew/Cellar/python@3.12/3.12.4/Frameworks/Python.framework/Versions/3.12/lib/python3.12",
		"/opt/homebrew/Cellar/python@3.12/3.12.4/Frameworks/Python.framework/Versions/3.12/lib/python3.12/lib-dynload",
		"/Users/dv/src/gogopython/venv/lib/python3.12/site-packages",
	}, ":")
	status = py.PyConfig_SetBytesString(&config, &config.PythonPathEnv, path)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to set path: %s\n", errmsg)
		return nil, errors.New(errmsg)
	}
	status = py.Py_InitializeFromConfig(&config)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to initialize Python: %s", errmsg)
		return nil, errors.New(errmsg)
	}

	// Save details on our Main interpreter, grab the GIL, and swap it out.
	gil := py.PyGILState_Ensure()
	mainStatePtr := py.PyThreadState_Get()
	py.PyThreadState_Swap(py.NullThreadState)

	interpreterConfig := py.PyInterpreterConfig{}
	interpreterConfig.Gil = py.DefaultGil // OwnGil works in 3.12, but is hard to use.
	interpreterConfig.CheckMultiInterpExtensions = 1

	var subStatePtr py.PyThreadStatePtr
	status = py.Py_NewInterpreterFromConfig(&subStatePtr, &interpreterConfig)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to create sub-interpreter: %s", errmsg)
		return nil, errors.New(errmsg)
	}
	logger.Info("created new subinterpreter")
	py.PyThreadState_Swap(mainStatePtr)

	py.PyGILState_Release(gil)
	py.PyThreadState_Swap(py.NullThreadState)

	return &pythonProcessor{
		logger:    logger,
		config:    config,
		mainState: mainStatePtr,
		subState:  subStatePtr,
		script:    script,
	}, nil
}

func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {

	p.logger.Info("processing message")

	py.PyThreadState_Swap(p.subState)

	py.PyRun_SimpleString(p.script)

	py.PyThreadState_Swap(py.NullThreadState)

	return []*service.Message{m}, nil
}

func (p *pythonProcessor) Close(ctx context.Context) error {
	// If these fail, you get fireworks :D

	//py.PyGILState_Release(p.gil)
	//py.Py_FinalizeEx()

	return nil
}
