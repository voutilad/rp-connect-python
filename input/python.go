package input

import (
	"context"
	"errors"
	"fmt"
	py "github.com/voutilad/gogopython"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
)

type inputMode int

const (
	Callable inputMode = iota // Callable acts like a Python function.
	Iterable                  // Iterable acts like a Python iterable or generator.
	List
	Tuple
)

type pythonInput struct {
	logger        *service.Logger
	runtime       python.Runtime
	generator     py.PyObjectPtr
	mode          inputMode
	ack           py.PyObjectPtr
	globals       py.PyObjectPtr
	locals        py.PyObjectPtr
	code          py.PyCodeObjectPtr
	script        string
	generatorName string
	ackName       string
	idx           int
}

var configSpec = service.NewConfigSpec().
	Summary("Generate data with Python.").
	Field(service.NewStringField("script").
		Description("Python code to execute.")).
	Field(service.NewStringField("exe").
		Description("Path to a Python executable.").
		Default("python3")).
	Field(service.NewStringField("name").
		Description("Name of python function to call for generating data.").
		Default("read")).
	Field(service.NewStringField("ack").
		Description("Name of python function to call for acknowledging data.").
		Default("")).
	Field(service.NewStringField("mode").
		Description("Toggle different Python runtime modes: 'multi', 'single', and 'legacy' (the default)").
		Default(string(python.LegacyMode)))

func init() {
	err := service.RegisterInput("python", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			// Extract our configuration.
			exe, err := conf.FieldString("exe")
			if err != nil {
				panic(err)
			}
			script, err := conf.FieldString("script")
			if err != nil {
				return nil, err
			}
			modeString, err := conf.FieldString("mode")
			if err != nil {
				return nil, err
			}
			name, err := conf.FieldString("name")
			if err != nil {
				return nil, err
			}
			ack, err := conf.FieldString("ack")
			if err != nil {
				return nil, err
			}

			return newPythonInput(exe, script, name, ack, python.StringAsMode(modeString), mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

func newPythonInput(exe, script, name, ack string, mode python.Mode, logger *service.Logger) (service.Input, error) {
	var err error
	var r python.Runtime

	switch mode {
	case python.LegacyMode:
		r, err = python.NewMultiInterpreterRuntime(exe, 1, true, logger)
	case python.SingleMode:
		r, err = python.NewSingleInterpreterRuntime(exe, logger)
	case python.MultiMode:
		r, err = python.NewMultiInterpreterRuntime(exe, 1, false, logger)
	default:
		return nil, errors.New("invalid mode")
	}
	if err != nil {
		return nil, err
	}

	// TODO: do we want nacks?
	return &pythonInput{
		logger:        logger,
		runtime:       r,
		script:        script,
		generatorName: name,
		ackName:       ack,
	}, nil
}

func (p *pythonInput) Connect(ctx context.Context) error {
	err := p.runtime.Start(ctx)
	if err != nil {
		return err
	}

	err = p.runtime.Map(ctx, func(_ *python.InterpreterTicket) error {
		locals := py.PyDict_New()
		if locals == py.NullPyObjectPtr {
			return errors.New("failed to create new locals dict")
		}
		globals := py.PyDict_New()
		if globals == py.NullPyObjectPtr {
			return errors.New("failed to create new globals dict")
		}

		p.locals = locals
		p.globals = globals

		// Compile our script and find our helpers.
		code := py.Py_CompileString(p.script, "rp_connect_python_input.py", py.PyFileInput)
		if code == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python script")
		}
		p.code = code

		result := py.PyEval_EvalCode(code, p.globals, p.locals)
		if result == py.NullPyObjectPtr {
			return errors.New("failed to evaluate input script")
		}
		defer py.Py_DecRef(result)

		obj := py.PyDict_GetItemString(locals, p.generatorName)
		if obj == py.NullPyObjectPtr {
			return errors.New(fmt.Sprintf("failed to find python data generator object '%s'", p.generatorName))
		}
		switch t := py.BaseType(obj); t {
		case py.Generator:
			p.mode = Iterable
		case py.List:
			p.mode = List
		case py.Tuple:
			p.mode = Tuple
		case py.Function:
			p.mode = Callable
		default:
			return errors.New(fmt.Sprintf("invalid python data generator object type '%s'", t.String()))
		}
		p.generator = obj

		if p.ackName != "" {
			ack := py.PyDict_GetItemString(locals, p.ackName)
			if ack == py.NullPyObjectPtr {
				return errors.New(fmt.Sprintf("failed to find python ack object '%s'", p.ackName))
			}

			if py.BaseType(ack) != py.Function {
				return errors.New(fmt.Sprintf("python ack object '%s' is not callable", p.ackName))
			}
			p.ack = ack
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return nil
}

func (p *pythonInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	var m *service.Message = nil

	ticket, err := p.runtime.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer func() { _ = p.runtime.Release(ticket) }()

	err = p.runtime.Apply(ticket, ctx, func() error {
		// TODO: memoize function into a closure
		switch p.mode {
		case Iterable:
			next := py.PyIter_Next(p.generator)
			if next == py.NullPyObjectPtr {
				return service.ErrEndOfInput
			}
			py.Py_DecRef(next)
			m = service.NewMessage([]byte("generator blah"))
			return nil
		case List:
			next := py.PyList_GetItem(p.generator, int64(p.idx))
			p.idx++
			if next == py.NullPyObjectPtr {
				py.PyErr_Clear()
				return service.ErrEndOfInput
			}
			m = service.NewMessage([]byte("list blah"))
			return nil
		case Tuple:
			next := py.PyTuple_GetItem(p.generator, int64(p.idx))
			p.idx++
			if next == py.NullPyObjectPtr {
				py.PyErr_Clear()
				return service.ErrEndOfInput
			}
			m = service.NewMessage([]byte("tuple blah"))
			return nil
		case Callable:
			next := py.PyObject_CallObject(p.generator, py.NullPyObjectPtr)
			if next == py.NullPyObjectPtr {
				p.logger.Error("null result from calling python input function")
				return service.ErrEndOfInput
			}
			if py.BaseType(next) == py.None {
				// No more work.
				return service.ErrEndOfInput
			}
			m = service.NewMessage([]byte("function blah"))
			return nil
		}
		return service.ErrEndOfInput
	})

	return m, func(ctx context.Context, err error) error { return nil }, err
}

func (p *pythonInput) Close(ctx context.Context) error {
	_ = p.runtime.Map(ctx, func(_ *python.InterpreterTicket) error {
		// Even if one of these are null, Py_DecRef is fine being passed NULL.
		py.Py_DecRef(p.ack)
		py.Py_DecRef(p.generator)
		py.Py_DecRef(p.locals)
		py.Py_DecRef(p.globals)
		return nil
	})

	return p.runtime.Stop(ctx)
}
