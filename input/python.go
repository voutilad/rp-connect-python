package input

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	py "github.com/voutilad/gogopython"
	"unsafe"

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
	globals       py.PyObjectPtr
	locals        py.PyObjectPtr
	code          py.PyCodeObjectPtr
	serializer    *python.Serializer
	args          py.PyObjectPtr
	kwargs        py.PyObjectPtr
	script        string
	generatorName string
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
			return newPythonInput(exe, script, name, python.StringAsMode(modeString), mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

func newPythonInput(exe, script, name string, mode python.Mode, logger *service.Logger) (service.Input, error) {
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

		main := py.PyImport_AddModule("__main__")
		if main == py.NullPyObjectPtr {
			return errors.New("failed to add __main__ module")
		}
		globals := py.PyModule_GetDict(main)
		if globals == py.NullPyObjectPtr {
			return errors.New("failed to create globals")
		}

		kwargs := py.PyDict_New()
		if kwargs == py.NullPyObjectPtr {
			return errors.New("failed to create new state dict")
		}
		args := py.PyTuple_New(0)
		if args == py.NullPyObjectPtr {
			return errors.New("failed to create new tuple")
		}

		p.locals = locals
		p.globals = globals
		p.args = args
		p.kwargs = kwargs

		// Compile our script and find our helpers.
		code := py.Py_CompileString(p.script, "rp_connect_python_input.py", py.PyFileInput)
		if code == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python script")
		}
		p.code = code

		result := py.PyEval_EvalCode(code, p.globals, p.locals)
		if result == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to evaluate input script")
		}
		defer py.Py_DecRef(result)

		obj := py.PyDict_GetItemString(p.locals, p.generatorName)
		if obj == py.NullPyObjectPtr {
			// Fallback to checking globals.
			obj = py.PyDict_GetItemString(p.globals, p.generatorName)
			if obj == py.NullPyObjectPtr {
				return errors.New(fmt.Sprintf("failed to find python data generator object '%s'", p.generatorName))
			}
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

		serializer, err := python.NewSerializer()
		if err != nil {
			panic(err)
		}
		p.serializer = serializer

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
		var next py.PyObjectPtr

		// TODO: memoize function into a closure
		switch p.mode {
		case Iterable:
			next = py.PyIter_Next(p.generator)
			if next == py.NullPyObjectPtr {
				return service.ErrEndOfInput
			}
			defer py.Py_DecRef(next)
		case List:
			next = py.PyList_GetItem(p.generator, int64(p.idx))
			p.idx++
			if next == py.NullPyObjectPtr {
				py.PyErr_Clear()
				return service.ErrEndOfInput
			}
		case Tuple:
			next = py.PyTuple_GetItem(p.generator, int64(p.idx))
			p.idx++
			if next == py.NullPyObjectPtr {
				py.PyErr_Clear()
				return service.ErrEndOfInput
			}
		case Callable:
			py.PyErr_Clear()
			next = py.PyObject_Call(p.generator, p.args, p.kwargs)
			if next == py.NullPyObjectPtr {
				py.PyErr_Print()
				p.logger.Error("null result from calling python input function")
				return service.ErrEndOfInput
			}
			if py.BaseType(next) == py.None {
				// No more work.
				return service.ErrEndOfInput
			}
		default:
			panic("unhandled input mode")
		}

		switch py.BaseType(next) {
		case py.None:
			return service.ErrEndOfInput

		case py.Long:
			// TODO: overflow (signed vs. unsigned)
			long := py.PyLong_AsLong(next)
			m = service.NewMessage([]byte{})
			m.SetStructured(long)

		case py.Float:
			float := py.PyFloat_AsDouble(next)
			m = service.NewMessage([]byte{})
			m.SetStructured(float)

		case py.String:
			s, err := py.UnicodeToString(next)
			if err != nil {
				p.logger.Error("failed to decode python input string")
				return service.ErrEndOfInput
			}
			m = service.NewMessage([]byte(s))

		case py.Bytes:
			// Copy out the bytes.
			bytes := py.PyBytes_AsString(next)
			sz := py.PyBytes_Size(next)
			buffer := make([]byte, sz)
			copy(buffer, unsafe.Slice(bytes, sz))
			m = service.NewMessage(buffer)

		case py.Tuple, py.List, py.Dict, py.Unknown:
			// Use JSON serializer.
			buffer, err := p.serializer.JsonBytes(next)
			if err != nil {
				panic(err)
			}
			m = service.NewMessage(buffer)
		}
		return nil
	})

	return m, func(ctx context.Context, err error) error { return nil }, err
}

func (p *pythonInput) Close(ctx context.Context) error {
	_ = p.runtime.Map(ctx, func(_ *python.InterpreterTicket) error {
		// Even if one of these are null, Py_DecRef is fine being passed NULL.
		py.Py_DecRef(p.generator)
		py.Py_DecRef(p.locals)
		py.Py_DecRef(p.globals)
		py.Py_DecRef(p.args)
		py.Py_DecRef(p.kwargs)
		return nil
	})

	return p.runtime.Stop(ctx)
}
