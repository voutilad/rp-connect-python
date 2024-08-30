package input

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"unsafe"
)

type inputMode int

const (
	Callable inputMode = iota // Callable acts like a Python function.
	Iterable                  // Iterable acts like a Python iterable or generator.
	List                      // A Python List.
	Tuple                     // A Python Tuple.
	Object                    // A single Python Object.
)

type pythonInput struct {
	logger    *service.Logger
	runtime   python.Runtime
	generator py.PyObjectPtr
	mode      inputMode
	globals   py.PyObjectPtr
	code      py.PyCodeObjectPtr

	serializer     *python.Serializer
	serializerMode python.SerializerMode

	args          py.PyObjectPtr
	kwargs        py.PyObjectPtr
	modules       []string
	script        string
	generatorName string
	idx           int64
	batchSize     int
	boundsHint    int64
}

var configSpec = service.NewConfigSpec().
	Summary("Generate data with Python.").
	Field(service.NewStringField("script").
		Description("Python code to execute.")).
	Field(service.NewStringField("exe").
		Description("Path to a Python executable.").
		Default("python3")).
	Field(service.NewStringField("name").
		Description("Name of python function to call or object to read for generating data.").
		Default("read")).
	Field(service.NewIntField("batch_size").
		Description("Size of batches to generate.").
		Default(1)).
	Field(service.NewStringListField("modules").
		Description("A list of Python function modules to pre-import.")).
	Field(service.NewStringField("mode").
		Description("Toggle different Python runtime modes.").
		Examples(string(python.Global), string(python.Isolated), string(python.IsolatedLegacy)).
		Default(string(python.Global))).
	Field(service.NewStringField("serializer").
		Description("Serialization mode to use on results.").
		Examples(string(python.None), string(python.Pickle), string(python.Bloblang)).
		Default(string(python.Bloblang)))

func noOpAckFn(_ context.Context, _ error) error { return nil }

func init() {
	err := service.RegisterBatchInput("python", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			// Extract our configuration.
			exe, err := conf.FieldString("exe")
			if err != nil {
				panic(err)
			}
			script, err := conf.FieldString("script")
			if err != nil {
				return nil, err
			}
			mode, err := conf.FieldString("mode")
			if err != nil {
				return nil, err
			}
			name, err := conf.FieldString("name")
			if err != nil {
				return nil, err
			}
			batchSize, err := conf.FieldInt("batch_size")
			if err != nil {
				return nil, err
			}
			serializerMode, err := conf.FieldString("serializer")
			if err != nil {
				return nil, err
			}
			modules, err := conf.FieldStringList("modules")
			if err != nil {
				modules = []string{}
			}
			return newPythonInput(exe, script, name, modules, batchSize, python.StringAsMode(mode), python.StringAsSerializerMode(serializerMode), mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

func newPythonInput(exe, script, name string, modules []string, batchSize int, mode python.Mode, serializer python.SerializerMode, logger *service.Logger) (service.BatchInput, error) {
	var err error
	var r python.Runtime

	// XXX for now, enforce that we only support non-serializing modes when
	// using a global interpreter mode.
	if serializer == python.None && mode != python.Global {
		return nil,
			errors.New("isolated interpreters require bloblang or pickle serialization")
	}

	switch mode {
	case python.IsolatedLegacy:
		r, err = python.NewMultiInterpreterRuntime(exe, 1, true, logger)
	case python.Global:
		r, err = python.NewSingleInterpreterRuntime(exe, logger)
	case python.Isolated:
		r, err = python.NewMultiInterpreterRuntime(exe, 1, false, logger)
	default:
		return nil, errors.New("invalid mode")
	}
	if err != nil {
		return nil, err
	}

	// TODO: do we want nacks?
	return &pythonInput{
		logger:         logger,
		runtime:        r,
		script:         script,
		generatorName:  name,
		batchSize:      batchSize,
		boundsHint:     -1,
		serializerMode: serializer,
		modules:        modules,
	}, nil
}

func (p *pythonInput) Connect(ctx context.Context) error {
	err := p.runtime.Start(ctx)
	if err != nil {
		return err
	}

	if len(p.modules) > 0 {
		err = python.LoadModules(p.modules, ctx)
		if err != nil {
			return err
		}
	}

	err = p.runtime.Map(ctx, func(_ *python.InterpreterTicket) error {
		// Compile our script early to detect syntax errors.
		code := py.Py_CompileString(p.script, "__rp_connect_python_input__.py", py.PyFileInput)
		if code == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python script")
		}
		p.code = code

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

		p.globals = globals
		p.args = args
		p.kwargs = kwargs

		// Execute the script to establish our data generating object.
		result := py.PyEval_EvalCode(code, p.globals, py.NullPyObjectPtr)
		if result == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to evaluate input script")
		}
		defer py.Py_DecRef(result)

		// Find our data generator.
		obj := py.PyDict_GetItemString(p.globals, p.generatorName)
		if obj == py.NullPyObjectPtr {
			return errors.New(fmt.Sprintf("failed to find python data generator object '%s'", p.generatorName))
		}

		switch t := py.BaseType(obj); t {
		case py.Generator:
			p.mode = Iterable
			p.logger.Debug("generating data from an iterable")
		case py.List:
			p.mode = List
			p.boundsHint = py.PyList_Size(obj)
			p.logger.Debug("generating data from list")
		case py.Tuple:
			p.mode = Tuple
			p.boundsHint = py.PyTuple_Size(obj)
			p.logger.Debug("generating data from a tuple")
		case py.Function:
			p.mode = Callable
			p.logger.Debug("generating data from a callable")
		default:
			p.mode = Object
			p.boundsHint = 1
			p.logger.Debug("generating data from a single object")
		}
		p.generator = obj

		serializer, err := python.NewSerializer()
		if err != nil {
			return err
		}
		p.serializer = serializer

		return nil
	})

	if err != nil {
		// Try cleaning up if we had an issue.
		_ = p.runtime.Stop(ctx)
	}
	return err
}

func (p *pythonInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ticket, err := p.runtime.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer func() { _ = p.runtime.Release(ticket) }()

	batch := service.MessageBatch{}
	var objs []py.PyObjectPtr

	err = p.runtime.Apply(ticket, ctx, func() error {
		// Abort if we're cancelling execution.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		next := py.NullPyObjectPtr

		// TODO: add a flush timeout? Right now we fill a batch.
		for idx := 0; idx < p.batchSize; idx++ {
			needsDecref := false

			// Extract the next object to feed into the pipeline.
			switch p.mode {
			case Object:
				if p.idx >= p.boundsHint {
					return nil
				}
				next = p.generator
				p.idx++

			case Iterable:
				next = py.PyIter_Next(p.generator)
				if next == py.NullPyObjectPtr {
					return nil
				}
				needsDecref = true

			case List:
				if p.idx >= p.boundsHint {
					return nil
				}
				next = py.PyList_GetItem(p.generator, p.idx)
				p.idx++
				if next == py.NullPyObjectPtr {
					py.PyErr_Clear()
					panic("out of bounds Python list index")
				}

			case Tuple:
				if p.idx >= p.boundsHint {
					return nil
				}
				next = py.PyTuple_GetItem(p.generator, p.idx)
				p.idx++
				if next == py.NullPyObjectPtr {
					py.PyErr_Clear()
					panic("out of bounds Python tuple index")
				}

			case Callable:
				py.PyErr_Clear()
				next = py.PyObject_Call(p.generator, p.args, p.kwargs)
				if next == py.NullPyObjectPtr {
					py.PyErr_Print()
					panic("null result from calling python input function")
				}
				if py.BaseType(next) == py.None {
					// No more work.
					return nil
				}
				needsDecref = true

			default:
				panic("unhandled input mode")
			}

			// Based on serializer mode, determine how we package up the object
			// before sending our batch.
			var m *service.Message
			switch p.serializerMode {
			case python.None:
				m = service.NewMessage(nil)
				m.SetStructured(next)

				// Track our object and bump a reference as it's now part of
				// the Global interpreter and must outlive this input loop
				// until it is processed by an output.
				objs = append(objs, next)
				py.Py_IncRef(next)
			case python.Bloblang:
				m, err = toBloblang(next, p.serializer)
			case python.Pickle:
				m, err = toPickle(next, p.serializer)
			}
			if err != nil {
				// TODO: drop this message?
				panic(err)
			}

			if m != nil {
				// Tag the message with information on how it was serialized.
				m.MetaSetMut(python.SerializerMetaKey, p.serializerMode)

				// Add it to our batch.
				batch = append(batch, m)
			}

			if needsDecref {
				// Drop any local references we took in the loop.
				py.Py_DecRef(next)
			}
		}

		return nil
	})

	if len(batch) == 0 || err != nil {
		return nil, nil, service.ErrEndOfInput
	}

	// TODO: should we return service.ErrEndOfInput here, too, if we know
	//       that we're finished?
	return batch, func(ctx context.Context, err error) error {
		if err != nil {
			// XXX ??? What happens here?
			p.logger.Errorf("XXX?!?! %v\n", err)
			return err
		}
		return python.DropGlobalReferences(objs, ctx)
	}, nil
}

func (p *pythonInput) Close(ctx context.Context) error {
	_ = p.runtime.Map(ctx, func(ticket *python.InterpreterTicket) error {
		// Even if one of these are null, Py_DecRef is fine being passed NULL.
		py.Py_DecRef(p.generator)
		py.Py_DecRef(p.globals)
		py.Py_DecRef(p.args)
		py.Py_DecRef(p.kwargs)
		p.serializer.DecRef()

		return nil
	})

	return p.runtime.Stop(ctx)
}

func toBloblang(obj py.PyObjectPtr, serializer *python.Serializer) (*service.Message, error) {
	var m *service.Message
	switch py.BaseType(obj) {
	case py.None:
		return nil, nil

	case py.Long:
		// TODO: overflow (signed vs. unsigned)
		long := py.PyLong_AsLong(obj)
		m = service.NewMessage([]byte{})
		m.SetStructured(long)

	case py.Float:
		float := py.PyFloat_AsDouble(obj)
		m = service.NewMessage([]byte{})
		m.SetStructured(float)

	case py.String:
		s, decodeErr := py.UnicodeToString(obj)
		if decodeErr != nil {
			panic("failed to decode python input string")
		}
		m = service.NewMessage([]byte(s))

	case py.Bytes:
		// Copy out the bytes.
		bytes := py.PyBytes_AsString(obj)
		sz := py.PyBytes_Size(obj)
		buffer := make([]byte, sz)
		copy(buffer, unsafe.Slice(bytes, sz))
		m = service.NewMessage(buffer)

	case py.Tuple, py.List, py.Dict, py.Unknown:
		// Use the serializer.
		buffer, err := serializer.JsonBytes(obj)

		if err != nil {
			panic(err)
		}
		m = service.NewMessage(buffer)
	}

	return m, nil
}

func toPickle(obj py.PyObjectPtr, serializer *python.Serializer) (*service.Message, error) {
	pickled, err := serializer.Pickle(obj)
	if err != nil {
		return nil, err
	}

	return service.NewMessage(pickled), nil
}
