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
	List                      // A Python List.
	Tuple                     // A Python Tuple.
	Object                    // A single Python Object.
)

type pythonInput struct {
	logger        *service.Logger
	runtime       python.Runtime
	generator     py.PyObjectPtr
	mode          inputMode
	globals       py.PyObjectPtr
	code          py.PyCodeObjectPtr
	serializer    *python.Serializer
	args          py.PyObjectPtr
	kwargs        py.PyObjectPtr
	pickle        bool
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
	Field(service.NewBoolField("pickle").
		Description("Whether to serialize data with pickle.").
		Default(false)).
	Field(service.NewIntField("batch_size").
		Description("Size of batches to generate.").
		Default(1)).
	Field(service.NewStringField("mode").
		Description("Toggle different Python runtime modes: 'multi', 'single', and 'legacy' (the default)").
		Default(string(python.LegacyMode)))

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
			modeString, err := conf.FieldString("mode")
			if err != nil {
				return nil, err
			}
			name, err := conf.FieldString("name")
			if err != nil {
				return nil, err
			}
			pickle, err := conf.FieldBool("pickle")
			if err != nil {
				return nil, err
			}
			batchSize, err := conf.FieldInt("batch_size")
			if err != nil {
				return nil, err
			}
			return newPythonInput(exe, script, name, batchSize, pickle, python.StringAsMode(modeString), mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

func newPythonInput(exe, script, name string, batchSize int, pickle bool, mode python.Mode, logger *service.Logger) (service.BatchInput, error) {
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
		pickle:        pickle,
		batchSize:     batchSize,
		boundsHint:    -1,
	}, nil
}

func (p *pythonInput) Connect(ctx context.Context) error {
	err := p.runtime.Start(ctx)
	if err != nil {
		return err
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
		case py.List:
			p.mode = List
			p.boundsHint = py.PyList_Size(obj)
		case py.Tuple:
			p.mode = Tuple
			p.boundsHint = py.PyTuple_Size(obj)
		case py.Function:
			p.mode = Callable
		default:
			p.mode = Object
			p.boundsHint = 1
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

	batch := make(service.MessageBatch, p.batchSize)
	idx := 0

	err = p.runtime.Apply(ticket, ctx, func() error {
		next := py.NullPyObjectPtr

		// TODO: add a flush timeout? Right now we fill a batch.
		for ; idx < p.batchSize; idx++ {
			needsDecref := false

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

			switch py.BaseType(next) {
			case py.None:
				return nil

			case py.Long:
				// TODO: overflow (signed vs. unsigned)
				long := py.PyLong_AsLong(next)
				m := service.NewMessage([]byte{})
				m.SetStructured(long)
				batch[idx] = m

			case py.Float:
				float := py.PyFloat_AsDouble(next)
				m := service.NewMessage([]byte{})
				m.SetStructured(float)
				batch[idx] = m

			case py.String:
				s, decodeErr := py.UnicodeToString(next)
				if decodeErr != nil {
					panic("failed to decode python input string")
				}
				batch[idx] = service.NewMessage([]byte(s))

			case py.Bytes:
				// Copy out the bytes.
				bytes := py.PyBytes_AsString(next)
				sz := py.PyBytes_Size(next)
				buffer := make([]byte, sz)
				copy(buffer, unsafe.Slice(bytes, sz))
				batch[idx] = service.NewMessage(buffer)

			case py.Tuple, py.List, py.Dict, py.Unknown:
				// Use the serializer.
				var buffer []byte
				if p.pickle {
					buffer, err = p.serializer.Pickle(next)
				} else {
					buffer, err = p.serializer.JsonBytes(next)
				}
				if err != nil {
					panic(err)
				}
				batch[idx] = service.NewMessage(buffer)
			}

			if needsDecref {
				py.Py_DecRef(next)
			}
		}

		return nil
	})

	if idx == 0 || err != nil {
		return nil, nil, service.ErrEndOfInput
	}

	// TODO: should we return service.ErrEndOfInput here, too, if we know
	//       that we're finished?
	return batch[0:idx], noOpAckFn, nil
}

func (p *pythonInput) Close(ctx context.Context) error {
	_ = p.runtime.Map(ctx, func(_ *python.InterpreterTicket) error {
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
