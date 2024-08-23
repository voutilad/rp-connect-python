package python

import (
	_ "embed"
	"errors"
	"fmt"
	py "github.com/voutilad/gogopython"
	"unsafe"
)

//go:embed serializer.py
var source string

const (
	toJsonString = "to_json_string"
	toJsonBytes  = "to_json_bytes"
	toPickle     = "to_pickle"
)

const null = py.NullPyObjectPtr

type Serializer struct {
	code       py.PyCodeObjectPtr
	module     py.PyObjectPtr
	jsonString py.PyObjectPtr
	jsonBytes  py.PyObjectPtr
	pickle     py.PyObjectPtr
}

// NewSerializer attempts to compile, import, and prepare a set of Python
// objects to assist serialization.
//
// The caller must manage the interpreter state for this to succeed.
func NewSerializer() (*Serializer, error) {
	code := py.Py_CompileString(source, "__serializer__.py", py.PyFileInput)
	if code == py.NullPyCodeObjectPtr {
		return nil, errors.New("failed to compile serializer source")
	}
	module := py.PyImport_ExecCodeModule("__serializer__", code)
	if module == py.NullPyObjectPtr {
		return nil, errors.New("failed to import serializer module")
	}
	jsonString := py.PyObject_GetAttrString(module, toJsonString)
	if jsonString == py.NullPyObjectPtr {
		return nil, fmt.Errorf("failed to find %s in serializer module", toJsonString)
	}
	jsonBytes := py.PyObject_GetAttrString(module, toJsonBytes)
	if jsonBytes == py.NullPyObjectPtr {
		return nil, fmt.Errorf("failed to find %s in serializer module", toJsonBytes)
	}
	pickle := py.PyObject_GetAttrString(module, toPickle)
	if pickle == py.NullPyObjectPtr {
		return nil, fmt.Errorf("failed to find %s in serializer module", toPickle)
	}

	return &Serializer{
		code:       code,
		module:     module,
		jsonString: jsonString,
		jsonBytes:  jsonBytes,
		pickle:     pickle,
	}, nil
}

func (s *Serializer) call(fn, obj py.PyObjectPtr) (py.PyObjectPtr, error) {
	result := py.PyObject_CallOneArg(fn, obj)
	if result == py.NullPyObjectPtr {
		py.PyErr_Print()
		return null, errors.New("failed to serialize python object")
	}
	return result, nil
}

// JsonString serializes the given Python object to JSON.
func (s *Serializer) JsonString(obj py.PyObjectPtr) (string, error) {
	result, err := s.call(s.jsonString, obj)
	if err != nil {
		return "", err
	}
	defer py.Py_DecRef(result)

	str, err := py.UnicodeToString(result)
	if err != nil {
		return "", err
	}
	return str, nil
}

// JsonBytes serializes the given Python object to JSON, encoded to utf-8 bytes.
func (s *Serializer) JsonBytes(obj py.PyObjectPtr) ([]byte, error) {
	result, err := s.call(s.jsonBytes, obj)
	if err != nil {
		return nil, err
	}
	defer py.Py_DecRef(result)

	// Before copying out, we need the length to do some unsafe voodoo.
	sz := py.PyBytes_Size(result)
	rawBytes := py.PyBytes_AsString(result)

	buffer := make([]byte, sz)
	copy(buffer, unsafe.Slice(rawBytes, sz))

	return buffer, nil
}

// Pickle the given Python object.
func (s *Serializer) Pickle(obj py.PyObjectPtr) ([]byte, error) {
	result, err := s.call(s.pickle, obj)
	if err != nil {
		return nil, err
	}
	defer py.Py_DecRef(result)

	// Before copying out, we need the length to do some unsafe voodoo.
	sz := py.PyBytes_Size(result)
	rawBytes := py.PyBytes_AsString(result)

	buffer := make([]byte, sz)
	copy(buffer, unsafe.Slice(rawBytes, sz))

	return buffer, nil
}

func (s *Serializer) DecRef() {
	py.Py_DecRef(s.pickle)
	py.Py_DecRef(s.jsonBytes)
	py.Py_DecRef(s.jsonString)
	py.Py_DecRef(s.module)
	// code objects don't need Py_DecRef.
}
