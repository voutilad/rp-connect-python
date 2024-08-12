package processor

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
)

type callbackFunc func(self, args py.PyObjectPtr) py.PyObjectPtr

type callback struct {
	Name       string
	Definition *py.PyMethodDef
	Object     py.PyObjectPtr
}

func newCallback(name string, f callbackFunc) (*callback, error) {
	// TODO: push this into gogopython
	def := py.PyMethodDef{
		Name:   unsafe.StringData(name),
		Flags:  py.MethodVarArgs,
		Method: purego.NewCallback(f),
	}
	fn := py.PyCFunction_NewEx(&def, py.NullPyObjectPtr, py.NullPyObjectPtr)
	if fn == py.NullPyObjectPtr {
		return nil, errors.New("failed to create python function")
	}
	return &callback{Name: name, Definition: &def, Object: fn}, nil
}

// contentCallback is called from Python and copies the underlying bytes of
// a service.Message into Python. It has a Python function definition like:
//
// def __content(__msg)
//
//	where __msg is the virtual address of the service.Message.
func contentCallback(_, tuple py.PyObjectPtr) py.PyObjectPtr {
	if py.BaseType(tuple) != py.Tuple {
		panic("argument should be a Python tuple")
	}

	// First argument is a pointer to our service.Message.
	var m *service.Message
	addr := py.PyTuple_GetItem(tuple, 0)
	if addr == py.NullPyObjectPtr {
		panic("first tuple item should not be null")
	}
	m = (*service.Message)(unsafe.Pointer(uintptr(py.PyLong_AsUnsignedLong(addr))))

	// Create a Python bytes object and return it.
	data, err := m.AsBytes()
	if err != nil {
		// TODO: return None instead of empty bytes?
		return py.PyBytes_FromStringAndSize(nil, 0)
	}

	bytes := py.PyBytes_FromStringAndSize(unsafe.SliceData(data), int64(len(data)))
	if bytes == py.NullPyObjectPtr {
		// In order of the callback to return nil, we need to set an exception,
		// but we don't have those hooks in gogopython yet.
		panic("failed to create Python bytes")
	}
	return bytes
}

// metadataCallback is called from Python and has a Python function definition
// that looks like:
//
// def __metadata(__msg -> int, key = "")
//
// where __msg is the virtual address of the service.Message and key is a
// string containing the key of the metadata item to retrieve.
func metadataCallback(_, tuple py.PyObjectPtr) py.PyObjectPtr {
	if py.BaseType(tuple) != py.Tuple {
		panic("argument should be a Python tuple")
	}

	// First argument is a pointer to our service.Message.
	var m *service.Message
	addr := py.PyTuple_GetItem(tuple, 0)
	if addr == py.NullPyObjectPtr {
		panic("first tuple item should not be null")
	}
	m = (*service.Message)(unsafe.Pointer(uintptr(py.PyLong_AsUnsignedLong(addr))))

	// Second argument is an optional key. Empty string denotes "all keys".
	str := py.PyTuple_GetItem(tuple, 1)
	key, err := py.UnicodeToString(str)
	if err != nil {
		// TODO: raise Python exception
		panic(err)
	}

	// Now we copy the value (if any) into Python.
	val, ok := m.MetaGetMut(key)
	if !ok {
		// No such metadata.
		// TODO: return None
		return py.PyUnicode_FromString("")
	}

	switch val := val.(type) {
	case string:
		return py.PyUnicode_FromString(val)
	case int:
		return py.PyLong_FromLong(int64(val))
	case uint:
		return py.PyLong_FromUnsignedLong(uint64(val))
	case bool:
		// XXX this is ugly...seriously? I really don't like Go. :P
		var i int64
		if val {
			i = 1
		} else {
			i = 0
		}
		return py.PyBool_FromLong(i)
	default:
		// TODO: catch more types in the switch.
		// XXX for now, we bail out to a string.
		s, _ := m.MetaGet(key) // always true
		return py.PyUnicode_FromString(s)
	}
}
