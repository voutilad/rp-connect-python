package python

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	py "github.com/voutilad/gogopython"
)

type callbackFunc func(self, args py.PyObjectPtr) py.PyObjectPtr

type Callback struct {
	Name       string
	Definition *py.PyMethodDef
	Object     py.PyObjectPtr
}

func NewCallback(name string, f callbackFunc) (*Callback, error) {
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
	return &Callback{Name: name, Definition: &def, Object: fn}, nil
}
