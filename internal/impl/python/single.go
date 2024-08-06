package python

import (
	py "github.com/voutilad/gogopython"
	"sync"
)

type SingleInterpreterRuntime struct {
	exe    string
	home   string
	paths  []string
	mtx    sync.Mutex
	thread py.PyThreadStatePtr
}
