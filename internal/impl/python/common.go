package python

import "strings"

type Mode string

const (
	Isolated       Mode = "isolated"
	Global         Mode = "global"
	IsolatedLegacy Mode = "isolated_legacy"
	InvalidMode    Mode = "invalid"
)

func StringAsMode(s string) Mode {
	switch strings.ToLower(s) {
	case string(Isolated):
		return Isolated
	case string(Global):
		return Global
	case string(IsolatedLegacy):
		return IsolatedLegacy
	default:
		return InvalidMode
	}
}

type SerializerMode string

const (
	// Pickle SerializerMode will pickle any Python results.
	Pickle SerializerMode = "pickle"

	// Bloblang SerializerMode will approximate JSON serialization used by Bloblang.
	Bloblang SerializerMode = "bloblang"

	// None SerializerMode will not attempt serialization and simply pass Python object pointers.
	None SerializerMode = "none"

	InvalidSerializer SerializerMode = "invalid"
)

func StringAsSerializerMode(s string) SerializerMode {
	switch strings.ToLower(s) {
	case string(Pickle):
		return Pickle
	case string(Bloblang):
		return Bloblang
	case string(None):
		return None
	default:
		return InvalidSerializer
	}
}

const PythonSerializerMetaKey = "_python_serializer"
