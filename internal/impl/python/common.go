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
