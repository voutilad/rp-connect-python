package python

import "strings"

type Mode string

const (
	MultiMode   Mode = "multi"
	SingleMode  Mode = "single"
	LegacyMode  Mode = "legacy"
	InvalidMode Mode = "invalid"
)

func StringAsMode(s string) Mode {
	switch strings.ToLower(s) {
	case string(MultiMode):
		return MultiMode
	case string(SingleMode):
		return SingleMode
	case string(LegacyMode):
		return LegacyMode
	default:
		return InvalidMode
	}
}
