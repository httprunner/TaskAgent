package taskagent

import "strings"

const (
	// EnvDeviceAllowlist optionally restricts the local device pool to a subset of device serials.
	// The value can be a comma/semicolon/whitespace-separated list, for example:
	//   DEVICE_ALLOWLIST="device-A,device-B"
	//   DEVICE_ALLOWLIST="device-A device-B"
	EnvDeviceAllowlist = "DEVICE_ALLOWLIST"
)

func parseDeviceAllowlist(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		switch r {
		case ',', ';', '\n', '\r', '\t', ' ', '|':
			return true
		default:
			return false
		}
	})
	return normalizeDeviceAllowlist(parts)
}

func normalizeDeviceAllowlist(serials []string) []string {
	if len(serials) == 0 {
		return nil
	}
	out := make([]string, 0, len(serials))
	seen := make(map[string]struct{}, len(serials))
	for _, serial := range serials {
		trimmed := strings.TrimSpace(serial)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func buildDeviceAllowlistSet(serials []string) map[string]struct{} {
	serials = normalizeDeviceAllowlist(serials)
	if len(serials) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(serials))
	for _, serial := range serials {
		if strings.TrimSpace(serial) == "" {
			continue
		}
		set[serial] = struct{}{}
	}
	return set
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for k := range in {
		trimmed := strings.TrimSpace(k)
		if trimmed == "" {
			continue
		}
		out[trimmed] = struct{}{}
	}
	return out
}
