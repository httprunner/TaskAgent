package main

import "strings"

func firstNonEmpty(values ...string) string {
	for _, val := range values {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
