package config

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	envload "github.com/httprunner/TaskAgent/internal"
)

var ensureOnce sync.Once

func ensureEnvLoaded() {
	ensureOnce.Do(func() {
		envload.Ensure()
	})
}

// String returns the trimmed environment variable or fallback when unset.
func String(key, fallback string) string {
	ensureEnvLoaded()
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

// Duration parses a time duration from environment or returns fallback.
func Duration(key string, fallback time.Duration) time.Duration {
	ensureEnvLoaded()
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if parsed, err := time.ParseDuration(val); err == nil {
			return parsed
		}
	}
	return fallback
}

// Int returns an integer environment variable or fallback when invalid.
func Int(key string, fallback int) int {
	ensureEnvLoaded()
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return fallback
}

// Bool parses a boolean environment variable.
func Bool(key string, fallback bool) bool {
	ensureEnvLoaded()
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		lower := strings.ToLower(val)
		if lower == "1" || lower == "true" || lower == "yes" {
			return true
		}
		if lower == "0" || lower == "false" || lower == "no" {
			return false
		}
	}
	return fallback
}
