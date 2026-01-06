package taskagent

import (
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
)

// EnvString reads an environment variable with a fallback default. It is a
// thin wrapper over internal/env so downstream code can avoid importing
// internal packages directly.
func EnvString(key, defaultValue string) string {
	return env.String(key, defaultValue)
}

// EnvBool parses an environment variable as a boolean.
func EnvBool(key string, defaultValue bool) bool {
	return env.Bool(key, defaultValue)
}

// EnvInt parses an environment variable as an integer.
func EnvInt(key string, defaultValue int) int {
	return env.Int(key, defaultValue)
}

// EnvDuration parses an environment variable as time.Duration.
func EnvDuration(key string, defaultValue time.Duration) time.Duration {
	return env.Duration(key, defaultValue)
}
