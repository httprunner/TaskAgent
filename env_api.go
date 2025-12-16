package taskagent

import "github.com/httprunner/TaskAgent/internal/env"

// EnvString reads an environment variable with a fallback default. It is a
// thin wrapper over internal/env so downstream code can avoid importing
// internal packages directly.
func EnvString(key, defaultValue string) string {
	return env.String(key, defaultValue)
}
