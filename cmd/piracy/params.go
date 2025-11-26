package main

import "strings"

// getParams parses comma-separated params from CLI.
func getParams(cliParams string) []string {
	if strings.TrimSpace(cliParams) == "" {
		return nil
	}
	parts := strings.Split(cliParams, ",")
	params := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			params = append(params, trimmed)
		}
	}
	return params
}
