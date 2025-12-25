package taskagent

import (
	"fmt"
	"strings"
)

// FormatSQLForLog interpolates positional parameters into a SQL query string for logging only.
func FormatSQLForLog(query string, args ...any) string {
	if strings.TrimSpace(query) == "" || len(args) == 0 {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + len(args)*8)
	argIdx := 0
	for _, ch := range query {
		if ch == '?' && argIdx < len(args) {
			b.WriteString(FormatSQLArg(args[argIdx]))
			argIdx++
			continue
		}
		b.WriteRune(ch)
	}
	if argIdx < len(args) {
		b.WriteString(" /* args:")
		for i := argIdx; i < len(args); i++ {
			if i > argIdx {
				b.WriteString(",")
			}
			b.WriteString(" ")
			b.WriteString(FormatSQLArg(args[i]))
		}
		b.WriteString(" */")
	}
	return b.String()
}

// FormatSQLArg formats a SQL argument for logging only.
func FormatSQLArg(arg any) string {
	if arg == nil {
		return "NULL"
	}
	switch v := arg.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	case fmt.Stringer:
		return "'" + strings.ReplaceAll(v.String(), "'", "''") + "'"
	default:
		return fmt.Sprintf("%v", arg)
	}
}
