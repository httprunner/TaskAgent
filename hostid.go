package pool

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// getHostUUID returns a best-effort hardware UUID for the host to populate ProviderUUID.
// On macOS it uses `system_profiler`; on Linux it prefers /etc/machine-id then falls back to /sys/class/dmi/id/product_uuid.
func getHostUUID() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		cmd := exec.CommandContext(context.Background(), "bash", "-c", "system_profiler SPHardwareDataType | awk '/Hardware UUID/ {print $3}'")
		out, err := cmd.Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	case "linux":
		if id, err := readSystemFile("/etc/machine-id"); err == nil && id != "" {
			return id, nil
		}
		if id, err := readSystemFile("/sys/class/dmi/id/product_uuid"); err == nil && id != "" {
			return id, nil
		}
		return "", nil
	default:
		return "", nil
	}
}

func readSystemFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
