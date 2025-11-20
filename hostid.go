package pool

import (
	"context"
	"os/exec"
	"runtime"
	"strings"
)

// getHostUUID returns a best-effort hardware UUID for the host to populate ProviderUUID.
// On macOS it uses `system_profiler`; on Linux it reads /sys/class/dmi/id/product_uuid.
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
		cmd := exec.CommandContext(context.Background(), "cat", "/sys/class/dmi/id/product_uuid")
		out, err := cmd.Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	default:
		return "", nil
	}
}
