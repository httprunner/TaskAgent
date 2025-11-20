package pool

import (
	"strings"

	gadb "github.com/httprunner/httprunner/v5/pkg/gadb"
)

// fetchDeviceMeta collects OS version and root status via adb where possible.
func (a *DevicePoolAgent) fetchDeviceMeta(serial string) deviceMeta {
	meta := deviceMeta{providerUUID: a.hostUUID}
	meta.osType = strings.TrimSpace(a.cfg.OSType)
	if meta.osType == "" {
		meta.osType = "android"
	}
	dev, err := a.findDevice(serial)
	if err != nil || dev == nil {
		return meta
	}

	if output, err := dev.RunShellCommand("getprop", "ro.build.version.release"); err == nil {
		meta.osVersion = strings.TrimSpace(output)
	}

	if output, err := dev.RunShellCommand("su", "-c", "id"); err == nil {
		if strings.Contains(output, "uid=0") {
			meta.isRoot = "true"
			return meta
		}
	}
	if output, err := dev.RunShellCommand("which", "su"); err == nil {
		if strings.TrimSpace(output) != "" {
			meta.isRoot = "true"
			return meta
		}
	}
	meta.isRoot = "false"
	return meta
}

func (a *DevicePoolAgent) ensureADBClient() (gadb.Client, error) {
	if a.adbReady {
		return a.adbClient, nil
	}
	client, err := gadb.NewClient()
	if err != nil {
		return gadb.Client{}, err
	}
	a.adbClient = client
	a.adbReady = true
	return client, nil
}

func (a *DevicePoolAgent) findDevice(serial string) (*gadb.Device, error) {
	client, err := a.ensureADBClient()
	if err != nil {
		return nil, err
	}
	devs, err := client.DeviceList()
	if err != nil {
		return nil, err
	}
	for _, d := range devs {
		if d != nil && strings.TrimSpace(d.Serial()) == strings.TrimSpace(serial) {
			return d, nil
		}
	}
	return nil, nil
}
