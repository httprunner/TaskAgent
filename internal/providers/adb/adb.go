package adb

import (
	"context"
	"strings"

	"github.com/httprunner/httprunner/v5/pkg/gadb"
	"github.com/pkg/errors"
)

// Provider implements DeviceStateProvider using gadb.
type Provider struct {
	client gadb.Client
}

// New creates a Provider backed by the given gadb client.
func New(client gadb.Client) *Provider {
	return &Provider{client: client}
}

// NewDefault creates a Provider using a default gadb client.
func NewDefault() (*Provider, error) {
	client, err := gadb.NewClient()
	if err != nil {
		return nil, errors.Wrap(err, "init adb client for provider")
	}
	return New(client), nil
}

// ListDevices returns all available device serials from adb.
func (p *Provider) ListDevices(ctx context.Context) ([]string, error) {
	return p.client.DeviceSerialList()
}

// ListDevicesWithState returns device serials with their raw gadb state names.
func (p *Provider) ListDevicesWithState(ctx context.Context) (map[string]string, error) {
	if p == nil {
		return nil, errors.New("adb provider is nil")
	}
	devs, err := p.client.DeviceList()
	if err != nil {
		return nil, errors.Wrap(err, "list adb devices")
	}
	stateBySerial := make(map[string]string, len(devs))
	for _, dev := range devs {
		if dev == nil {
			continue
		}
		serial := strings.TrimSpace(dev.Serial())
		if serial == "" {
			continue
		}
		state, err := dev.State()
		if err != nil {
			stateBySerial[serial] = string(gadb.StateUnknown)
			continue
		}
		stateBySerial[serial] = string(state)
	}
	return stateBySerial, nil
}

// RunShell executes a shell command on the given device serial.
func (p *Provider) RunShell(serial string, args ...string) (string, error) {
	if p == nil {
		return "", errors.New("adb provider is nil")
	}
	if len(args) == 0 {
		return "", errors.New("adb provider: empty shell command")
	}
	devs, err := p.client.DeviceList()
	if err != nil {
		return "", errors.Wrap(err, "list adb devices")
	}
	target := strings.TrimSpace(serial)
	for _, d := range devs {
		if d == nil {
			continue
		}
		if strings.TrimSpace(d.Serial()) == target {
			return d.RunShellCommand(args[0], args[1:]...)
		}
	}
	return "", errors.Errorf("device %s not found", serial)
}
