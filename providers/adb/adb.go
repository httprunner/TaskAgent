package adb

import (
	"context"

	gadb "github.com/httprunner/httprunner/v5/pkg/gadb"
)

// Provider implements pool.DeviceProvider using gadb.
type Provider struct {
	client gadb.Client
}

// New creates a Provider backed by the given gadb client.
func New(client gadb.Client) *Provider {
	return &Provider{client: client}
}

// ListDevices returns all available device serials from adb.
func (p *Provider) ListDevices(ctx context.Context) ([]string, error) {
	return p.client.DeviceSerialList()
}
