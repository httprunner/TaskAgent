package feishu

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Environment override keys for device info table.
const (
	EnvDeviceInfoURL               = "DEVICE_INFO_BITABLE_URL"
	EnvDeviceInfoFieldSerial       = "DEVICE_INFO_FIELD_SERIAL"
	EnvDeviceInfoFieldOSType       = "DEVICE_INFO_FIELD_OSTYPE"
	EnvDeviceInfoFieldOSVersion    = "DEVICE_INFO_FIELD_OSVERSION"
	EnvDeviceInfoFieldIPLocation   = "DEVICE_INFO_FIELD_IP_LOCATION"
	EnvDeviceInfoFieldIsRoot       = "DEVICE_INFO_FIELD_ISROOT"
	EnvDeviceInfoFieldProviderUUID = "DEVICE_INFO_FIELD_PROVIDERUUID"
	EnvDeviceInfoFieldAgentVersion = "DEVICE_INFO_FIELD_AGENT_VERSION"
	EnvDeviceInfoFieldStatus       = "DEVICE_INFO_FIELD_STATUS"
	EnvDeviceInfoFieldLastSeenAt   = "DEVICE_INFO_FIELD_LAST_SEEN_AT"
	EnvDeviceInfoFieldLastError    = "DEVICE_INFO_FIELD_LAST_ERROR"
	EnvDeviceInfoFieldTags         = "DEVICE_INFO_FIELD_TAGS"
	EnvDeviceInfoFieldRunningTask  = "DEVICE_INFO_FIELD_RUNNING_TASK"
	EnvDeviceInfoFieldPendingTasks = "DEVICE_INFO_FIELD_PENDING_TASKS"
)

// DeviceInfoFields lists column names for the device inventory table.
type DeviceInfoFields struct {
	DeviceSerial string
	OSType       string
	OSVersion    string
	IPLocation   string
	IsRoot       string
	ProviderUUID string
	AgentVersion string
	Status       string
	LastSeenAt   string
	LastError    string
	Tags         string
	RunningTask  string
	PendingTasks string
}

// DefaultDeviceInfoFields provides sensible defaults matching the design doc.
var DefaultDeviceInfoFields = DeviceInfoFields{
	DeviceSerial: "DeviceSerial",
	OSType:       "OSType",
	OSVersion:    "OSVersion",
	IPLocation:   "IPLocation",
	IsRoot:       "IsRoot",
	ProviderUUID: "ProviderUUID",
	AgentVersion: "AgentVersion",
	Status:       "Status",
	LastSeenAt:   "LastSeenAt",
	LastError:    "LastError",
	Tags:         "Tags",
	RunningTask:  "RunningTask",
	PendingTasks: "PendingTasks",
}

// DeviceInfoRecordInput describes the payload used to create or update a device row.
type DeviceInfoRecordInput struct {
	DeviceSerial string
	OSType       string
	OSVersion    string
	IPLocation   string
	IsRoot       string
	ProviderUUID string
	AgentVersion string
	Status       string
	LastSeenAt   *time.Time
	LastError    string
	Tags         string
	RunningTask  string
	PendingTasks []string
}

// DeviceInfoFieldsFromEnv builds fields with environment overrides.
func DeviceInfoFieldsFromEnv() DeviceInfoFields {
	f := DefaultDeviceInfoFields
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldSerial)); v != "" {
		f.DeviceSerial = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldOSType)); v != "" {
		f.OSType = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldOSVersion)); v != "" {
		f.OSVersion = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldIPLocation)); v != "" {
		f.IPLocation = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldIsRoot)); v != "" {
		f.IsRoot = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldProviderUUID)); v != "" {
		f.ProviderUUID = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldAgentVersion)); v != "" {
		f.AgentVersion = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldStatus)); v != "" {
		f.Status = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldLastSeenAt)); v != "" {
		f.LastSeenAt = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldLastError)); v != "" {
		f.LastError = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldTags)); v != "" {
		f.Tags = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldRunningTask)); v != "" {
		f.RunningTask = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldPendingTasks)); v != "" {
		f.PendingTasks = v
	}
	return f
}

// DeviceInfoTable caches decoded rows for quick lookup.
type DeviceInfoTable struct {
	Ref    BitableRef
	Fields DeviceInfoFields
	Rows   []DeviceInfoRecordInput
	index  map[string]string // DeviceSerial -> RecordID
}

// RecordIDBySerial returns the record id for a given device serial.
func (t *DeviceInfoTable) RecordIDBySerial(serial string) string {
	if t == nil {
		return ""
	}
	if t.index == nil {
		return ""
	}
	return t.index[strings.TrimSpace(serial)]
}

// FetchDeviceInfoTable downloads the device info table.
func (c *Client) FetchDeviceInfoTable(ctx context.Context, rawURL string, override *DeviceInfoFields) (*DeviceInfoTable, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return nil, err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return nil, err
	}
	fields := DefaultDeviceInfoFields
	if override != nil {
		fields = fields.merge(*override)
	}
	records, err := c.listBitableRecords(ctx, ref, defaultBitablePageSize, nil)
	if err != nil {
		return nil, err
	}
	table := &DeviceInfoTable{
		Ref:    ref,
		Fields: fields,
		Rows:   make([]DeviceInfoRecordInput, 0, len(records)),
		index:  make(map[string]string, len(records)),
	}
	for _, rec := range records {
		serial := toString(rec.Fields[fields.DeviceSerial])
		if serial == "" {
			continue
		}
		row := DeviceInfoRecordInput{
			DeviceSerial: serial,
			OSType:       toString(rec.Fields[fields.OSType]),
			OSVersion:    toString(rec.Fields[fields.OSVersion]),
			IPLocation:   toString(rec.Fields[fields.IPLocation]),
			IsRoot:       toString(rec.Fields[fields.IsRoot]),
			ProviderUUID: toString(rec.Fields[fields.ProviderUUID]),
			AgentVersion: toString(rec.Fields[fields.AgentVersion]),
			Status:       toString(rec.Fields[fields.Status]),
			LastError:    toString(rec.Fields[fields.LastError]),
			Tags:         toString(rec.Fields[fields.Tags]),
			RunningTask:  toString(rec.Fields[fields.RunningTask]),
		}
		if v := rec.Fields[fields.PendingTasks]; v != nil {
			switch list := v.(type) {
			case []any:
				for _, item := range list {
					if s := toString(item); strings.TrimSpace(s) != "" {
						row.PendingTasks = append(row.PendingTasks, s)
					}
				}
			case string:
				if strings.TrimSpace(list) != "" {
					row.PendingTasks = append(row.PendingTasks, list)
				}
			}
		}
		if ts := toTime(rec.Fields[fields.LastSeenAt]); ts != nil {
			row.LastSeenAt = ts
		}
		table.Rows = append(table.Rows, row)
		table.index[serial] = rec.RecordID
	}
	return table, nil
}

// UpsertDeviceInfo creates or updates a device row keyed by DeviceSerial.
func (c *Client) UpsertDeviceInfo(ctx context.Context, rawURL string, fields DeviceInfoFields, rec DeviceInfoRecordInput) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(rawURL) == "" {
		return errors.New("feishu: device info table url is empty")
	}
	table, err := c.FetchDeviceInfoTable(ctx, rawURL, &fields)
	if err != nil {
		return err
	}
	payload, err := buildDeviceInfoPayload(rec, table.Fields)
	if err != nil {
		return err
	}
	recordID := table.RecordIDBySerial(strings.TrimSpace(rec.DeviceSerial))
	if recordID == "" {
		_, err = c.createBitableRecord(ctx, table.Ref, payload)
		return err
	}
	return c.updateBitableRecord(ctx, table.Ref, recordID, payload)
}

func (fields DeviceInfoFields) merge(override DeviceInfoFields) DeviceInfoFields {
	result := fields
	if strings.TrimSpace(override.DeviceSerial) != "" {
		result.DeviceSerial = override.DeviceSerial
	}
	if strings.TrimSpace(override.OSType) != "" {
		result.OSType = override.OSType
	}
	if strings.TrimSpace(override.OSVersion) != "" {
		result.OSVersion = override.OSVersion
	}
	if strings.TrimSpace(override.IPLocation) != "" {
		result.IPLocation = override.IPLocation
	}
	if strings.TrimSpace(override.IsRoot) != "" {
		result.IsRoot = override.IsRoot
	}
	if strings.TrimSpace(override.ProviderUUID) != "" {
		result.ProviderUUID = override.ProviderUUID
	}
	if strings.TrimSpace(override.AgentVersion) != "" {
		result.AgentVersion = override.AgentVersion
	}
	if strings.TrimSpace(override.Status) != "" {
		result.Status = override.Status
	}
	if strings.TrimSpace(override.LastSeenAt) != "" {
		result.LastSeenAt = override.LastSeenAt
	}
	if strings.TrimSpace(override.LastError) != "" {
		result.LastError = override.LastError
	}
	if strings.TrimSpace(override.Tags) != "" {
		result.Tags = override.Tags
	}
	if strings.TrimSpace(override.RunningTask) != "" {
		result.RunningTask = override.RunningTask
	}
	if strings.TrimSpace(override.PendingTasks) != "" {
		result.PendingTasks = override.PendingTasks
	}
	return result
}

func buildDeviceInfoPayload(rec DeviceInfoRecordInput, fields DeviceInfoFields) (map[string]any, error) {
	row := make(map[string]any)
	addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
	addOptionalField(row, fields.OSType, rec.OSType)
	addOptionalField(row, fields.OSVersion, rec.OSVersion)
	addOptionalField(row, fields.IPLocation, rec.IPLocation)
	addOptionalField(row, fields.IsRoot, rec.IsRoot)
	addOptionalField(row, fields.ProviderUUID, rec.ProviderUUID)
	addOptionalField(row, fields.AgentVersion, rec.AgentVersion)
	addOptionalField(row, fields.Status, rec.Status)
	addOptionalField(row, fields.LastError, rec.LastError)
	addOptionalField(row, fields.Tags, rec.Tags)
	addOptionalField(row, fields.RunningTask, rec.RunningTask)
	if len(rec.PendingTasks) > 0 && strings.TrimSpace(fields.PendingTasks) != "" {
		row[fields.PendingTasks] = rec.PendingTasks
	}
	if rec.LastSeenAt != nil {
		if strings.TrimSpace(fields.LastSeenAt) == "" {
			return nil, fmt.Errorf("feishu: LastSeenAt field not configured")
		}
		row[fields.LastSeenAt] = rec.LastSeenAt.UTC().UnixMilli()
	}
	if len(row) == 0 {
		return nil, errors.New("feishu: device info payload is empty")
	}
	return row, nil
}

// Helpers to decode primitive values from Feishu record fields.
func toTime(val any) *time.Time {
	switch v := val.(type) {
	case float64:
		ts := int64(v)
		if ts == 0 {
			return nil
		}
		t := time.UnixMilli(ts)
		return &t
	case int64:
		t := time.UnixMilli(v)
		return &t
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		// try parse common layout
		if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return &parsed
		}
	}
	return nil
}
