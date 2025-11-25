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
	EnvDeviceFieldSerial       = "DEVICE_FIELD_SERIAL"
	EnvDeviceFieldOSType       = "DEVICE_FIELD_OSTYPE"
	EnvDeviceFieldOSVersion    = "DEVICE_FIELD_OSVERSION"
	EnvDeviceFieldIPLocation   = "DEVICE_FIELD_IP_LOCATION"
	EnvDeviceFieldIsRoot       = "DEVICE_FIELD_ISROOT"
	EnvDeviceFieldProviderUUID = "DEVICE_FIELD_PROVIDERUUID"
	EnvDeviceFieldAgentVersion = "DEVICE_FIELD_AGENT_VERSION"
	EnvDeviceFieldStatus       = "DEVICE_FIELD_STATUS"
	EnvDeviceFieldLastSeenAt   = "DEVICE_FIELD_LAST_SEEN_AT"
	EnvDeviceFieldLastError    = "DEVICE_FIELD_LAST_ERROR"
	EnvDeviceFieldTags         = "DEVICE_FIELD_TAGS"
	EnvDeviceFieldRunningTask  = "DEVICE_FIELD_RUNNING_TASK"
	EnvDeviceFieldPendingTasks = "DEVICE_FIELD_PENDING_TASKS"
)

// DeviceFields lists column names for the device inventory table.
type DeviceFields struct {
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

// DeviceRecordInput describes the payload used to create or update a device row.
type DeviceRecordInput struct {
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

// DeviceFieldsFromEnv builds fields with environment overrides.
func DeviceFieldsFromEnv() DeviceFields {
	f := DefaultDeviceFields
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldSerial)); v != "" {
		f.DeviceSerial = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldOSType)); v != "" {
		f.OSType = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldOSVersion)); v != "" {
		f.OSVersion = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldIPLocation)); v != "" {
		f.IPLocation = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldIsRoot)); v != "" {
		f.IsRoot = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldProviderUUID)); v != "" {
		f.ProviderUUID = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldAgentVersion)); v != "" {
		f.AgentVersion = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldStatus)); v != "" {
		f.Status = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldLastSeenAt)); v != "" {
		f.LastSeenAt = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldLastError)); v != "" {
		f.LastError = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldTags)); v != "" {
		f.Tags = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldRunningTask)); v != "" {
		f.RunningTask = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceFieldPendingTasks)); v != "" {
		f.PendingTasks = v
	}
	return f
}

// DeviceInfoTable caches decoded rows for quick lookup.
type DeviceInfoTable struct {
	Ref    BitableRef
	Fields DeviceFields
	Rows   []DeviceRecordInput
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

// FetchDeviceTable downloads the device info table.
func (c *Client) FetchDeviceTable(ctx context.Context, rawURL string, override *DeviceFields) (*DeviceInfoTable, error) {
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
	fields := DefaultDeviceFields
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
		Rows:   make([]DeviceRecordInput, 0, len(records)),
		index:  make(map[string]string, len(records)),
	}
	for _, rec := range records {
		serial := strings.TrimSpace(toString(rec.Fields[fields.DeviceSerial]))
		if serial == "" {
			continue
		}
		row := DeviceRecordInput{
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
			PendingTasks: toStrings(rec.Fields[fields.PendingTasks]),
		}
		if ts := toTime(rec.Fields[fields.LastSeenAt]); ts != nil {
			row.LastSeenAt = ts
		}
		table.Rows = append(table.Rows, row)
		table.index[serial] = rec.RecordID
	}
	return table, nil
}

// UpsertDevice creates or updates a device row keyed by DeviceSerial.
func (c *Client) UpsertDevice(ctx context.Context, rawURL string, fields DeviceFields, rec DeviceRecordInput) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(rawURL) == "" {
		return errors.New("feishu: device info table url is empty")
	}
	table, err := c.FetchDeviceTable(ctx, rawURL, &fields)
	if err != nil {
		return err
	}
	payload, err := buildDeviceInfoPayload(rec, table.Fields)
	if err != nil {
		return err
	}
	serial := strings.TrimSpace(rec.DeviceSerial)
	recordID := table.RecordIDBySerial(serial)
	if recordID == "" {
		recordID, err = c.lookupDeviceRecordID(ctx, table.Ref, table.Fields, serial)
		if err != nil {
			return err
		}
		if recordID == "" {
			_, err = c.createBitableRecord(ctx, table.Ref, payload)
			return err
		}
	}
	return c.updateBitableRecord(ctx, table.Ref, recordID, payload)
}

func (c *Client) lookupDeviceRecordID(ctx context.Context, ref BitableRef, fields DeviceFields, serial string) (string, error) {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return "", nil
	}
	filter := NewFilterInfo("and")
	condition := NewCondition(fields.DeviceSerial, "is", serial)
	if condition == nil {
		return "", fmt.Errorf("feishu: device serial field not configured")
	}
	filter.Conditions = append(filter.Conditions, condition)
	opts := &TaskQueryOptions{Limit: 1, Filter: filter}
	refs := []BitableRef{ref}
	if strings.TrimSpace(ref.ViewID) != "" {
		refNoView := ref
		refNoView.ViewID = ""
		refs = append(refs, refNoView)
	}
	for _, candidate := range refs {
		records, err := c.listBitableRecords(ctx, candidate, 1, opts)
		if err != nil {
			return "", err
		}
		for _, rec := range records {
			val := strings.TrimSpace(toString(rec.Fields[fields.DeviceSerial]))
			if strings.EqualFold(val, serial) {
				return rec.RecordID, nil
			}
		}
	}
	return "", nil
}

func (fields DeviceFields) merge(override DeviceFields) DeviceFields {
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

func buildDeviceInfoPayload(rec DeviceRecordInput, fields DeviceFields) (map[string]any, error) {
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

	// Always sync running/pending fields so bitable view reflects the latest state.
	if runningCol := strings.TrimSpace(fields.RunningTask); runningCol != "" {
		row[runningCol] = strings.TrimSpace(rec.RunningTask)
	}
	if pendingCol := strings.TrimSpace(fields.PendingTasks); pendingCol != "" {
		if pending := filterNonEmpty(rec.PendingTasks); pending != nil {
			row[pendingCol] = pending
		} else {
			// Write empty slice to clear stale pending tasks.
			row[pendingCol] = []string{}
		}
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

// addOptionalStringSlice writes a slice to a multi-select column when not empty.

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

// toStrings flattens Feishu multi-select or scalar values into a slice of strings.
func toStrings(val any) []string {
	switch v := val.(type) {
	case nil:
		return nil
	case []string:
		return filterNonEmpty(v)
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(toString(item)); s != "" {
				result = append(result, s)
			}
		}
		return filterNonEmpty(result)
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil
		}
		return []string{trimmed}
	default:
		if s := strings.TrimSpace(toString(v)); s != "" {
			return []string{s}
		}
		return nil
	}
}

func addOptionalStringSlice(dst map[string]any, column string, values []string) {
	if strings.TrimSpace(column) == "" {
		return
	}
	trimmed := filterNonEmpty(values)
	if len(trimmed) == 0 {
		return
	}
	dst[column] = trimmed
}

func filterNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
