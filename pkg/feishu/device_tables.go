package feishu

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Environment override keys for device info/task tables.
const (
	EnvDeviceInfoURL = "DEVICE_INFO_BITABLE_URL"
	EnvDeviceTaskURL = "DEVICE_TASK_BITABLE_URL"

	EnvDeviceInfoFieldSerial       = "DEVICE_INFO_FIELD_SERIAL"
	EnvDeviceInfoFieldOSType       = "DEVICE_INFO_FIELD_OSTYPE"
	EnvDeviceInfoFieldOSVersion    = "DEVICE_INFO_FIELD_OSVERSION"
	EnvDeviceInfoFieldLocationCity = "DEVICE_INFO_FIELD_LOCATION_CITY"
	EnvDeviceInfoFieldIsRoot       = "DEVICE_INFO_FIELD_ISROOT"
	EnvDeviceInfoFieldProviderUUID = "DEVICE_INFO_FIELD_PROVIDERUUID"
	EnvDeviceInfoFieldAgentVersion = "DEVICE_INFO_FIELD_AGENT_VERSION"
	EnvDeviceInfoFieldStatus       = "DEVICE_INFO_FIELD_STATUS"
	EnvDeviceInfoFieldLastSeenAt   = "DEVICE_INFO_FIELD_LAST_SEEN_AT"
	EnvDeviceInfoFieldLastError    = "DEVICE_INFO_FIELD_LAST_ERROR"
	EnvDeviceInfoFieldTags         = "DEVICE_INFO_FIELD_TAGS"

	EnvDeviceTaskFieldJobID         = "DEVICE_TASK_FIELD_JOBID"
	EnvDeviceTaskFieldDeviceSerial  = "DEVICE_TASK_FIELD_DEVICE_SERIAL"
	EnvDeviceTaskFieldApp           = "DEVICE_TASK_FIELD_APP"
	EnvDeviceTaskFieldState         = "DEVICE_TASK_FIELD_STATE"
	EnvDeviceTaskFieldAssignedTasks = "DEVICE_TASK_FIELD_ASSIGNED_TASKS"
	EnvDeviceTaskFieldRunningTask   = "DEVICE_TASK_FIELD_RUNNING_TASK"
	EnvDeviceTaskFieldStartAt       = "DEVICE_TASK_FIELD_START_AT"
	EnvDeviceTaskFieldEndAt         = "DEVICE_TASK_FIELD_END_AT"
	EnvDeviceTaskFieldErrorMessage  = "DEVICE_TASK_FIELD_ERROR_MESSAGE"
)

// DeviceInfoFields lists column names for the device inventory table.
type DeviceInfoFields struct {
	DeviceSerial string
	OSType       string
	OSVersion    string
	LocationCity string
	IsRoot       string
	ProviderUUID string
	AgentVersion string
	Status       string
	LastSeenAt   string
	LastError    string
	Tags         string
}

// DefaultDeviceInfoFields provides sensible defaults matching the design doc.
var DefaultDeviceInfoFields = DeviceInfoFields{
	DeviceSerial: "DeviceSerial",
	OSType:       "OSType",
	OSVersion:    "OSVersion",
	LocationCity: "LocationCity",
	IsRoot:       "IsRoot",
	ProviderUUID: "ProviderUUID",
	AgentVersion: "AgentVersion",
	Status:       "Status",
	LastSeenAt:   "LastSeenAt",
	LastError:    "LastError",
	Tags:         "Tags",
}

// DeviceTaskFields lists column names for the device task tracking table.
type DeviceTaskFields struct {
	JobID         string
	DeviceSerial  string
	App           string
	State         string
	AssignedTasks string
	RunningTask   string
	StartAt       string
	EndAt         string
	ErrorMessage  string
}

// DefaultDeviceTaskFields provides sensible defaults.
var DefaultDeviceTaskFields = DeviceTaskFields{
	JobID:         "JobID",
	DeviceSerial:  "DeviceSerial",
	App:           "App",
	State:         "State",
	AssignedTasks: "AssignedTasks",
	RunningTask:   "RunningTask",
	StartAt:       "StartAt",
	EndAt:         "EndAt",
	ErrorMessage:  "ErrorMessage",
}

// DeviceInfoRecordInput describes the payload used to create or update a device row.
type DeviceInfoRecordInput struct {
	DeviceSerial string
	OSType       string
	OSVersion    string
	LocationCity string
	IsRoot       string
	ProviderUUID string
	AgentVersion string
	Status       string
	LastSeenAt   *time.Time
	LastError    string
	Tags         string
}

// DeviceTaskRecordInput describes a single device job record creation.
type DeviceTaskRecordInput struct {
	JobID         string
	DeviceSerial  string
	App           string
	State         string
	AssignedTasks []string
	RunningTask   string
	StartAt       *time.Time
	EndAt         *time.Time
	Elapsed       *int64
	ErrorMessage  string
}

// DeviceTaskUpdate describes partial updates to an existing job row.
type DeviceTaskUpdate struct {
	State        string
	RunningTask  string
	EndAt        *time.Time
	Elapsed      *int64
	ErrorMessage string
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
	if v := strings.TrimSpace(os.Getenv(EnvDeviceInfoFieldLocationCity)); v != "" {
		f.LocationCity = v
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
	return f
}

// DeviceTaskFieldsFromEnv builds task fields with env overrides.
func DeviceTaskFieldsFromEnv() DeviceTaskFields {
	f := DefaultDeviceTaskFields
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldJobID)); v != "" {
		f.JobID = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldDeviceSerial)); v != "" {
		f.DeviceSerial = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldApp)); v != "" {
		f.App = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldState)); v != "" {
		f.State = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldAssignedTasks)); v != "" {
		f.AssignedTasks = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldRunningTask)); v != "" {
		f.RunningTask = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldStartAt)); v != "" {
		f.StartAt = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldEndAt)); v != "" {
		f.EndAt = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvDeviceTaskFieldErrorMessage)); v != "" {
		f.ErrorMessage = v
	}
	return f
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

// CreateDeviceTaskRecord creates one job row.
func (c *Client) CreateDeviceTaskRecord(ctx context.Context, rawURL string, rec DeviceTaskRecordInput, fields DeviceTaskFields) (string, error) {
	if c == nil {
		return "", errors.New("feishu: client is nil")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return "", err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return "", err
	}
	payload, err := buildDeviceTaskPayload(rec, fields)
	if err != nil {
		return "", err
	}
	return c.createBitableRecord(ctx, ref, payload)
}

// UpdateDeviceTaskByJob updates a task row identified by JobID.
func (c *Client) UpdateDeviceTaskByJob(ctx context.Context, rawURL, jobID string, fields DeviceTaskFields, upd DeviceTaskUpdate) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(jobID) == "" {
		return errors.New("feishu: job id is empty")
	}
	table, err := c.FetchDeviceTaskTable(ctx, rawURL, &fields)
	if err != nil {
		return err
	}
	recordID := table.RecordIDByJob(strings.TrimSpace(jobID))
	if recordID == "" {
		return fmt.Errorf("feishu: job %s not found", jobID)
	}
	payload, err := buildDeviceTaskUpdatePayload(upd, table.Fields)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return c.updateBitableRecord(ctx, table.Ref, recordID, payload)
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

// DeviceTaskTable caches decoded job rows keyed by JobID.
type DeviceTaskTable struct {
	Ref    BitableRef
	Fields DeviceTaskFields
	Rows   []DeviceTaskRecordInput
	index  map[string]string // JobID -> RecordID
}

// RecordIDByJob returns the record id for a given JobID.
func (t *DeviceTaskTable) RecordIDByJob(jobID string) string {
	if t == nil {
		return ""
	}
	if t.index == nil {
		return ""
	}
	return t.index[strings.TrimSpace(jobID)]
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
			LocationCity: toString(rec.Fields[fields.LocationCity]),
			IsRoot:       toString(rec.Fields[fields.IsRoot]),
			ProviderUUID: toString(rec.Fields[fields.ProviderUUID]),
			AgentVersion: toString(rec.Fields[fields.AgentVersion]),
			Status:       toString(rec.Fields[fields.Status]),
			LastError:    toString(rec.Fields[fields.LastError]),
			Tags:         toString(rec.Fields[fields.Tags]),
		}
		if ts := toTime(rec.Fields[fields.LastSeenAt]); ts != nil {
			row.LastSeenAt = ts
		}
		table.Rows = append(table.Rows, row)
		table.index[serial] = rec.RecordID
	}
	return table, nil
}

// FetchDeviceTaskTable downloads the device task table.
func (c *Client) FetchDeviceTaskTable(ctx context.Context, rawURL string, override *DeviceTaskFields) (*DeviceTaskTable, error) {
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
	fields := DefaultDeviceTaskFields
	if override != nil {
		fields = fields.merge(*override)
	}
	records, err := c.listBitableRecords(ctx, ref, defaultBitablePageSize, nil)
	if err != nil {
		return nil, err
	}
	table := &DeviceTaskTable{
		Ref:    ref,
		Fields: fields,
		Rows:   make([]DeviceTaskRecordInput, 0, len(records)),
		index:  make(map[string]string, len(records)),
	}
	for _, rec := range records {
		job := toString(rec.Fields[fields.JobID])
		if job == "" {
			continue
		}
		row := DeviceTaskRecordInput{
			JobID:        job,
			DeviceSerial: toString(rec.Fields[fields.DeviceSerial]),
			App:          toString(rec.Fields[fields.App]),
			State:        toString(rec.Fields[fields.State]),
			RunningTask:  toString(rec.Fields[fields.RunningTask]),
			ErrorMessage: toString(rec.Fields[fields.ErrorMessage]),
		}
		if start := toTime(rec.Fields[fields.StartAt]); start != nil {
			row.StartAt = start
		}
		if end := toTime(rec.Fields[fields.EndAt]); end != nil {
			row.EndAt = end
		}
		// AssignedTasks might be array or string
		switch v := rec.Fields[fields.AssignedTasks].(type) {
		case []any:
			for _, item := range v {
				if s := toString(item); s != "" {
					row.AssignedTasks = append(row.AssignedTasks, s)
				}
			}
		case string:
			if strings.TrimSpace(v) != "" {
				row.AssignedTasks = []string{v}
			}
		}
		table.Rows = append(table.Rows, row)
		table.index[job] = rec.RecordID
	}
	return table, nil
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
	if strings.TrimSpace(override.LocationCity) != "" {
		result.LocationCity = override.LocationCity
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
	return result
}

func (fields DeviceTaskFields) merge(override DeviceTaskFields) DeviceTaskFields {
	result := fields
	if strings.TrimSpace(override.JobID) != "" {
		result.JobID = override.JobID
	}
	if strings.TrimSpace(override.DeviceSerial) != "" {
		result.DeviceSerial = override.DeviceSerial
	}
	if strings.TrimSpace(override.App) != "" {
		result.App = override.App
	}
	if strings.TrimSpace(override.State) != "" {
		result.State = override.State
	}
	if strings.TrimSpace(override.AssignedTasks) != "" {
		result.AssignedTasks = override.AssignedTasks
	}
	if strings.TrimSpace(override.RunningTask) != "" {
		result.RunningTask = override.RunningTask
	}
	if strings.TrimSpace(override.StartAt) != "" {
		result.StartAt = override.StartAt
	}
	if strings.TrimSpace(override.EndAt) != "" {
		result.EndAt = override.EndAt
	}
	if strings.TrimSpace(override.ErrorMessage) != "" {
		result.ErrorMessage = override.ErrorMessage
	}
	return result
}

func buildDeviceInfoPayload(rec DeviceInfoRecordInput, fields DeviceInfoFields) (map[string]any, error) {
	row := make(map[string]any)
	addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
	addOptionalField(row, fields.OSType, rec.OSType)
	addOptionalField(row, fields.OSVersion, rec.OSVersion)
	addOptionalField(row, fields.LocationCity, rec.LocationCity)
	addOptionalField(row, fields.IsRoot, rec.IsRoot)
	addOptionalField(row, fields.ProviderUUID, rec.ProviderUUID)
	addOptionalField(row, fields.AgentVersion, rec.AgentVersion)
	addOptionalField(row, fields.Status, rec.Status)
	addOptionalField(row, fields.LastError, rec.LastError)
	addOptionalField(row, fields.Tags, rec.Tags)
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

func buildDeviceTaskPayload(rec DeviceTaskRecordInput, fields DeviceTaskFields) (map[string]any, error) {
	row := make(map[string]any)
	addOptionalField(row, fields.JobID, rec.JobID)
	addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
	addOptionalField(row, fields.App, rec.App)
	addOptionalField(row, fields.State, rec.State)
	if len(rec.AssignedTasks) > 0 && strings.TrimSpace(fields.AssignedTasks) != "" {
		row[fields.AssignedTasks] = rec.AssignedTasks
	}
	addOptionalField(row, fields.RunningTask, rec.RunningTask)
	if rec.StartAt != nil && strings.TrimSpace(fields.StartAt) != "" {
		row[fields.StartAt] = rec.StartAt.UTC().UnixMilli()
	}
	if rec.EndAt != nil && strings.TrimSpace(fields.EndAt) != "" {
		row[fields.EndAt] = rec.EndAt.UTC().UnixMilli()
	}
	addOptionalField(row, fields.ErrorMessage, rec.ErrorMessage)
	if len(row) == 0 {
		return nil, errors.New("feishu: device task payload is empty")
	}
	return row, nil
}

func buildDeviceTaskUpdatePayload(upd DeviceTaskUpdate, fields DeviceTaskFields) (map[string]any, error) {
	row := make(map[string]any)
	addOptionalField(row, fields.State, upd.State)
	if strings.TrimSpace(fields.RunningTask) != "" {
		// allow clearing running task to empty string
		row[fields.RunningTask] = upd.RunningTask
	}
	if upd.EndAt != nil && strings.TrimSpace(fields.EndAt) != "" {
		row[fields.EndAt] = upd.EndAt.UTC().UnixMilli()
	}
	addOptionalField(row, fields.ErrorMessage, upd.ErrorMessage)
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
