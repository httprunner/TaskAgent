package feishusdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	driveParentTypeBitableImage = "bitable_image"
	driveParentTypeBitableFile  = "bitable_file"
)

// UploadBitableMedia uploads bytes via Feishu drive media upload API and returns a file_token.
// The returned token can be used in a Bitable attachment field.
func (c *Client) UploadBitableMedia(ctx context.Context, appToken string, fileName string, content []byte, asImage bool) (string, error) {
	if c == nil {
		return "", fmt.Errorf("feishu: client is nil")
	}
	appToken = strings.TrimSpace(appToken)
	if appToken == "" {
		return "", fmt.Errorf("feishu: app token is empty")
	}
	fileName = strings.TrimSpace(fileName)
	if fileName == "" {
		fileName = "upload.bin"
	}
	if base := filepath.Base(fileName); strings.TrimSpace(base) != "" {
		fileName = base
	}
	if len(content) == 0 {
		return "", fmt.Errorf("feishu: upload content is empty")
	}

	parentType := driveParentTypeBitableFile
	if asImage {
		parentType = driveParentTypeBitableImage
	}

	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return "", err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("file_name", fileName)
	_ = writer.WriteField("parent_type", parentType)
	_ = writer.WriteField("parent_node", appToken)
	_ = writer.WriteField("size", strconv.Itoa(len(content)))
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		_ = writer.Close()
		return "", fmt.Errorf("feishu: create multipart file field: %w", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(content)); err != nil {
		_ = writer.Close()
		return "", fmt.Errorf("feishu: write multipart file: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("feishu: finalize multipart payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase()+"/open-apis/drive/v1/medias/upload_all", &body)
	if err != nil {
		return "", fmt.Errorf("feishu: build upload request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("feishu: execute upload request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("feishu: read upload response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("feishu: upload http %d response: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			FileToken string `json:"file_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("feishu: decode upload response: %w", err)
	}
	if parsed.Code != 0 {
		return "", fmt.Errorf("feishu: upload failed code=%d msg=%s", parsed.Code, parsed.Msg)
	}
	if strings.TrimSpace(parsed.Data.FileToken) == "" {
		return "", fmt.Errorf("feishu: upload response missing file_token")
	}
	return strings.TrimSpace(parsed.Data.FileToken), nil
}
