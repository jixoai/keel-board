package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"

	boardsync "github.com/jixoai/keel-board/sync"
)

// ErrFileTooLarge means a file is over the configured limit. It is permanent;
// the caller may keep the message and say the file did not come over.
var ErrFileTooLarge = errors.New("slack: file is too large")

// UploadFileRef names an uploaded file for files.completeUploadExternal.
type UploadFileRef struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

// GetUploadURLExternal starts Slack's upload flow. The URL is Slack's own;
// the caller must not POST anywhere else.
func (c *Client) GetUploadURLExternal(ctx context.Context, filename string, length int64) (uploadURL, fileID string, err error) {
	if filename == "" || length <= 0 {
		return "", "", fmt.Errorf("%w: slack upload needs a filename and a length", boardsync.ErrBadRequest)
	}
	var response struct {
		apiEnvelope
		UploadURL string `json:"upload_url"`
		FileID    string `json:"file_id"`
	}
	form := url.Values{"filename": {filename}, "length": {fmt.Sprintf("%d", length)}}
	if err := c.call(ctx, "files.getUploadURLExternal", form, &response); err != nil {
		return "", "", err
	}
	if response.UploadURL == "" || response.FileID == "" {
		return "", "", errors.New("slack files.getUploadURLExternal answered no URL or file ID")
	}
	if !isSlackFileURL(response.UploadURL) {
		return "", "", fmt.Errorf("%w: slack upload URL is not on a Slack domain", boardsync.ErrBadRequest)
	}
	return response.UploadURL, response.FileID, nil
}

// UploadExternal posts a file's bytes to the URL Slack handed out. The URL is
// pre-signed, so no token is sent; a redirect off Slack's domain is refused.
func (c *Client) UploadExternal(ctx context.Context, uploadURL, filename string, data []byte) error {
	if !isSlackFileURL(uploadURL) {
		return fmt.Errorf("%w: slack upload URL is not on a Slack domain", boardsync.ErrBadRequest)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &body)
	if err != nil {
		return boardsync.RedactError(err, boardsync.Secret(uploadURL), c.token)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := c.fileClient().Do(request)
	if err != nil {
		return boardsync.RedactError(err, boardsync.Secret(uploadURL), c.token)
	}
	defer response.Body.Close()
	if err := httpStatusError("files.upload", response); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	return nil
}

// CompleteUploadExternal shares uploaded files in a channel or thread.
func (c *Client) CompleteUploadExternal(ctx context.Context, files []UploadFileRef, channelID, threadTS, comment string) ([]string, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: slack upload has no files", boardsync.ErrBadRequest)
	}
	if channelID == "" {
		return nil, fmt.Errorf("%w: slack upload has no channel", boardsync.ErrBadRequest)
	}
	payload, err := json.Marshal(files)
	if err != nil {
		return nil, err
	}
	form := url.Values{
		"files":      {string(payload)},
		"channel_id": {channelID},
	}
	if threadTS != "" {
		form.Set("thread_ts", threadTS)
	}
	if comment != "" {
		form.Set("initial_comment", comment)
	}
	var response struct {
		apiEnvelope
		Files []struct {
			ID string `json:"id"`
		} `json:"files"`
	}
	if err := c.call(ctx, "files.completeUploadExternal", form, &response); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(response.Files))
	for _, file := range response.Files {
		if file.ID != "" {
			ids = append(ids, file.ID)
		}
	}
	if len(ids) == 0 {
		return nil, errors.New("slack files.completeUploadExternal answered no file IDs")
	}
	return ids, nil
}

// DeleteFile removes an uploaded file that was never shared. Cleanup is
// best-effort; a file Slack already dropped is not an error worth failing a
// message for.
func (c *Client) DeleteFile(ctx context.Context, fileID string) error {
	if fileID == "" {
		return nil
	}
	return c.call(ctx, "files.delete", url.Values{"file": {fileID}}, nil)
}

// FetchFile downloads one file a Slack message carried. Only a Slack file URL
// is fetched, with the app's token; the body is bounded and redirects must
// stay on Slack's domain.
func (c *Client) FetchFile(ctx context.Context, file boardsync.File, maxBytes int64) ([]byte, error) {
	if !isSlackFileURL(file.URL) {
		return nil, fmt.Errorf("%w: slack file has no Slack download URL", boardsync.ErrBadRequest)
	}
	if maxBytes <= 0 {
		maxBytes = 25 << 20
	}
	if file.Size > maxBytes {
		return nil, fmt.Errorf("%w: %w: %d bytes is over %d", boardsync.ErrBadRequest, ErrFileTooLarge, file.Size, maxBytes)
	}
	if c.token.Empty() {
		return nil, fmt.Errorf("%w: slack bot token is unset", boardsync.ErrUnauthorized)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, file.URL, nil)
	if err != nil {
		return nil, boardsync.RedactError(err, boardsync.Secret(file.URL), c.token)
	}
	request.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	response, err := c.fileClient().Do(request)
	if err != nil {
		return nil, boardsync.RedactError(err, boardsync.Secret(file.URL), c.token)
	}
	defer response.Body.Close()
	if err := httpStatusError("files.download", response); err != nil {
		return nil, err
	}
	if response.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %w: %d bytes is over %d", boardsync.ErrBadRequest, ErrFileTooLarge, response.ContentLength, maxBytes)
	}
	body, err := readLimited(response.Body, maxBytes)
	if err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return nil, fmt.Errorf("%w: %w: over %d bytes", boardsync.ErrBadRequest, ErrFileTooLarge, maxBytes)
		}
		return nil, fmt.Errorf("slack: reading file: %w", err)
	}
	return body, nil
}

// fileClient is the HTTP client for Slack file traffic: it follows a redirect
// only while it stays on a Slack domain.
func (c *Client) fileClient() *http.Client {
	client := *c.http
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("slack: too many redirects")
		}
		if !isSlackFileURL(req.URL.String()) {
			return errors.New("slack: file redirect left Slack's domain")
		}
		return nil
	}
	return &client
}

// failureNotes says what happened to each file when the upload did not
// reach Slack. The name is untrusted; renderBody escapes the whole note.
func failureNotes(files []boardsync.OutboundFile, boardMessage uint64, err error) []string {
	reason := failureReason(err)
	notes := make([]string, 0, len(files))
	for _, file := range files {
		name := fileDisplayName(file.Filename)
		parts := []string{humanBytes(int64(len(file.Data)))}
		if reason != "" {
			parts = append(parts, reason)
		}
		if boardMessage != 0 {
			parts = append(parts, fmt.Sprintf("on the board: #%d", boardMessage))
		}
		notes = append(notes, fmt.Sprintf("📎 %s (%s)", name, strings.Join(parts, ", ")))
	}
	return notes
}

// failureReason is a Slack error code or a short class, never a URL, token,
// or message body.
func failureReason(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Code != "" {
		return apiErr.Code
	}
	switch {
	case errors.Is(err, ErrFileTooLarge):
		return "too large"
	case errors.Is(err, boardsync.ErrRateLimited):
		return "rate limited"
	case errors.Is(err, boardsync.ErrUnauthorized):
		return "not authorized"
	case errors.Is(err, boardsync.ErrNotFound):
		return "not found"
	default:
		return "upload failed"
	}
}

// uploadPlan decides which files Slack can take and what to say about the
// rest. A file over the limit or past the count is a line in the text, not a
// reason to lose the message.
type uploadPlan struct {
	files []boardsync.OutboundFile
	notes []string
}

func planUploads(files []boardsync.OutboundFile, cfg Config) uploadPlan {
	var plan uploadPlan
	maxFiles := cfg.maxFiles()
	maxBytes := cfg.maxFileBytes()
	for index, file := range files {
		name := fileDisplayName(file.Filename)
		switch {
		case index >= maxFiles:
			plan.notes = append(plan.notes, fileNote(name, "too many files in one message"))
		case len(file.Data) == 0:
			plan.notes = append(plan.notes, fileNote(name, "the file is empty"))
		case int64(len(file.Data)) > maxBytes:
			plan.notes = append(plan.notes, fileNote(name, fmt.Sprintf("%s is over %s", humanBytes(int64(len(file.Data))), humanBytes(maxBytes))))
		default:
			plan.files = append(plan.files, file)
		}
	}
	return plan
}

func fileNote(name, why string) string {
	return fmt.Sprintf("📎 %s didn't come over: %s", fileDisplayName(name), why)
}

func humanBytes(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// uploadFilename names a file as Slack should receive it. Slack picks a
// file's type from its extension and offers an unknown one as a binary
// download, so a text file with a developer extension gets .txt appended and
// is previewed. The board keeps the file's own name.
func uploadFilename(filename, mimeType string) string {
	filename = cleanText(filename)
	if filename == "" {
		filename = "file"
	}
	suffix := ""
	if isTextMIME(mimeType) {
		extension := strings.ToLower(strings.TrimPrefix(path.Ext(filename), "."))
		if !textExtensions[extension] {
			suffix = ".txt"
		}
	}
	return truncateRunes(filename, 200-len([]rune(suffix))) + suffix
}

func isTextMIME(mimeType string) bool {
	if strings.HasPrefix(mimeType, "text/") {
		return true
	}
	switch mimeType {
	case "application/json", "application/xml", "application/yaml", "application/x-yaml",
		"application/javascript", "application/x-sh", "application/x-ndjson", "application/sql":
		return true
	default:
		return false
	}
}

var textExtensions = map[string]bool{
	"txt": true, "md": true, "markdown": true, "csv": true,
	"json": true, "xml": true, "yaml": true, "yml": true,
	"html": true, "css": true, "js": true, "ts": true, "py": true, "rb": true,
	"go": true, "rs": true, "java": true, "c": true, "h": true, "cpp": true,
	"sh": true, "bash": true, "sql": true, "diff": true, "patch": true,
}
