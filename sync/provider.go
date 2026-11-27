package sync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

// Provider is the platform half of sync. It turns a platform's webhook into
// normalized events and turns an outbound message into the platform's own
// call. It performs no board writes and keeps no durable state: the board log
// is the record.
//
// Receive authenticates the request before it parses anything, and returns
// normalized records. The registry stores them before answering the platform.
type Provider interface {
	// Platform is the platform's name, in IDs and labels: "slack", "github".
	Platform() Platform
	// Capabilities says what the platform can do.
	Capabilities() Capabilities
	// Receive verifies a webhook for entry key and normalizes what it brought.
	// It must not perform board writes; it may answer a platform challenge.
	Receive(ctx context.Context, key Key, req *WebhookRequest) (Intake, error)
	// ResolveChannel turns a name ("#prs", "ENG", "support") into the
	// platform's channel ID.
	ResolveChannel(ctx context.Context, key Key, name string) (string, error)
	// ResolveTarget turns a native sync target into a channel or thread.
	ResolveTarget(ctx context.Context, key Key, target string) (ResolvedTarget, error)
	// Post posts a new message, or a thread root for a channel target.
	Post(ctx context.Context, key Key, target ResolvedTarget, msg OutboundMessage) (Posted, error)
}

// SelfIdentifier is an optional Provider capability: the platform author IDs
// that belong to the connection itself. The registry drops a record whose
// author is one of them before it reaches the inbox, as defence in depth
// beside each adapter's own check. An adapter without it relies on markers
// and must not mirror a channel where its own posts are indistinguishable.
type SelfIdentifier interface {
	SelfIDs() []string
}

// Capabilities are what a provider supports.
type Capabilities struct {
	// StartThreads: a board root in a mapped channel can start a platform
	// thread (Slack, Linear), or not (GitHub).
	StartThreads bool
	// SingleReaction: one reaction per person per message.
	SingleReaction bool
	// UploadsFiles: the platform can take files.
	UploadsFiles bool
	// StartsThreadsWithFiles: a new root's files can go in the same message.
	StartsThreadsWithFiles bool
	// Edits, Deletes, Reactions: changes to messages already delivered.
	Edits     bool
	Deletes   bool
	Reactions bool
	// Typing: the platform shows typing status.
	Typing bool
}

// Editor changes a message already delivered. Thread and message are native.
type Editor interface {
	Edit(ctx context.Context, key Key, thread ThreadRef, message string, msg OutboundMessage) error
}

// Deleter deletes a message already delivered.
type Deleter interface {
	Delete(ctx context.Context, key Key, thread ThreadRef, message string) error
}

// Reactor adds or removes a reaction on a delivered message.
type Reactor interface {
	React(ctx context.Context, key Key, thread ThreadRef, message, emoji string, added bool) error
}

// Typist shows typing status in a platform thread.
type Typist interface {
	Typing(ctx context.Context, key Key, thread ThreadRef, latest string, status string) error
}

// FileUploader uploads files after a message's text.
type FileUploader interface {
	UploadFiles(ctx context.Context, key Key, thread ThreadRef, files []OutboundFile) (fileIDs []string, err error)
}

// FileFetcher fetches a platform file's bytes for an import.
type FileFetcher interface {
	FetchFile(ctx context.Context, key Key, file File) ([]byte, error)
}

// RootFetcher recovers a thread's root when a reply arrives before the board
// has seen the root (a comment on an older PR or issue).
type RootFetcher interface {
	FetchRoot(ctx context.Context, key Key, thread string) (*RootDraft, error)
}

// Labeler applies a message's labels to a delivered copy without editing its
// text. A labels.changed event reaches a Labeler even when a renderer exists:
// the renderer's text must not overwrite a platform issue's own description.
type Labeler interface {
	Labels(ctx context.Context, key Key, thread ThreadRef, labels, before map[string]string) error
}

// Linker maps between a platform's URLs and its own thread and message IDs.
type Linker interface {
	// Locate reads a URL as native thread and message IDs. nil means the URL
	// isn't this platform's.
	Locate(url string) (thread string, message string, ok bool)
	// URL makes the platform's URL for one of its messages.
	URL(thread, message string) (string, error)
}

// Permalinker returns the platform's own link to an event's message.
type Permalinker interface {
	Permalink(ctx context.Context, key Key, event Event) (string, error)
}

// WebhookRequest is a bounded copy of an inbound HTTP request. Building one
// caps the body and strips the connection; adapters never read the network.
type WebhookRequest struct {
	Method     string      `json:"method"`
	Path       string      `json:"path"`
	Header     http.Header `json:"-"`
	Body       []byte      `json:"body"`
	RemoteAddr string      `json:"remoteAddr,omitempty"`
}

// Validate checks the request shape.
func (r *WebhookRequest) Validate() error {
	if r.Method == "" {
		return errors.New("sync: webhook request has no method")
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		return errors.New("sync: webhook request must be POST or GET")
	}
	if len(r.Header) == 0 {
		return errors.New("sync: webhook request has no headers")
	}
	return nil
}

// NewWebhookRequest copies an HTTP request's method, path, headers and body,
// reading at most maxBody bytes. A body over the limit is an error, so a
// platform's oversized payload never reaches an adapter.
func NewWebhookRequest(r *http.Request, maxBody int64) (*WebhookRequest, error) {
	if r == nil {
		return nil, errors.New("sync: nil webhook request")
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBody {
		return nil, errors.New("sync: webhook body is too large")
	}
	return &WebhookRequest{
		Method:     r.Method,
		Path:       r.URL.Path,
		Header:     r.Header.Clone(),
		Body:       body,
		RemoteAddr: r.RemoteAddr,
	}, nil
}

// WebhookResponse is what a platform expects back. Status 0 means 200.
type WebhookResponse struct {
	Status      int         `json:"status,omitempty"`
	Header      http.Header `json:"-"`
	Body        []byte      `json:"body,omitempty"`
	ContentType string      `json:"contentType,omitempty"`
}

// OK is an empty 200.
func OK() WebhookResponse { return WebhookResponse{Status: http.StatusOK} }

// WriteTo writes the response, defaulting to 200 and text/plain.
func (r WebhookResponse) WriteTo(w http.ResponseWriter) {
	status := r.Status
	if status == 0 {
		status = http.StatusOK
	}
	for name, values := range r.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	contentType := r.ContentType
	if contentType == "" {
		contentType = "text/plain; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	if len(r.Body) > 0 {
		_, _ = w.Write(r.Body)
	}
}

// Record is one normalized platform event an adapter emits, with the identity
// a retry keeps.
type Record struct {
	// Dedupe is the occurrence's identity. A platform redelivering the same
	// event must produce the same value; a later identical action must produce
	// a new one. It becomes the inbox ID.
	Dedupe string          `json:"dedupe"`
	Event  Event           `json:"event"`
	Raw    json.RawMessage `json:"raw,omitempty"`
}

// Validate checks the record and fills the event ID from Dedupe.
func (r *Record) Validate() error {
	if r.Dedupe == "" {
		return errors.New("sync: record has no dedupe ID")
	}
	if len(r.Dedupe) > 300 {
		return errors.New("sync: dedupe ID is longer than 300 bytes")
	}
	if r.Event.ID == "" {
		r.Event.ID = r.Dedupe
	}
	if len(r.Raw) > 0 {
		r.Event.Raw = r.Raw
	}
	return r.Event.Validate()
}

// Intake is what a webhook produced: records to store, and the answer to send
// once they are.
type Intake struct {
	Records  []Record        `json:"records,omitempty"`
	Response WebhookResponse `json:"response"`
}

// StatusCode maps an error to the HTTP answer a webhook sender should get.
// Anything not classified is 500, so the platform retries.
func StatusCode(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrRateLimited):
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

// RetryAfter reads a Retry-After header, in seconds or as an HTTP date. Zero
// means the header wasn't usable.
func RetryAfter(header http.Header) int {
	value := header.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return seconds
	}
	if when, err := http.ParseTime(value); err == nil {
		if seconds := int(when.Sub(timeNow()).Seconds()); seconds > 0 {
			return seconds
		}
	}
	return 0
}
