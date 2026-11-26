package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// signingMemory is a memory store that can sign direct uploads: the test
// plays the client's PUT by writing the key itself.
type signingMemory struct {
	*blob.Memory
	lastKey string
}

func (s *signingMemory) SignUpload(_ context.Context, key string, opts blob.SignOptions) (blob.SignedUpload, error) {
	s.lastKey = key
	return blob.SignedUpload{
		URL:     "https://blob.test/" + url.PathEscape(key),
		Headers: map[string]string{"x-test-signature": "ok"},
	}, nil
}

// rawRequest sends bytes with the headers a file upload needs.
func (h *harness) rawRequest(method, path string, body io.Reader, headers map[string]string) (int, []byte) {
	h.t.Helper()
	request, err := http.NewRequest(method, h.http.URL+server.APIPrefix+path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("content-type", "application/octet-stream")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := h.http.Client().Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, data
}

// upload sends a file as actor.
func (h *harness) upload(actor, name, contentType string, data []byte) (int, []byte) {
	h.t.Helper()
	headers := map[string]string{
		"X-Board-Actor":    actor,
		"X-Board-Filename": url.PathEscape(name),
		"Content-Type":     contentType,
	}
	return h.rawRequest(http.MethodPost, "/attachments", bytes.NewReader(data), headers)
}

// uploadInTenant sends a file on one board.
func (h *harness) uploadInTenant(actor, tenant, name, contentType string, data []byte) (int, []byte) {
	h.t.Helper()
	headers := map[string]string{
		"X-Board-Actor":    actor,
		"X-Board-Tenant":   tenant,
		"X-Board-Filename": url.PathEscape(name),
		"Content-Type":     contentType,
	}
	return h.rawRequest(http.MethodPost, "/attachments", bytes.NewReader(data), headers)
}

// attachment reads a file's bytes as actor on one board.
func (h *harness) attachmentInTenant(actor, tenant, id string) (int, []byte, http.Header) {
	h.t.Helper()
	request, err := http.NewRequest(http.MethodGet, h.http.URL+server.APIPrefix+"/attachments/"+id, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("X-Board-Actor", actor)
	if tenant != "" {
		request.Header.Set("X-Board-Tenant", tenant)
	}
	response, err := h.http.Client().Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, data, response.Header
}

// attachment reads a file's bytes as actor.
func (h *harness) attachment(actor, id string) (int, []byte, http.Header) {
	h.t.Helper()
	return h.attachmentInTenant(actor, "", id)
}

func decodeAttachment(t *testing.T, data []byte) protocol.Attachment {
	t.Helper()
	return decode[protocol.AttachmentResponse](t, data).Attachment
}

func TestAttachmentsNeedAStore(t *testing.T) {
	h := newHarness(t)
	status, data := h.upload("local:user:alice", "a.txt", "text/plain", []byte("x"))
	h.expect(status, data, http.StatusNotImplemented)
	if code := errorCode(t, data); code != protocol.CodeNoFiles {
		t.Fatalf("expected no_files, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "attachments": []string{"abcdefghijklmnopqrstuv"},
	}, "local:user:alice", "", nil)
	h.expect(status, data, http.StatusNotImplemented)
	if code := errorCode(t, data); code != protocol.CodeNoFiles {
		t.Fatalf("expected no_files, got %s", code)
	}
}

func TestAttachmentLifecycle(t *testing.T) {
	store := blob.NewMemory()
	policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
		if actor.ID == "local:user:bob" && channel == "private" && permission == protocol.PermissionRead {
			return protocol.Forbidden("no private")
		}
		return nil
	})
	h := newHarness(t, server.Options{Blobs: store, Authorize: policy})
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	status, data := h.upload(alice, `..\shots/..\deploy.png`, "image/png; charset=binary", []byte("png bytes"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)
	if file.Name != "deploy.png" || file.Type != "image/png" || file.Size != int64(len("png bytes")) {
		t.Fatalf("unexpected attachment: %+v", file)
	}
	if len(file.ID) != 22 {
		t.Fatalf("unexpected id: %q", file.ID)
	}

	// Its uploader reads it; anyone else can't until a message carries it.
	status, body, headers := h.attachment(alice, file.ID)
	h.expect(status, body, http.StatusOK)
	if string(body) != "png bytes" || headers.Get("Content-Type") != "image/png" {
		t.Fatalf("unexpected bytes: %q %q", body, headers.Get("Content-Type"))
	}
	if headers.Get("Content-Security-Policy") == "" || headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("unexpected safety headers: %+v", headers)
	}
	if headers.Get("Cache-Control") != "private, max-age=31536000, immutable" {
		t.Fatalf("unexpected cache header: %q", headers.Get("Cache-Control"))
	}
	if headers.Get("Content-Disposition") == "" || !strings.HasPrefix(headers.Get("Content-Disposition"), "inline") {
		t.Fatalf("an image shows inline: %q", headers.Get("Content-Disposition"))
	}
	status, _, _ = h.attachment(bob, file.ID)
	if status != http.StatusNotFound {
		t.Fatalf("an unbound file isn't bob's: %d", status)
	}

	// Binding it to a file-only message: the text may be empty.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "attachments": []string{file.ID, file.ID},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	message := decode[postBody](t, data).Message
	if message.Text != "" || len(message.Attachments) != 1 || message.Attachments[0].ID != file.ID {
		t.Fatalf("unexpected message: %+v", message)
	}
	// A reader of the message reads the file.
	status, body, _ = h.attachment(bob, file.ID)
	h.expect(status, body, http.StatusOK)

	// A file on a message in a channel bob can't read stays hidden.
	privateFile := decodeAttachment(t, func() []byte {
		status, data := h.upload(alice, "secret.txt", "text/plain", []byte("secret"))
		h.expect(status, data, http.StatusCreated)
		return data
	}())
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "private", "text": "private file", "attachments": []string{privateFile.ID},
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	status, _, _ = h.attachment(bob, privateFile.ID)
	if status != http.StatusNotFound {
		t.Fatalf("bob can't read private: %d", status)
	}
	status, body, _ = h.attachment(alice, privateFile.ID)
	h.expect(status, body, http.StatusOK)

	// A bound file can't have its bytes deleted.
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeAttachmentInUse {
		t.Fatalf("expected attachment_in_use, got %s", code)
	}
	// Deleting the message hides it and releases the binding.
	status, data = h.deleteMessage(alice, message.ID, nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodGet, "/messages/"+message.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	if thread := decode[protocol.ThreadView](t, data); len(thread.Message.Attachments) != 0 {
		t.Fatalf("a tombstone carries no files: %+v", thread.Message.Attachments)
	}
	status, _, _ = h.attachment(bob, file.ID)
	if status != http.StatusNotFound {
		t.Fatalf("a deleted message's file is hidden: %d", status)
	}
	// Its uploader still reads it, as upstream keeps the uploader's access.
	status, body, _ = h.attachment(alice, file.ID)
	h.expect(status, body, http.StatusOK)

	// Another actor can't delete the file.
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, bob, "", nil)
	h.expect(status, data, http.StatusForbidden)
	// Its uploader deletes it once nothing binds it; a retry is nothing.
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusNoContent)
	status, _, _ = h.attachment(alice, file.ID)
	if status != http.StatusNotFound {
		t.Fatalf("deleted bytes should be gone: %d", status)
	}
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusNoContent)
}

func TestAttachmentLimitsAndValidation(t *testing.T) {
	h := newHarness(t, server.Options{Blobs: blob.NewMemory()})
	const alice = "local:user:alice"

	tests := []struct {
		name string
		data []byte
		want int
		code protocol.Code
	}{
		{"empty", nil, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"at the limit", bytes.Repeat([]byte("x"), protocol.AttachmentBytesMax), http.StatusCreated, ""},
		{"over the limit", bytes.Repeat([]byte("x"), protocol.AttachmentBytesMax+1), http.StatusRequestEntityTooLarge, protocol.CodeTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, data := h.upload(alice, "file.bin", "application/octet-stream", test.data)
			h.expect(status, data, test.want)
			if test.code != "" {
				if code := errorCode(t, data); code != test.code {
					t.Fatalf("expected %s, got %s", test.code, code)
				}
			}
		})
	}

	// The count limit is checked before IDs are resolved.
	ids := make([]string, protocol.AttachmentsPerMessageMax+1)
	for at := range ids {
		ids[at] = "abcdefghijklmnopqrstuv"
	}
	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "too many", "attachments": ids,
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidRequest {
		t.Fatalf("expected invalid_request, got %s", code)
	}

	// A file the board never issued is 404, as one the caller can't read is.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "missing", "attachments": []string{"abcdefghijklmnopqrstuv"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeAttachmentNotFound {
		t.Fatalf("expected attachment_not_found, got %s", code)
	}
	// An attachment ID that isn't shaped like one is a 400.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "bad", "attachments": []string{"nope"},
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)

	// A dotfile and a name with invisible formatting are refused, not silently
	// rewritten.
	for _, name := range []string{".env", "a\u202eb.txt", ".\u200bhidden"} {
		t.Run("name "+name, func(t *testing.T) {
			status, data := h.upload(alice, name, "text/plain", []byte("x"))
			h.expect(status, data, http.StatusBadRequest)
			if code := errorCode(t, data); code != protocol.CodeInvalidAttachment {
				t.Fatalf("expected invalid_attachment, got %s", code)
			}
		})
	}
}

func TestDirectUploads(t *testing.T) {
	t.Run("without a signer", func(t *testing.T) {
		h := newHarness(t, server.Options{Blobs: blob.NewMemory()})
		status, data := h.request(http.MethodPost, "/attachments/uploads", map[string]any{
			"name": "big.bin", "type": "application/octet-stream", "size": 5,
		}, "local:user:alice", "", nil)
		h.expect(status, data, http.StatusNotImplemented)
		if code := errorCode(t, data); code != protocol.CodeNoDirectUploads {
			t.Fatalf("expected no_direct_uploads, got %s", code)
		}
	})

	signer := &signingMemory{Memory: blob.NewMemory()}
	h := newHarness(t, server.Options{Blobs: signer})
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	status, data := h.request(http.MethodPost, "/attachments/uploads", map[string]any{
		"name": "big.bin", "type": "application/octet-stream", "size": 5,
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	ticket := decode[protocol.UploadTicket](t, data)
	if ticket.ID == "" || ticket.URL == "" || ticket.Headers["content-type"] != "application/octet-stream" {
		t.Fatalf("unexpected ticket: %+v", ticket)
	}
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusConflict)
	if code := errorCode(t, data); code != protocol.CodeNotUploaded {
		t.Fatalf("expected not_uploaded, got %s", code)
	}
	// Only its sender finishes it.
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, bob, "", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeUploadNotFound {
		t.Fatalf("expected upload_not_found, got %s", code)
	}

	blobs := signer.Memory
	key := signer.lastKey
	if err := blobs.Put(context.Background(), key, []byte("four"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeSizeMismatch {
		t.Fatalf("expected size_mismatch, got %s", code)
	}
	if err := blobs.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := blobs.Put(context.Background(), key, []byte("12345"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)
	if file.ID != ticket.ID || file.Size != 5 || file.Name != "big.bin" {
		t.Fatalf("unexpected finished attachment: %+v", file)
	}
	// Finishing again answers the same attachment.
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	if again := decodeAttachment(t, data); again.ID != file.ID || again.Size != file.Size {
		t.Fatalf("unexpected second finish: %+v", again)
	}

	status, data = h.request(http.MethodPost, "/attachments/uploads", map[string]any{
		"name": "huge.bin", "size": protocol.DirectUploadBytesMax + 1,
	}, alice, "", nil)
	h.expect(status, data, http.StatusRequestEntityTooLarge)
	if code := errorCode(t, data); code != protocol.CodeTooLarge {
		t.Fatalf("expected too_large, got %s", code)
	}
	status, data = h.request(http.MethodPost, "/attachments/uploads", map[string]any{
		"name": ".env", "size": 1,
	}, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)
	if code := errorCode(t, data); code != protocol.CodeInvalidAttachment {
		t.Fatalf("expected invalid_attachment, got %s", code)
	}
}

// uploadGate is a policy that answers only the anywhere seam.
type uploadGate struct {
	post bool
	sync bool
}

func (g uploadGate) Authorize(context.Context, string, protocol.Actor, string, protocol.Permission) error {
	return nil
}

func (g uploadGate) AuthorizeAnywhere(_ context.Context, _ string, _ protocol.Actor, permission protocol.Permission) error {
	switch permission {
	case protocol.PermissionPost:
		if g.post {
			return nil
		}
	case protocol.PermissionSync:
		if g.sync {
			return nil
		}
	}
	return protocol.Forbidden("not anywhere")
}

func TestUploadsUseAnywhereAuthorizer(t *testing.T) {
	const alice = "local:user:alice"
	t.Run("neither post nor sync", func(t *testing.T) {
		signer := &signingMemory{Memory: blob.NewMemory()}
		h := newHarness(t, server.Options{Blobs: signer, Authorize: uploadGate{}})
		status, data := h.upload(alice, "a.txt", "text/plain", []byte("x"))
		h.expect(status, data, http.StatusForbidden)
		status, data = h.request(http.MethodPost, "/attachments/uploads", map[string]any{
			"name": "a.txt", "size": 1,
		}, alice, "", nil)
		h.expect(status, data, http.StatusForbidden)
	})
	for _, gate := range []uploadGate{{post: true}, {sync: true}} {
		name := "post anywhere"
		if gate.sync {
			name = "sync anywhere"
		}
		t.Run(name, func(t *testing.T) {
			signer := &signingMemory{Memory: blob.NewMemory()}
			h := newHarness(t, server.Options{Blobs: signer, Authorize: gate})
			status, data := h.upload(alice, "a.txt", "text/plain", []byte("x"))
			h.expect(status, data, http.StatusCreated)
			status, data = h.request(http.MethodPost, "/attachments/uploads", map[string]any{
				"name": "a.txt", "size": 1,
			}, alice, "", nil)
			h.expect(status, data, http.StatusCreated)
		})
	}
}

func TestAttachmentsArePerTenant(t *testing.T) {
	signer := &signingMemory{Memory: blob.NewMemory()}
	h := newHarness(t, server.Options{Blobs: signer})
	const alice = "local:user:alice"

	status, data := h.uploadInTenant(alice, "tenant-a", "a.txt", "text/plain", []byte("tenant a"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)

	status, body, _ := h.attachmentInTenant(alice, "tenant-a", file.ID)
	h.expect(status, body, http.StatusOK)
	if string(body) != "tenant a" {
		t.Fatalf("tenant a read %q", body)
	}
	status, _, _ = h.attachmentInTenant(alice, "tenant-b", file.ID)
	if status != http.StatusNotFound {
		t.Fatalf("tenant b read another tenant's file: %d", status)
	}
	// Binding it on the other board finds nothing.
	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "", "attachments": []string{file.ID},
	}, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeAttachmentNotFound {
		t.Fatalf("expected attachment_not_found, got %s", code)
	}
	// Deleting on the other board is nothing, and doesn't touch this one.
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusNoContent)
	status, body, _ = h.attachmentInTenant(alice, "tenant-a", file.ID)
	h.expect(status, body, http.StatusOK)

	// A direct upload's ticket is the tenant's too.
	status, data = h.request(http.MethodPost, "/attachments/uploads", map[string]any{
		"name": "big.bin", "size": 1,
	}, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusCreated)
	ticket := decode[protocol.UploadTicket](t, data)
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "tenant-b", nil)
	h.expect(status, data, http.StatusNotFound)
	if code := errorCode(t, data); code != protocol.CodeUploadNotFound {
		t.Fatalf("expected upload_not_found, got %s", code)
	}
	if err := signer.Memory.Put(context.Background(), signer.lastKey, []byte("1"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	status, data = h.request(http.MethodPost, "/attachments/uploads/"+ticket.ID, nil, alice, "tenant-a", nil)
	h.expect(status, data, http.StatusCreated)
}

// flakyDeleteStore refuses the first data-file delete, as a store outage does.
type flakyDeleteStore struct {
	*blob.Memory
	fail bool
}

func (s *flakyDeleteStore) Delete(ctx context.Context, key string) error {
	if s.fail && !strings.HasSuffix(key, ".json") {
		return errors.New("store unavailable")
	}
	return s.Memory.Delete(ctx, key)
}

func TestAttachmentDeleteCanBeRetried(t *testing.T) {
	store := &flakyDeleteStore{Memory: blob.NewMemory(), fail: true}
	h := newHarness(t, server.Options{Blobs: store})
	const alice = "local:user:alice"
	status, data := h.upload(alice, "a.txt", "text/plain", []byte("x"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)

	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusInternalServerError)
	// Data is deleted before the record, so a failed data delete leaves the
	// record and the bytes: the file still reads, and a retry can finish.
	status, body, _ := h.attachment(alice, file.ID)
	h.expect(status, body, http.StatusOK)
	if string(body) != "x" {
		t.Fatalf("failed delete changed the bytes: %q", body)
	}
	store.fail = false
	status, data = h.request(http.MethodDelete, "/attachments/"+file.ID, nil, alice, "", nil)
	h.expect(status, data, http.StatusNoContent)
	status, _, _ = h.attachment(alice, file.ID)
	if status != http.StatusNotFound {
		t.Fatalf("deleted file still reads: %d", status)
	}
	for _, key := range store.Keys() {
		if strings.Contains(key, file.ID) {
			t.Fatalf("delete left %s behind", key)
		}
	}
}

func TestTenantEmptyIsDefault(t *testing.T) {
	blobs := blob.NewMemory()
	h := newHarness(t, server.Options{
		Blobs: blobs,
		Tenant: func(r *http.Request) (string, error) {
			return r.Header.Get("X-Board-Tenant"), nil
		},
	})
	const alice = "local:user:alice"

	status, data := h.upload(alice, "a.txt", "text/plain", []byte("default board"))
	h.expect(status, data, http.StatusCreated)
	file := decodeAttachment(t, data)
	status, body, _ := h.attachmentInTenant(alice, "default", file.ID)
	h.expect(status, body, http.StatusOK)
	if string(body) != "default board" {
		t.Fatalf("unexpected bytes: %q", body)
	}

	status, data = h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "on the default board",
	}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	message := decode[postBody](t, data).Message
	status, data = h.request(http.MethodGet, "/messages/"+message.ID, nil, alice, "default", nil)
	h.expect(status, data, http.StatusOK)
	if got := decode[protocol.ThreadView](t, data).Message; got.ID != message.ID {
		t.Fatalf("the default board didn't see the post: %+v", got)
	}
}
