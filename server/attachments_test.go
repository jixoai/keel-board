package server_test

import (
	"bytes"
	"context"
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
}

func (s signingMemory) SignUpload(_ context.Context, key string, opts blob.SignOptions) (blob.SignedUpload, error) {
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

// attachment reads a file's bytes as actor.
func (h *harness) attachment(actor, id string) (int, []byte, http.Header) {
	h.t.Helper()
	request, err := http.NewRequest(http.MethodGet, h.http.URL+server.APIPrefix+"/attachments/"+id, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("X-Board-Actor", actor)
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
	policy := server.AuthorizerFunc(func(_ context.Context, actor protocol.Actor, channel string, permission protocol.Permission) error {
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

	h := newHarness(t, server.Options{Blobs: signingMemory{blob.NewMemory()}})
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

	blobs := h.blobs
	key := "attachments/" + ticket.ID
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
}
