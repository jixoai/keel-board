package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
)

func TestOpenBlobs(t *testing.T) {
	memory, err := openBlobs("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := memory.(*blob.Memory); !ok {
		t.Fatalf("an empty --blobs should be memory, got %T", memory)
	}
	dir := t.TempDir()
	files, err := openBlobs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := files.(*blob.FS); !ok {
		t.Fatalf("--blobs should be an FS store, got %T", files)
	}
}

// TestDevServerSmoke runs the same handler `board serve` runs over an FS blob
// store and walks a file through upload, read, and delete.
func TestDevServerSmoke(t *testing.T) {
	blobs, err := openBlobs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	board := log.New(log.Options{Store: store.NewMemory()})
	httpServer := httptest.NewServer(devHandler(logger, board, blobs))
	defer httpServer.Close()

	upload, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/attachments", bytes.NewReader([]byte("smoke")))
	if err != nil {
		t.Fatal(err)
	}
	upload.Header.Set("content-type", "text/plain")
	upload.Header.Set("x-board-filename", url.PathEscape("smoke.txt"))
	response, err := httpServer.Client().Do(upload)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("upload: %d %s", response.StatusCode, body)
	}
	var made protocol.AttachmentResponse
	if err := json.Unmarshal(body, &made); err != nil {
		t.Fatal(err)
	}
	if made.Attachment.Name != "smoke.txt" || made.Attachment.Size != 5 {
		t.Fatalf("unexpected attachment: %+v", made.Attachment)
	}

	read, err := http.Get(httpServer.URL + server.APIPrefix + "/attachments/" + made.Attachment.ID)
	if err != nil {
		t.Fatal(err)
	}
	readBody, _ := io.ReadAll(read.Body)
	read.Body.Close()
	if read.StatusCode != http.StatusOK || string(readBody) != "smoke" {
		t.Fatalf("read: %d %q", read.StatusCode, readBody)
	}
	if read.Header.Get("Cache-Control") != "private, max-age=31536000, immutable" {
		t.Fatalf("unexpected cache header: %q", read.Header.Get("Cache-Control"))
	}

	deleteRequest, err := http.NewRequest(http.MethodDelete, httpServer.URL+server.APIPrefix+"/attachments/"+made.Attachment.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := httpServer.Client().Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	deleted.Body.Close()
	if deleted.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %d", deleted.StatusCode)
	}
	gone, err := http.Get(httpServer.URL + server.APIPrefix + "/attachments/" + made.Attachment.ID)
	if err != nil {
		t.Fatal(err)
	}
	gone.Body.Close()
	if gone.StatusCode != http.StatusNotFound {
		t.Fatalf("after delete: %d", gone.StatusCode)
	}
}
