package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// Attachment lifecycle. A file is created by an upload, bound to a message by
// a post (or a task's root), read while its uploader or a reader of a message
// carrying it asks, and hidden when the last message carrying it is deleted.
// Its uploader may delete the bytes outright only while no live message binds
// them.
//
// The bytes and the record live in the configured blob.Store under keys the
// board chooses; a record's UploadedBy is the principal that sent it, which is
// what makes an unbound upload readable and deletable by its sender.

const (
	attachmentMetaSuffix = ".json"
	directUploadTTL      = 30 * time.Minute
	attachmentTicketType = "application/json"
)

// attachmentRecord is what the board remembers about a file.
type attachmentRecord struct {
	ID         string    `json:"id"`
	Tenant     string    `json:"tenant"`
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	Size       int64     `json:"size"`
	UploadedBy string    `json:"uploadedBy"`
	CreatedAt  time.Time `json:"createdAt"`
}

// uploadTicket is a signed direct upload waiting for its bytes.
type uploadTicket struct {
	ID         string    `json:"id"`
	Tenant     string    `json:"tenant"`
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	Size       int64     `json:"size"`
	UploadedBy string    `json:"uploadedBy"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

// tenantPart names a tenant in a blob key without trusting its bytes: a
// custom TenantFunc may return anything, and blobs live in one store. A hash
// keeps every tenant's files under its own prefix.
func tenantPart(tenant string) string {
	sum := sha256.Sum256([]byte(tenant))
	return hex.EncodeToString(sum[:])
}

func attachmentDataKey(tenant, id string) string {
	return "tenants/" + tenantPart(tenant) + "/attachments/" + id
}

func attachmentMetaKey(tenant, id string) string {
	return "tenants/" + tenantPart(tenant) + "/attachments/" + id + attachmentMetaSuffix
}

func uploadTicketKey(tenant, id string) string {
	return "tenants/" + tenantPart(tenant) + "/uploads/" + id + attachmentMetaSuffix
}

func (r attachmentRecord) view() protocol.Attachment {
	return protocol.Attachment{ID: r.ID, Name: r.Name, Type: r.Type, Size: r.Size}
}

// noFiles is the answer for an attachment route on a board that keeps no
// files.
func noFiles() *protocol.Error {
	return protocol.NewError(protocol.CodeNoFiles, "this board keeps no files")
}

// permitUpload asks an optional AnywhereAuthorizer whether the caller may post
// or sync somewhere. A board whose policy doesn't implement the seam keeps its
// old behavior: uploads are authenticated only, and binding a file to a
// message still needs `post` there.
func (s *Server) permitUpload(req *request) *protocol.Error {
	gate, ok := s.authorize.(AnywhereAuthorizer)
	if !ok {
		return nil
	}
	postErr := gate.AuthorizeAnywhere(req.ctx, req.tenant, req.actor, protocol.PermissionPost)
	if postErr == nil {
		return nil
	}
	syncErr := gate.AuthorizeAnywhere(req.ctx, req.tenant, req.actor, protocol.PermissionSync)
	if syncErr == nil {
		return nil
	}
	// A refusal is the answer; a bad scope, a canceled request, or a store
	// failure isn't hidden as a 403.
	for _, err := range []error{postErr, syncErr} {
		boardErr := asProtocol(err)
		if boardErr.Status != http.StatusForbidden && boardErr.Status != http.StatusNotFound {
			return boardErr
		}
	}
	return protocol.Forbidden("this token can't post or sync anywhere, so it can't upload")
}

// newAttachmentID returns 128 random bits in base64url. An ID starting with
// "-" is drawn again: a command line reads it as a flag.
func newAttachmentID() (string, error) {
	for {
		buffer := make([]byte, 16)
		if _, err := rand.Read(buffer); err != nil {
			return "", err
		}
		id := base64.RawURLEncoding.EncodeToString(buffer)
		if !strings.HasPrefix(id, "-") {
			return id, nil
		}
	}
}

// saveAttachmentRecord writes a record, refusing to overwrite one. It returns
// the record already stored when the key was taken, so a retried finish
// answers what landed.
func (s *Server) saveAttachmentRecord(r *http.Request, tenant string, record attachmentRecord) (attachmentRecord, error) {
	return putAttachmentRecord(r.Context(), s.blobs, tenant, record)
}

// putAttachmentRecord is saveAttachmentRecord for a caller outside an HTTP
// request, such as an inbound sync import.
func putAttachmentRecord(ctx context.Context, blobs blob.Store, tenant string, record attachmentRecord) (attachmentRecord, error) {
	record.Tenant = tenant
	encoded, err := json.Marshal(record)
	if err != nil {
		return attachmentRecord{}, err
	}
	err = blobs.Put(ctx, attachmentMetaKey(tenant, record.ID), encoded, attachmentTicketType)
	if err == nil {
		return record, nil
	}
	existing, readErr := readAttachmentRecord(ctx, blobs, tenant, record.ID)
	if readErr == nil {
		return existing, nil
	}
	return attachmentRecord{}, err
}

// loadAttachmentRecord reads a file's record, or ErrNotFound. A record from
// another tenant is not found, even if a caller names its ID.
func (s *Server) loadAttachmentRecord(r *http.Request, tenant, id string) (attachmentRecord, error) {
	return readAttachmentRecord(r.Context(), s.blobs, tenant, id)
}

// readAttachmentRecord is loadAttachmentRecord for a caller outside an HTTP
// request.
func readAttachmentRecord(ctx context.Context, blobs blob.Store, tenant, id string) (attachmentRecord, error) {
	data, _, err := blobs.Get(ctx, attachmentMetaKey(tenant, id))
	if err != nil {
		return attachmentRecord{}, err
	}
	var record attachmentRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return attachmentRecord{}, fmt.Errorf("attachment %s: bad record: %w", id, err)
	}
	if record.Tenant != tenant {
		return attachmentRecord{}, blob.ErrNotFound
	}
	return record, nil
}

// liveAttachmentRefs returns the messages that still carry a file.
func liveAttachmentRefs(snapshot *log.Snapshot, id string) []*log.Message {
	var refs []*log.Message
	for _, message := range snapshot.AllMessages() {
		if message.Deleted {
			continue
		}
		for _, attachment := range message.Attachments {
			if attachment.ID == id {
				refs = append(refs, message)
				break
			}
		}
	}
	return refs
}

// attachmentReadable reports whether a request may read a file: its uploader
// may, and so may a reader of any live message carrying it.
func (s *Server) attachmentReadable(req *request, snapshot *log.Snapshot, record attachmentRecord) bool {
	if record.UploadedBy != "" && record.UploadedBy == req.actor.Principal {
		return true
	}
	for _, message := range liveAttachmentRefs(snapshot, record.ID) {
		if req.can(s, message.Channel, protocol.PermissionRead) {
			return true
		}
	}
	return false
}

// bindAttachmentIDs resolves file IDs a write carries, deduplicated, refusing
// one the caller may not read as if it didn't exist. It enforces no per-message
// count: a batch binds several messages' worth at once.
func (s *Server) bindAttachmentIDs(req *request, ids []string) (map[string]protocol.Attachment, *protocol.Error) {
	bound := make(map[string]protocol.Attachment, len(ids))
	if len(ids) == 0 {
		return bound, nil
	}
	if s.blobs == nil {
		return nil, noFiles()
	}
	for _, id := range ids {
		if err := protocol.ValidateAttachmentID(id); err != nil {
			return nil, err
		}
	}
	// Read the records before taking the board's lock: blob I/O stays outside
	// the log's global lock, and only the message bindings need the fold.
	records := make(map[string]attachmentRecord, len(ids))
	for _, id := range ids {
		if _, done := records[id]; done {
			continue
		}
		record, err := s.loadAttachmentRecord(req.http, req.tenant, id)
		if errors.Is(err, blob.ErrNotFound) {
			return nil, protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id))
		}
		if err != nil {
			return nil, protocol.NewError(protocol.CodeInternal, "could not read attachments")
		}
		records[id] = record
	}
	var failure *protocol.Error
	err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		for _, id := range ids {
			record := records[id]
			if !s.attachmentReadable(req, snapshot, record) {
				failure = protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id))
				return nil
			}
			bound[id] = record.view()
		}
		return nil
	})
	if err != nil {
		return nil, protocol.NewError(protocol.CodeInternal, "could not read attachments")
	}
	if failure != nil {
		return nil, failure
	}
	return bound, nil
}

// bindAttachments resolves the file IDs one message carries, in request order
// and without duplicates.
func (s *Server) bindAttachments(req *request, ids []string) ([]protocol.Attachment, *protocol.Error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := protocol.ValidateAttachmentIDs(ids); err != nil {
		return nil, err
	}
	byID, perr := s.bindAttachmentIDs(req, ids)
	if perr != nil {
		return nil, perr
	}
	bound := make([]protocol.Attachment, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		bound = append(bound, byID[id])
	}
	return bound, nil
}

// verifyAttachments re-reads the records a write bound, under the write's
// board lock: a delete that won the race leaves nothing to bind, and the write
// answers 404 rather than referencing bytes that are gone.
func (s *Server) verifyAttachments(req *request, attachments []protocol.Attachment) *protocol.Error {
	for _, attachment := range attachments {
		if _, err := s.loadAttachmentRecord(req.http, req.tenant, attachment.ID); errors.Is(err, blob.ErrNotFound) {
			return protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", attachment.ID))
		} else if err != nil {
			return protocol.NewError(protocol.CodeInternal, "could not read attachments")
		}
	}
	return nil
}

// handleUpload answers POST /attachments: one file's bytes in the request
// body, with its name in X-Board-Filename and its type in Content-Type.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if s.blobs == nil {
		writeError(w, noFiles())
		return
	}
	if err := s.permitUpload(req); err != nil {
		writeError(w, err)
		return
	}
	name := "file"
	if header := r.Header.Get("X-Board-Filename"); header != "" {
		decoded, err := url.PathUnescape(header)
		if err != nil {
			writeError(w, protocol.Invalid("`x-board-filename` is not URI-encoded"))
			return
		}
		name = decoded
	}
	if err := protocol.ValidateFileName(name); err != nil {
		writeError(w, err)
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, protocol.AttachmentBytesMax+1))
	if err != nil {
		writeError(w, protocol.Invalid("the upload couldn't be read"))
		return
	}
	if len(data) == 0 {
		writeError(w, protocol.Invalid("the file is empty"))
		return
	}
	if len(data) > protocol.AttachmentBytesMax {
		writeError(w, protocol.NewError(protocol.CodeTooLarge,
			fmt.Sprintf("attachments are at most %d MB", protocol.AttachmentBytesMax/1024/1024)))
		return
	}
	id, err := newAttachmentID()
	if err != nil {
		s.fail(w, r, err)
		return
	}
	contentType := protocol.MediaType(r.Header.Get("Content-Type"))
	record := attachmentRecord{
		ID:         id,
		Name:       protocol.SanitizeFileName(name),
		Type:       contentType,
		Size:       int64(len(data)),
		UploadedBy: req.actor.Principal,
		CreatedAt:  s.now().UTC(),
	}
	if err := s.blobs.Put(req.ctx, attachmentDataKey(req.tenant, id), data, contentType); err != nil {
		s.fail(w, r, err)
		return
	}
	record, err = s.saveAttachmentRecord(r, req.tenant, record)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The record is the answer only once the store really has that many bytes.
	size, err := s.blobs.Stat(req.ctx, attachmentDataKey(req.tenant, id))
	if err != nil || size != record.Size {
		s.fail(w, r, errOrMismatch(err, size, record.Size))
		return
	}
	writeJSON(w, http.StatusCreated, protocol.AttachmentResponse{Attachment: record.view()})
}

// errOrMismatch is the failure of a stored size that doesn't match its record.
func errOrMismatch(err error, have, want int64) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("attachment store has %d bytes, want %d", have, want)
}

// handleAttachment answers GET /attachments/{id}: the file's bytes, with
// headers that keep it from running as a page.
func (s *Server) handleAttachment(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if s.blobs == nil {
		writeError(w, noFiles())
		return
	}
	id := r.PathValue("id")
	if err := protocol.ValidateAttachmentID(id); err != nil {
		writeError(w, protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id)))
		return
	}
	record, err := s.loadAttachmentRecord(r, req.tenant, id)
	if errors.Is(err, blob.ErrNotFound) {
		writeError(w, protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id)))
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var readable bool
	err = s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
		readable = s.attachmentReadable(req, snapshot, record)
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !readable {
		writeError(w, protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id)))
		return
	}
	data, contentType, err := s.blobs.Get(req.ctx, attachmentDataKey(req.tenant, id))
	if errors.Is(err, blob.ErrNotFound) {
		writeError(w, protocol.NotFound(protocol.CodeAttachmentNotFound, fmt.Sprintf("no attachment: %s", id)))
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if contentType == "" {
		contentType = record.Type
	}
	disposition := "attachment"
	if inlineAttachment(contentType) {
		disposition = "inline"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename*=UTF-8''%s", disposition, url.PathEscape(record.Name)))
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// inlineAttachment reports whether a browser can't run a type, so it may show
// inline. Everything else downloads.
func inlineAttachment(contentType string) bool {
	switch {
	case contentType == "image/png", contentType == "image/jpeg", contentType == "image/gif",
		contentType == "image/webp", contentType == "image/avif":
		return true
	case contentType == "video/mp4", contentType == "video/webm":
		return true
	case contentType == "text/plain":
		return true
	}
	return false
}

// handleDeleteAttachment answers DELETE /attachments/{id}: its uploader
// removes a file no live message carries. Deleting one already gone is
// nothing, so a retry succeeds.
func (s *Server) handleDeleteAttachment(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if s.blobs == nil {
		writeError(w, noFiles())
		return
	}
	id := r.PathValue("id")
	if err := protocol.ValidateAttachmentID(id); err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	record, err := s.loadAttachmentRecord(r, req.tenant, id)
	if errors.Is(err, blob.ErrNotFound) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if record.UploadedBy != req.actor.Principal {
		writeError(w, protocol.Forbidden("only its uploader may delete an attachment"))
		return
	}
	// The binding check and the delete run under the board's lock, so a write
	// that binds the file either lands before this check or waits until its
	// bytes are gone and finds no record.
	err = s.board.Update(req.ctx, req.tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		if refs := liveAttachmentRefs(tx.Read(), id); len(refs) > 0 {
			return protocol.NewError(protocol.CodeAttachmentInUse,
				fmt.Sprintf("%s is on %d message(s): delete them before its bytes", id, len(refs)))
		}
		// Data first, then its record: a store that fails midway leaves the
		// record, so the same delete can be retried.
		if err := s.blobs.Delete(req.ctx, attachmentDataKey(req.tenant, id)); err != nil {
			return err
		}
		return s.blobs.Delete(req.ctx, attachmentMetaKey(req.tenant, id))
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleUploadStart answers POST /attachments/uploads: a signed URL a client
// PUTs a large file's bytes to, for a store that can sign one.
func (s *Server) handleUploadStart(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if s.blobs == nil {
		writeError(w, noFiles())
		return
	}
	if err := s.permitUpload(req); err != nil {
		writeError(w, err)
		return
	}
	signer, ok := s.blobs.(blob.UploadSigner)
	if !ok {
		writeError(w, protocol.NewError(protocol.CodeNoDirectUploads,
			fmt.Sprintf("this board's blob store can't take direct uploads, so files are at most %d MB", protocol.AttachmentBytesMax/1024/1024)))
		return
	}
	var body protocol.UploadStartRequest
	if err := decodeBody(r, &body, true, false); err != nil {
		writeError(w, err)
		return
	}
	if body.Size <= 0 {
		writeError(w, protocol.Invalid("size: a file is at least one byte"))
		return
	}
	if body.Size > protocol.DirectUploadBytesMax {
		writeError(w, protocol.NewError(protocol.CodeTooLarge,
			fmt.Sprintf("direct uploads are at most %d MB", protocol.DirectUploadBytesMax/1024/1024)))
		return
	}
	name := "file"
	if body.Name != nil {
		name = *body.Name
	}
	if err := protocol.ValidateFileName(name); err != nil {
		writeError(w, err)
		return
	}
	contentType := "application/octet-stream"
	if body.Type != nil {
		contentType = protocol.MediaType(*body.Type)
	}
	id, err := newAttachmentID()
	if err != nil {
		s.fail(w, r, err)
		return
	}
	expires := s.now().UTC().Add(directUploadTTL)
	ticket := uploadTicket{
		ID:         id,
		Tenant:     req.tenant,
		Name:       protocol.SanitizeFileName(name),
		Type:       contentType,
		Size:       body.Size,
		UploadedBy: req.actor.Principal,
		ExpiresAt:  expires,
	}
	encoded, err := json.Marshal(ticket)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.blobs.Put(req.ctx, uploadTicketKey(req.tenant, id), encoded, attachmentTicketType); err != nil {
		s.fail(w, r, err)
		return
	}
	signed, err := signer.SignUpload(req.ctx, attachmentDataKey(req.tenant, id), blob.SignOptions{
		ContentType: contentType,
		MaxBytes:    body.Size,
		ExpiresAt:   expires,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	headers := make(map[string]string, len(signed.Headers)+1)
	for key, value := range signed.Headers {
		headers[key] = value
	}
	headers["content-type"] = contentType
	writeJSON(w, http.StatusCreated, protocol.UploadTicket{
		ID:        id,
		URL:       signed.URL,
		Headers:   headers,
		ExpiresAt: protocol.FormatTime(expires),
	})
}

// handleUploadFinish answers POST /attachments/uploads/{id}: once the store
// has exactly the bytes the start named, the board records the attachment.
func (s *Server) handleUploadFinish(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if s.blobs == nil {
		writeError(w, noFiles())
		return
	}
	id := r.PathValue("id")
	if err := protocol.ValidateAttachmentID(id); err != nil {
		writeError(w, protocol.NotFound(protocol.CodeUploadNotFound, fmt.Sprintf("no upload: %s", id)))
		return
	}
	ticketData, _, err := s.blobs.Get(req.ctx, uploadTicketKey(req.tenant, id))
	if errors.Is(err, blob.ErrNotFound) {
		writeError(w, protocol.NotFound(protocol.CodeUploadNotFound, fmt.Sprintf("no upload: %s", id)))
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var ticket uploadTicket
	if err := json.Unmarshal(ticketData, &ticket); err != nil {
		s.fail(w, r, err)
		return
	}
	if ticket.Tenant != req.tenant || (ticket.UploadedBy != "" && ticket.UploadedBy != req.actor.Principal) {
		writeError(w, protocol.NotFound(protocol.CodeUploadNotFound, fmt.Sprintf("no upload: %s", id)))
		return
	}
	// A second finish answers what the first recorded.
	if existing, err := s.loadAttachmentRecord(r, req.tenant, id); err == nil {
		writeJSON(w, http.StatusCreated, protocol.AttachmentResponse{Attachment: existing.view()})
		return
	}
	size, err := s.blobs.Stat(req.ctx, attachmentDataKey(req.tenant, id))
	if errors.Is(err, blob.ErrNotFound) {
		writeError(w, protocol.NewError(protocol.CodeNotUploaded,
			fmt.Sprintf("nothing's been uploaded for %s yet: PUT the file to its URL first", id)))
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if size != ticket.Size {
		writeError(w, protocol.NewError(protocol.CodeSizeMismatch,
			fmt.Sprintf("the store has %d bytes for %s, but its upload said %d", size, id, ticket.Size)))
		return
	}
	record := attachmentRecord{
		ID:         id,
		Tenant:     req.tenant,
		Name:       ticket.Name,
		Type:       ticket.Type,
		Size:       size,
		UploadedBy: ticket.UploadedBy,
		CreatedAt:  s.now().UTC(),
	}
	record, err = s.saveAttachmentRecord(r, req.tenant, record)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	stored, err := s.blobs.Stat(req.ctx, attachmentDataKey(req.tenant, id))
	if err != nil || stored != record.Size {
		s.fail(w, r, errOrMismatch(err, stored, record.Size))
		return
	}
	writeJSON(w, http.StatusCreated, protocol.AttachmentResponse{Attachment: record.view()})
}
