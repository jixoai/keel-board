package syncstore

import (
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// WebhookOutcome classifies one webhook request's result with a bounded token.
// It is what an auditor and a dashboard may use in place of an error string,
// which could quote a platform's body or a header.
type WebhookOutcome string

const (
	// WebhookAccepted means the provider verified the request and the inbox
	// committed every record it kept (or the request was a challenge).
	WebhookAccepted WebhookOutcome = "accepted"
	// WebhookDisabled means the tenant owns the key but switched it off.
	WebhookDisabled WebhookOutcome = "disabled"
	// WebhookUnknownKey means the tenant owns no entry with that key.
	WebhookUnknownKey WebhookOutcome = "unknown_key"
	// WebhookUnregistered means the tenant's entry isn't registered in this
	// process yet.
	WebhookUnregistered WebhookOutcome = "unregistered"
	// WebhookPlatformMismatch means the stored entry and the provider disagree.
	WebhookPlatformMismatch WebhookOutcome = "platform_mismatch"
	// WebhookTenantRefused means the deployment's tenant resolver refused.
	WebhookTenantRefused WebhookOutcome = "tenant_refused"
	// WebhookTooLarge means the body exceeded the handler's cap.
	WebhookTooLarge WebhookOutcome = "too_large"
	// WebhookRejected means the provider refused the request (signature,
	// format, replay window).
	WebhookRejected WebhookOutcome = "rejected"
	// WebhookFailed means a store or internal failure; the platform retries.
	WebhookFailed WebhookOutcome = "failed"
)

// WebhookObservation is one webhook request's bounded result. Platform and Key
// are empty when the request never resolved them.
type WebhookObservation struct {
	Tenant   string
	Key      boardsync.Key
	Platform boardsync.Platform
	Outcome  WebhookOutcome
	// Status is the HTTP status the handler answered.
	Status int
	// Added and Skipped are what the inbox push did, when it ran.
	Added, Skipped int
	// Took is the handler's wall time.
	Took time.Duration
}

// Accepted reports whether the outcome stored or acknowledged the request.
func (o WebhookOutcome) Accepted() bool { return o == WebhookAccepted }

// WebhookObserver receives bounded facts about webhook handling. It is how a
// deployment feeds metrics and the durable audit without the handler importing
// either. Implementations must be fast and must not fail the request; the
// handler ignores what they return (they return nothing) and never lets an
// observation change its answer.
type WebhookObserver interface {
	// WebhookHandled reports one finished request.
	WebhookHandled(WebhookObservation)
	// WebhookDeduped reports how many records an inbox already held and
	// therefore skipped. It runs beside the successful push.
	WebhookDeduped(tenant string, key boardsync.Key, hits int)
}

// WorkerHooks receives bounded facts about sync work. Any nil field is not
// called. Implementations must not block a worker or change its result; a
// failure to record a hook is a logging concern, never a work item's failure.
type WorkerHooks struct {
	// InboxDead reports an inbound item that exhausted its retries.
	InboxDead func(tenant string, key boardsync.Key, platform boardsync.Platform, id string, attempts int, code string)
	// OutboxDead reports a delivery that exhausted its retries. target is the
	// key-prefixed external target and delivery the outbox row's ID; both are
	// platform identifiers, never message text.
	OutboxDead func(tenant string, key boardsync.Key, platform boardsync.Platform, delivery, target string, attempts int, code string)
	// DeliveryFailed reports one failed delivery attempt, whether it will be
	// retried or was skipped by policy. code is a syncstore.ErrorCode value.
	DeliveryFailed func(tenant string, key boardsync.Key, platform boardsync.Platform, code string)
}
