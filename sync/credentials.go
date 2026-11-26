package sync

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// timeNow is a package seam for tests. Nothing in sync's core depends on the
// wall clock; adapters take a Now function of their own.
var timeNow = time.Now

// Secret is a credential kept out of every string, JSON document and log line
// by default. Only the adapter that performs the HTTP call reveals it, at the
// call site.
//
// A Secret may be loaded from the environment or a keel secret store; it is
// never part of a Config's JSON form and never written to the board log.
type Secret string

// Redacted is what every formatting verb shows instead of the secret.
const Redacted = "[redacted]"

// String returns the redacted form.
func (s Secret) String() string { return Redacted }

// GoString returns the redacted form for %#v.
func (s Secret) GoString() string { return `sync.Secret("[redacted]")` }

// LogValue returns the redacted form for log/slog.
func (s Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// MarshalJSON writes the redacted form, so a Config can be logged as JSON.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(Redacted) }

// UnmarshalJSON reads a plain string. It exists so a secret can be loaded from
// configuration; it never writes one.
func (s *Secret) UnmarshalJSON(data []byte) error {
	var plain string
	if err := json.Unmarshal(data, &plain); err != nil {
		return err
	}
	*s = Secret(plain)
	return nil
}

// Reveal returns the secret for an adapter's HTTP call. Calling it is the
// explicit act that lets a credential leave this package.
func (s Secret) Reveal() string { return string(s) }

// Empty reports whether the secret is unset.
func (s Secret) Empty() bool { return s == "" }

// Set stores a secret. It is the only mutator, so a zero Config never holds a
// token by accident.
func (s *Secret) Set(value string) { *s = Secret(value) }

// SecretFromEnv reads a secret from getenv. A missing value is an empty Secret
// and an error that names the variable, never a value.
func SecretFromEnv(getenv func(string) string, name string) (Secret, error) {
	if getenv == nil {
		return "", fmt.Errorf("sync: no environment lookup for %s", name)
	}
	value := getenv(name)
	if value == "" {
		return "", fmt.Errorf("sync: %s is not set", name)
	}
	return Secret(value), nil
}

// RedactError makes sure an error's text carries no secret. Adapters call it
// before wrapping platform errors, whose messages may quote a URL or token.
func RedactError(err error, secrets ...Secret) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	for _, secret := range secrets {
		if secret.Empty() {
			continue
		}
		text = strings.ReplaceAll(text, secret.Reveal(), Redacted)
	}
	return errors.New(text)
}
