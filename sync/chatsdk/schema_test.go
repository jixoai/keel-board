package chatsdk_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"unicode/utf8"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

// TestFixtureWireMatchesSchema runs a representative wire document for each
// direction through the checked-in JSON schema. It catches a Go field, a
// limit, or a required field drifting away from the contract a sidecar reads.
func TestFixtureWireMatchesSchema(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "bridge-v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	check := func(name, definition string, value any) {
		t.Helper()
		document, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var decoded any
		if err := json.Unmarshal(document, &decoded); err != nil {
			t.Fatal(err)
		}
		entry := map[string]any{"$ref": "#/$defs/" + definition}
		if err := validateSchema(schema, entry, decoded, name); err != nil {
			t.Fatalf("%s doesn't match the schema: %v\nwire: %s", name, err, document)
		}
	}
	author := boardsync.Author{ID: "telegram:user:7", Handle: "ada", Name: "Ada", Avatar: "https://example.test/a.png", Bot: false}
	message := boardsync.Event{
		ID: "m1", Key: "telegram", Platform: "telegram",
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "-100", Thread: "-100:42", Message: "42", IsRoot: true, Text: "hello",
		Author: author, Context: "src/a.go:1",
		Files:     []boardsync.File{{Type: "file", Name: "a.txt", MIMEType: "text/plain", Size: 3, URL: "https://example.test/a.txt", FetchMetadata: map[string]string{"id": "1"}}},
		Links:     []boardsync.Link{{URL: "https://example.test/b", Thread: "-100:9", Message: "9", Available: true}},
		Mentioned: []boardsync.Author{{ID: "telegram:user:8", Name: "Bob"}},
		Raw:       json.RawMessage(`{"update_id":1}`),
	}
	lifecycle := boardsync.Event{
		ID: "l1", Key: "telegram", Platform: "telegram",
		Kind: boardsync.KindLifecycle, Type: "pr.opened",
		Channel: "-100", Thread: "-100:7", Author: author,
		Lifecycle: &boardsync.Lifecycle{
			Type: "pr.opened", Title: "Checkout", URL: "https://example.test/pr/7", Body: "Retries.",
			Labels: map[string]string{"state": "open"}, Unset: []string{"review"}, Owns: []string{"state"},
			Reply: "opened by ada", CreateRoot: true, Retitled: false,
		},
	}
	check("inbound", "inboundBatch", chatsdk.InboundBatch{
		Version: chatsdk.BridgeVersion,
		Events: []chatsdk.InboundEvent{
			{Dedupe: "m1", Event: message},
			{Dedupe: "l1", Event: lifecycle},
		},
	})
	sender := &boardsync.Sender{Name: "Ada", Avatar: "https://example.test/a.png", Attribution: ": via support"}
	target := boardsync.ResolvedTarget{Key: "telegram", Platform: "telegram", Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread}
	check("deliver message", "deliverRequest", chatsdk.DeliverRequest{
		DeliveryID: "delivery:1", Key: "telegram", Target: target, Kind: chatsdk.DeliverMessage,
		Body: chatsdk.OutboundBody{Markdown: "**Ada**: hi", Sender: sender, External: true, BoardMessage: 9, BoardThread: 9},
	})
	check("deliver edit", "deliverRequest", chatsdk.DeliverRequest{
		DeliveryID: "delivery:2", Key: "telegram", Target: target, Kind: chatsdk.DeliverEdit,
		Body: chatsdk.OutboundBody{Markdown: "edited"}, Message: "43",
	})
	check("deliver delete", "deliverRequest", chatsdk.DeliverRequest{
		DeliveryID: "delivery:3", Key: "telegram", Target: target, Kind: chatsdk.DeliverDelete, Message: "43",
	})
	check("deliver reaction", "deliverRequest", chatsdk.DeliverRequest{
		DeliveryID: "delivery:4", Key: "telegram", Target: target, Kind: chatsdk.DeliverReaction,
		Message: "43", Emoji: "thumbsup", Added: true,
	})
	check("resolve name", "resolveRequest", chatsdk.ResolveRequest{Name: "#support"})
	check("resolve target", "resolveRequest", chatsdk.ResolveRequest{Target: "-100:42"})
	check("resolve response", "resolveResponse", chatsdk.ResolveResponse{Target: &boardsync.ResolvedTarget{
		Channel: "-100", Thread: "-100:42", Kind: boardsync.TargetThread,
	}})
	check("error", "error", chatsdk.BridgeError{Code: chatsdk.ErrorRateLimited, Message: "slow down", RetryAfter: 2})
}

// validateSchema implements the subset of JSON Schema the bridge contract
// uses: $ref, type, required, properties, additionalProperties, items,
// maxItems, maxLength, minLength, pattern, enum, const, oneOf, allOf, if/then.
func validateSchema(root, schema any, value any, path string) error {
	object, ok := schema.(map[string]any)
	if !ok {
		return nil
	}
	if ref, ok := object["$ref"].(string); ok {
		resolved, err := resolveRef(root, ref)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return validateSchema(root, resolved, value, path)
	}
	if want, ok := object["const"]; ok && !reflect.DeepEqual(want, value) {
		return fmt.Errorf("%s: %v isn't const %v", path, value, want)
	}
	if values, ok := object["enum"].([]any); ok {
		matched := false
		for _, want := range values {
			if reflect.DeepEqual(want, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: %v isn't in enum", path, value)
		}
	}
	if alternatives, ok := object["oneOf"].([]any); ok {
		matched := 0
		for _, alternative := range alternatives {
			if validateSchema(root, alternative, value, path) == nil {
				matched++
			}
		}
		if matched != 1 {
			return fmt.Errorf("%s: %d oneOf branches matched, want 1", path, matched)
		}
	}
	if all, ok := object["allOf"].([]any); ok {
		for _, part := range all {
			if err := validateSchema(root, part, value, path); err != nil {
				return err
			}
		}
	}
	if condition, ok := object["if"].(map[string]any); ok {
		if validateSchema(root, condition, value, path) == nil {
			if then, ok := object["then"]; ok {
				if err := validateSchema(root, then, value, path); err != nil {
					return err
				}
			}
		}
	}
	if want, ok := object["type"].(string); ok && !typeMatches(want, value) {
		return fmt.Errorf("%s: %T isn't %s", path, value, want)
	}
	if text, ok := value.(string); ok {
		if max, ok := number(object["maxLength"]); ok && utf8.RuneCountInString(text) > int(max) {
			return fmt.Errorf("%s: longer than %d runes", path, int(max))
		}
		if min, ok := number(object["minLength"]); ok && utf8.RuneCountInString(text) < int(min) {
			return fmt.Errorf("%s: shorter than %d runes", path, int(min))
		}
		if pattern, ok := object["pattern"].(string); ok {
			matched, err := regexp.MatchString(pattern, text)
			if err != nil {
				return fmt.Errorf("%s: pattern: %w", path, err)
			}
			if !matched {
				return fmt.Errorf("%s: %q doesn't match %s", path, text, pattern)
			}
		}
	}
	switch typed := value.(type) {
	case map[string]any:
		properties, _ := object["properties"].(map[string]any)
		if required, ok := object["required"].([]any); ok {
			for _, name := range required {
				if _, present := typed[name.(string)]; !present {
					return fmt.Errorf("%s: missing required %q", path, name)
				}
			}
		}
		for name, child := range typed {
			if property, ok := properties[name]; ok {
				if err := validateSchema(root, property, child, path+"."+name); err != nil {
					return err
				}
				continue
			}
			switch extra := object["additionalProperties"].(type) {
			case bool:
				if !extra {
					return fmt.Errorf("%s: unexpected property %q", path, name)
				}
			case map[string]any:
				if err := validateSchema(root, extra, child, path+"."+name); err != nil {
					return err
				}
			}
		}
	case []any:
		if max, ok := number(object["maxItems"]); ok && len(typed) > int(max) {
			return fmt.Errorf("%s: more than %d items", path, int(max))
		}
		if items, ok := object["items"]; ok {
			for i, item := range typed {
				if err := validateSchema(root, items, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func resolveRef(root any, ref string) (any, error) {
	const prefix = "#/$defs/"
	if len(ref) <= len(prefix) || ref[:len(prefix)] != prefix {
		return nil, fmt.Errorf("unsupported ref %q", ref)
	}
	defs, _ := root.(map[string]any)["$defs"].(map[string]any)
	target, ok := defs[ref[len(prefix):]]
	if !ok {
		return nil, fmt.Errorf("unknown ref %q", ref)
	}
	return target, nil
}

func typeMatches(want string, value any) bool {
	switch want {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer", "number":
		_, ok := value.(float64)
		return ok
	default:
		return true
	}
}

func number(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok
}
