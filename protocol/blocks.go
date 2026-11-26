package protocol

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Blocks are JSON in a message's text, in a fence that names a schema:
//
//	```json#git-commit@1
//	{"sha": "…", "title": "…"}
//	```
//
// The text stays the message: a block is read out of it, never kept beside
// it. Upstream registers schemas and checks each block against the version its
// fence names. This port has no schema registry yet, so it knows the two
// built-in schemas and refuses a block that names any other.
//
// The scanner is deliberately smaller than upstream's Markdown-aware one: it
// reads top-level fences only, not fences inside quotes or list items.

// BlockSchema is a schema version blocks can name.
type BlockSchema struct {
	Name        string
	Version     int
	Description string
	Definition  map[string]any
}

// BuiltInBlockSchemas are the schemas every board has, as upstream's built-ins.
var BuiltInBlockSchemas = map[string]BlockSchema{
	"git-commit@1": {
		Name:        "git-commit",
		Version:     1,
		Description: "A Git commit, by its full SHA and the first line of its message.",
		Definition: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"sha", "title"},
			"properties": map[string]any{
				"sha":   map[string]any{"type": "string", "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$"},
				"title": map[string]any{"type": "string", "minLength": 1, "maxLength": 1024},
			},
		},
	},
	"vercel-deployment@1": {
		Name:        "vercel-deployment",
		Version:     1,
		Description: "A Vercel deployment, by its URL and optionally the commit it was built from.",
		Definition: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"url"},
			"properties": map[string]any{
				"url":    map[string]any{"type": "string", "format": "uri", "pattern": "^https://", "maxLength": 2048},
				"gitSha": map[string]any{"type": "string", "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$"},
			},
		},
	},
}

// FoundBlock is one block-shaped fence in a text.
type FoundBlock struct {
	// ID is what the fence names after `json#`.
	ID string
	// Name and Version are its schema's, when ID names one.
	Name    string
	Version int
	// Source is what's between the fences, as written.
	Source string
	// Data is the source's JSON object, when it has one.
	Data map[string]any
	// Problem says why it has no data.
	Problem string
}

var blockInfoExpr = regexp.MustCompile(`^json#(\S*)`)

// FindBlocks returns every top-level `json#…` fence in text, in order. A fence
// that never closes is a block with a problem.
func FindBlocks(text string) []FoundBlock {
	if !strings.Contains(text, "json#") {
		return nil
	}
	blocks := make([]FoundBlock, 0)
	lines := strings.SplitAfter(text, "\n")
	var open blockOpen
	for _, line := range lines {
		body := strings.TrimSuffix(line, "\n")
		if open.fence == 0 {
			if found := readFenceLine(body); found != nil {
				open = blockOpen{
					fence:   found.fence[0],
					length:  len(found.fence),
					info:    found.info,
					collect: strings.HasPrefix(found.info, "json#"),
				}
			}
			continue
		}
		if blockCloses(body, open.fence, open.length) {
			if open.collect {
				blocks = append(blocks, readBlock(open.info, open.source.String()))
			}
			open = blockOpen{}
			continue
		}
		open.source.WriteString(body)
		open.source.WriteString("\n")
	}
	if open.fence != 0 && open.collect {
		block := readBlock(open.info, strings.TrimSuffix(open.source.String(), "\n"))
		if block.Problem == "" {
			block.Problem = fmt.Sprintf("the %s block's fence never closes", block.ID)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

type blockOpen struct {
	fence   byte
	length  int
	info    string
	collect bool
	source  strings.Builder
}

type fenceLine struct {
	fence string
	info  string
}

// fenceInfo reads a line that opens a fence: up to three spaces, three or more
// backticks or tildes, and an info string. A backtick fence's info may not hold
// a backtick: that's inline code.
func readFenceLine(line string) *fenceLine {
	trimmed := strings.TrimLeft(line, " ")
	if len(line)-len(trimmed) > 3 {
		return nil
	}
	if len(trimmed) < 3 || (trimmed[0] != '`' && trimmed[0] != '~') {
		return nil
	}
	character := trimmed[0]
	length := 0
	for length < len(trimmed) && trimmed[length] == character {
		length++
	}
	if length < 3 {
		return nil
	}
	info := strings.TrimSpace(trimmed[length:])
	if character == '`' && strings.Contains(info, "`") {
		return nil
	}
	return &fenceLine{fence: trimmed[:length], info: info}
}

// blockCloses reports whether line closes a fence of length with the same
// character, and nothing after it but spaces.
func blockCloses(line string, character byte, length int) bool {
	trimmed := strings.TrimLeft(line, " ")
	if len(line)-len(trimmed) > 3 {
		return false
	}
	count := 0
	for count < len(trimmed) && trimmed[count] == character {
		count++
	}
	return count >= length && strings.TrimSpace(trimmed[count:]) == ""
}

// readBlock reads a fence's info and source into a block.
func readBlock(info, source string) FoundBlock {
	found := blockInfoExpr.FindStringSubmatch(info)
	id := ""
	if found != nil {
		id = found[1]
	}
	block := FoundBlock{ID: id, Source: source}
	name, version, ok := parseBlockID(id)
	if !ok {
		block.Problem = fmt.Sprintf("%s names no schema: one is name@version, like git-commit@1", displayBlockID(id))
		return block
	}
	block.Name, block.Version = name, version
	if _, known := BuiltInBlockSchemas[id]; !known {
		block.Problem = fmt.Sprintf("the %s block names a schema this board doesn't have", id)
		return block
	}
	var data any
	if err := json.Unmarshal([]byte(source), &data); err != nil {
		block.Problem = fmt.Sprintf("the %s block isn't JSON", id)
		return block
	}
	object, ok := data.(map[string]any)
	if !ok {
		block.Problem = fmt.Sprintf("the %s block isn't a JSON object", id)
		return block
	}
	block.Data = object
	return block
}

func displayBlockID(id string) string {
	if id == "" {
		return "json#"
	}
	return id
}

var blockIDExpr = regexp.MustCompile(`^([a-z][a-z0-9._-]{0,79})@([1-9][0-9]{0,4})$`)

// parseBlockID parses `name@version`.
func parseBlockID(id string) (string, int, bool) {
	found := blockIDExpr.FindStringSubmatch(id)
	if found == nil {
		return "", 0, false
	}
	version := 0
	for _, c := range found[2] {
		version = version*10 + int(c-'0')
	}
	return found[1], version, true
}

// ValidateBlocks checks every block a text carries, each against the schema
// its fence names. A text with no `json#` fence is no work.
func ValidateBlocks(text string) *Error {
	if !strings.Contains(text, "json#") {
		return nil
	}
	for _, block := range FindBlocks(text) {
		if block.Problem != "" {
			return NewError(CodeInvalidBlock, block.Problem)
		}
		schema := BuiltInBlockSchemas[block.ID]
		if problem := CheckValue(schema.Definition, block.Data); problem != "" {
			return NewError(CodeInvalidBlock, fmt.Sprintf("the %s block doesn't match its schema: %s", block.ID, problem))
		}
	}
	return nil
}

// BlocksIn returns the blocks a message's text shows: each fence that names a
// known schema and holds an object that matches it.
func BlocksIn(text string) []BlockView {
	found := FindBlocks(text)
	if len(found) == 0 {
		return []BlockView{}
	}
	blocks := make([]BlockView, 0, len(found))
	for _, block := range found {
		if block.Problem != "" || block.Data == nil {
			continue
		}
		blocks = append(blocks, BlockView{Schema: block.Name, Version: block.Version, Data: block.Data})
	}
	return blocks
}

// CheckValue checks value against a JSON Schema definition in the structural
// subset upstream allows. It returns the first problem, or "" when the value
// matches.
func CheckValue(schema any, value any) string {
	return checkValue(schema, value, "")
}

func checkValue(schema any, value any, path string) string {
	switch typed := schema.(type) {
	case bool:
		if !typed {
			return at(path, "nothing matches false")
		}
		return ""
	case map[string]any:
		schema = typed
	default:
		return ""
	}
	definition := schema.(map[string]any)
	if wanted, ok := definition["type"]; ok && !matchesType(wanted, value) {
		return at(path, fmt.Sprintf("it should be %s", describeType(wanted)))
	}
	if enum, ok := definition["enum"].([]any); ok {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return at(path, "it isn't one of the allowed values")
		}
	}
	if constant, ok := definition["const"]; ok && !reflect.DeepEqual(constant, value) {
		return at(path, "it isn't the required value")
	}
	if children, ok := definition["allOf"].([]any); ok {
		for _, child := range children {
			if problem := checkValue(child, value, path); problem != "" {
				return problem
			}
		}
	}
	if children, ok := definition["anyOf"].([]any); ok {
		matched := false
		for _, child := range children {
			if checkValue(child, value, path) == "" {
				matched = true
				break
			}
		}
		if !matched {
			return at(path, "it matches none of the allowed shapes")
		}
	}
	if children, ok := definition["oneOf"].([]any); ok {
		matches := 0
		for _, child := range children {
			if checkValue(child, value, path) == "" {
				matches++
			}
		}
		if matches != 1 {
			return at(path, "it should match exactly one shape")
		}
	}
	if child, ok := definition["not"]; ok && checkValue(child, value, path) == "" {
		return at(path, "it matches a shape it must not")
	}

	switch typed := value.(type) {
	case string:
		if problem := checkString(definition, typed, path); problem != "" {
			return problem
		}
	case float64:
		if problem := checkNumber(definition, typed, path); problem != "" {
			return problem
		}
	case []any:
		if problem := checkArray(definition, typed, path); problem != "" {
			return problem
		}
	case map[string]any:
		if problem := checkObject(definition, typed, path); problem != "" {
			return problem
		}
	}
	return ""
}

func checkString(definition map[string]any, value, path string) string {
	length := utf8.RuneCountInString(value)
	if min, ok := numberKeyword(definition["minLength"]); ok && length < int(min) {
		return at(path, fmt.Sprintf("it is shorter than %d characters", int(min)))
	}
	if max, ok := numberKeyword(definition["maxLength"]); ok && length > int(max) {
		return at(path, fmt.Sprintf("it is longer than %d characters", int(max)))
	}
	if pattern, ok := definition["pattern"].(string); ok {
		matched, err := regexp.MatchString(pattern, value)
		if err == nil && !matched {
			return at(path, "it doesn't match its pattern")
		}
	}
	if format, ok := definition["format"].(string); ok {
		if problem := checkFormat(format, value); problem != "" {
			return at(path, problem)
		}
	}
	return ""
}

func checkNumber(definition map[string]any, value float64, path string) string {
	if min, ok := numberKeyword(definition["minimum"]); ok && value < min {
		return at(path, fmt.Sprintf("it is below %v", min))
	}
	if max, ok := numberKeyword(definition["maximum"]); ok && value > max {
		return at(path, fmt.Sprintf("it is above %v", max))
	}
	if min, ok := numberKeyword(definition["exclusiveMinimum"]); ok && value <= min {
		return at(path, fmt.Sprintf("it should be above %v", min))
	}
	if max, ok := numberKeyword(definition["exclusiveMaximum"]); ok && value >= max {
		return at(path, fmt.Sprintf("it should be below %v", max))
	}
	if multiple, ok := numberKeyword(definition["multipleOf"]); ok && multiple != 0 {
		quotient := value / multiple
		if math.Abs(quotient-math.Round(quotient)) > 1e-9 {
			return at(path, fmt.Sprintf("it isn't a multiple of %v", multiple))
		}
	}
	return ""
}

func checkArray(definition map[string]any, value []any, path string) string {
	if min, ok := numberKeyword(definition["minItems"]); ok && len(value) < int(min) {
		return at(path, fmt.Sprintf("it has fewer than %d items", int(min)))
	}
	if max, ok := numberKeyword(definition["maxItems"]); ok && len(value) > int(max) {
		return at(path, fmt.Sprintf("it has more than %d items", int(max)))
	}
	if items, ok := definition["items"]; ok {
		for at, item := range value {
			if problem := checkValue(items, item, child(path, indexPath(at))); problem != "" {
				return problem
			}
		}
	}
	return ""
}

func checkObject(definition map[string]any, value map[string]any, path string) string {
	properties, _ := definition["properties"].(map[string]any)
	if required, ok := definition["required"].([]any); ok {
		for _, name := range required {
			key, ok := name.(string)
			if !ok {
				continue
			}
			if _, present := value[key]; !present {
				return at(path, fmt.Sprintf("%s is required", quotePath(key)))
			}
		}
	}
	for key, item := range value {
		if childSchema, ok := properties[key]; ok {
			if problem := checkValue(childSchema, item, child(path, quotePath(key))); problem != "" {
				return problem
			}
			continue
		}
		if additional, ok := definition["additionalProperties"]; ok {
			if allowed, isBool := additional.(bool); isBool {
				if !allowed {
					return at(path, fmt.Sprintf("%s isn't allowed", quotePath(key)))
				}
				continue
			}
			if problem := checkValue(additional, item, child(path, quotePath(key))); problem != "" {
				return problem
			}
		}
	}
	return ""
}

func checkFormat(format, value string) string {
	switch format {
	case "uri":
		if !strings.Contains(value, "://") {
			return "it isn't a URI"
		}
	case "date-time":
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return "it isn't a date-time"
		}
	case "date":
		if _, err := time.Parse("2006-01-02", value); err != nil {
			return "it isn't a date"
		}
	case "time":
		if _, err := time.Parse("15:04:05Z07:00", value); err != nil {
			return "it isn't a time"
		}
	case "email":
		if !strings.Contains(value, "@") || strings.ContainsAny(value, " \t\n") {
			return "it isn't an email"
		}
	case "uuid":
		if !regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).MatchString(value) {
			return "it isn't a UUID"
		}
	case "ipv4":
		if !regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`).MatchString(value) {
			return "it isn't an IPv4 address"
		}
	case "ipv6":
		if !strings.Contains(value, ":") {
			return "it isn't an IPv6 address"
		}
	case "hostname":
		if !regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$`).MatchString(value) {
			return "it isn't a hostname"
		}
	}
	return ""
}

func matchesType(wanted any, value any) bool {
	check := func(name string) bool {
		switch name {
		case "object":
			_, ok := value.(map[string]any)
			return ok
		case "array":
			_, ok := value.([]any)
			return ok
		case "string":
			_, ok := value.(string)
			return ok
		case "number":
			_, ok := value.(float64)
			return ok
		case "integer":
			number, ok := value.(float64)
			return ok && number == math.Trunc(number)
		case "boolean":
			_, ok := value.(bool)
			return ok
		case "null":
			return value == nil
		}
		return false
	}
	switch typed := wanted.(type) {
	case string:
		return check(typed)
	case []any:
		for _, name := range typed {
			if text, ok := name.(string); ok && check(text) {
				return true
			}
		}
	}
	return true
}

func describeType(wanted any) string {
	if name, ok := wanted.(string); ok {
		return name
	}
	parts := make([]string, 0)
	if names, ok := wanted.([]any); ok {
		for _, name := range names {
			if text, ok := name.(string); ok {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, " or ")
}

func numberKeyword(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		number, err := typed.Float64()
		return number, err == nil
	}
	return 0, false
}

func child(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func indexPath(at int) string {
	return fmt.Sprintf("[%d]", at)
}

func quotePath(key string) string {
	if strings.ContainsAny(key, ". []") {
		return fmt.Sprintf("%q", key)
	}
	return key
}

func at(path, problem string) string {
	if path == "" {
		return problem
	}
	return path + ": " + problem
}
