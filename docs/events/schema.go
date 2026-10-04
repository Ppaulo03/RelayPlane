// Package eventschema embeds the JSON Schema of the tenant-facing events (the webhook body) and validates documents
// against it. It is used by tests: the schema is the written contract, and the examples next to it are what the SDKs and
// the Go code are checked against.
package eventschema

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed events.schema.json examples/*.json
var files embed.FS

var (
	once   sync.Once
	schema *jsonschema.Schema
	serr   error
)

func compile() (*jsonschema.Schema, error) {
	once.Do(func() {
		raw, err := files.ReadFile("events.schema.json")
		if err != nil {
			serr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			serr = err
			return
		}
		c := jsonschema.NewCompiler()
		c.AssertFormat() // date-time is part of the contract
		if serr = c.AddResource("events.schema.json", doc); serr != nil {
			return
		}
		schema, serr = c.Compile("events.schema.json")
	})
	return schema, serr
}

// Validate checks one webhook body against the contract.
func Validate(body []byte) error {
	s, err := compile()
	if err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return fmt.Errorf("not JSON: %w", err)
	}
	return s.Validate(v)
}

// Examples returns the example events by file name.
func Examples() (map[string][]byte, error) {
	entries, err := files.ReadDir("examples")
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, e := range entries {
		b, err := files.ReadFile("examples/" + e.Name())
		if err != nil {
			return nil, err
		}
		out[e.Name()] = b
	}
	return out, nil
}
