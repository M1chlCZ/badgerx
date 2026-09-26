// Package jsonx provides deterministic, strict JSON.
package jsonx

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
)

// RawMessage is an unparsed JSON value.
type RawMessage = jsontext.Value

// Marshal returns deterministic JSON with sorted object members.
func Marshal(value any) ([]byte, error) {
	return jsonv2.Marshal(value, jsonv2.Deterministic(true))
}

// MarshalIndent returns deterministic JSON with indentation.
func MarshalIndent(value any, prefix, indent string) ([]byte, error) {
	return jsonv2.Marshal(
		value,
		jsonv2.Deterministic(true),
		jsontext.WithIndentPrefix(prefix),
		jsontext.WithIndent(indent),
	)
}

// MarshalWrite writes deterministic JSON to output.
func MarshalWrite(output io.Writer, value any) error {
	return jsonv2.MarshalWrite(output, value, jsonv2.Deterministic(true))
}

// Unmarshal parses JSON and rejects duplicate object members.
func Unmarshal(data []byte, target any) error {
	return jsonv2.Unmarshal(data, target)
}

// UnmarshalStrict parses JSON and rejects unknown and duplicate object members.
func UnmarshalStrict(data []byte, target any) error {
	return jsonv2.Unmarshal(data, target, jsonv2.RejectUnknownMembers(true))
}

// UnmarshalRead parses JSON from input.
func UnmarshalRead(input io.Reader, target any) error {
	return jsonv2.UnmarshalRead(input, target)
}

// UnmarshalReadStrict parses JSON from input and rejects unknown and duplicate
// object members.
func UnmarshalReadStrict(input io.Reader, target any) error {
	return jsonv2.UnmarshalRead(input, target, jsonv2.RejectUnknownMembers(true))
}

// Valid reports whether data holds valid JSON.
func Valid(data []byte) bool {
	return jsontext.Value(data).IsValid()
}
