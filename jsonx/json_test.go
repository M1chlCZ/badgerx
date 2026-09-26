package jsonx_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/m1chlcz/badgerx/jsonx"
)

func TestMarshalIsDeterministic(t *testing.T) {
	t.Parallel()

	encoded, err := jsonx.Marshal(map[string]int{"z": 1, "a": 2})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(encoded) != `{"a":2,"z":1}` {
		t.Fatalf("Marshal = %s, want deterministic object", encoded)
	}
}

func TestUnmarshalRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()

	for name, data := range map[string][]byte{
		"duplicate name": []byte(`{"name":"Rex","name":"Max"}`),
		"invalid UTF-8":  []byte("{\"name\":\"\xff\"}"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var value map[string]string
			if err := jsonx.Unmarshal(data, &value); err == nil {
				t.Fatal("Unmarshal error = nil")
			}
		})
	}
}

func TestUnmarshalStrictRejectsUnknownMembers(t *testing.T) {
	t.Parallel()

	var value struct {
		Name string `json:"name"`
	}
	if err := jsonx.UnmarshalStrict([]byte(`{"name":"Rex","secret":true}`), &value); err == nil {
		t.Fatal("UnmarshalStrict error = nil")
	}
}

func TestUnmarshalReadConsumesEntireInput(t *testing.T) {
	t.Parallel()

	var value map[string]string
	if err := jsonx.UnmarshalRead(strings.NewReader(`{"name":"Rex"} {}`), &value); err == nil {
		t.Fatal("UnmarshalRead error = nil")
	}
}

func TestUnmarshalReadStrictRejectsUnknownMembers(t *testing.T) {
	t.Parallel()

	var value struct {
		Name string `json:"name"`
	}
	if err := jsonx.UnmarshalReadStrict(
		strings.NewReader(`{"name":"Rex","secret":true}`),
		&value,
	); err == nil {
		t.Fatal("UnmarshalReadStrict error = nil")
	}
}

func TestRawMessageRoundTrip(t *testing.T) {
	t.Parallel()

	want := jsonx.RawMessage(`{"name":"Rex"}`)
	encoded, err := jsonx.Marshal(struct {
		Value jsonx.RawMessage `json:"value"`
	}{Value: want})
	if err != nil {
		t.Fatalf("Marshal RawMessage: %v", err)
	}
	var decoded struct {
		Value jsonx.RawMessage `json:"value"`
	}
	if err = jsonx.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal RawMessage: %v", err)
	}
	if !bytes.Equal(decoded.Value, want) {
		t.Fatalf("RawMessage = %s, want %s", decoded.Value, want)
	}
}

func TestMarshalWriteDoesNotAppendNewline(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	if err := jsonx.MarshalWrite(&output, map[string]int{"value": 1}); err != nil {
		t.Fatalf("MarshalWrite: %v", err)
	}
	if output.String() != `{"value":1}` {
		t.Fatalf("MarshalWrite = %q", output.String())
	}
}

func TestValidUsesStrictSyntax(t *testing.T) {
	t.Parallel()

	if !jsonx.Valid([]byte(`{"name":"Rex"}`)) {
		t.Fatal("Valid rejected valid JSON")
	}
	if jsonx.Valid([]byte(`{"name":"Rex","name":"Max"}`)) {
		t.Fatal("Valid accepted duplicate object names")
	}
}
