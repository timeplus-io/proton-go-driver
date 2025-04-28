package chcol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNestedMap(t *testing.T) {
	cases := []struct {
		name     string
		input    *JSON
		expected map[string]interface{}
	}{
		{
			name: "nested object with values present",
			input: &JSON{
				valuesByPath: map[string]interface{}{
					"x":       NewVariant(nil),
					"x.a":     NewVariant(42),
					"x.b":     NewVariant(64),
					"x.b.c.d": NewVariant(96),
					"a.b.c":   NewVariant(128),
				},
			},
			expected: map[string]interface{}{
				"x": map[string]interface{}{
					"a": NewVariant(42),
					"b": NewVariant(64),
					"c": map[string]interface{}{
						"d": NewVariant(96),
					},
				},
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": NewVariant(128),
					},
				},
			},
		},
		{
			name: "nested object with only top level path present",
			input: &JSON{
				valuesByPath: map[string]interface{}{
					"x":       NewVariant(42),
					"x.a":     NewVariant(nil),
					"x.b":     NewVariant(nil),
					"x.b.c.d": NewVariant(nil),
					"a.b.c":   NewVariant(nil),
				},
			},
			expected: map[string]interface{}{
				"x": NewVariant(42),
			},
		},
		{
			name: "nested object with typed paths",
			input: &JSON{
				valuesByPath: map[string]interface{}{
					"x":   42,
					"a.b": "test value",
				},
			},
			expected: map[string]interface{}{
				"x": 42,
				"a": map[string]interface{}{
					"b": "test value",
				},
			},
		},
		{
			name:     "empty object",
			input:    NewJSON(),
			expected: map[string]interface{}{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := c.input.NestedMap()
			require.Equal(t, c.expected, actual)
		})
	}
}

func TestJSONMarshal(t *testing.T) {
	obj := &JSON{
		valuesByPath: map[string]interface{}{
			"x.a":     NewVariant(42),
			"x.b":     NewVariant(64),
			"x.b.c.d": NewVariant(96),
			"a.b.c":   NewVariant(128),
		},
	}

	objStr := []byte("{\"a\":{\"b\":{\"c\":128}},\"x\":{\"a\":42,\"b\":64,\"c\":{\"d\":96}}}")

	jsonStr, err := json.Marshal(obj)
	require.NoError(t, err)
	require.Equal(t, objStr, jsonStr)
}
