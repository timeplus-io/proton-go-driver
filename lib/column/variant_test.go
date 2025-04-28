package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColVariant_parse(t *testing.T) {
	cases := []struct {
		typ           Type
		expectedTypes []Type
	}{
		{typ: "variant(int64)", expectedTypes: []Type{"int64"}},
		{typ: "variant(int64, string)", expectedTypes: []Type{"int64", "string"}},
		{typ: "variant(array(string), int64, string)", expectedTypes: []Type{"array(string)", "int64", "string"}},
		{typ: "variant(array(map(string, string)), map(string, int64))", expectedTypes: []Type{"array(map(string, string))", "map(string, int64)"}},
		{typ: "variant(array(map(string, tuple(a string, b int64))), map(string, int64))", expectedTypes: []Type{"array(map(string, tuple(a string, b int64)))", "map(string, int64)"}},
	}

	for i, c := range cases {
		col, err := (&Variant{name: "vt"}).parse(c.typ, nil)
		require.NoError(t, err, "case index %d failed to parse Variant column", i)

		require.Equal(t, "vt", col.Name())
		require.Equal(t, c.typ, col.chType)
		require.Equal(t, len(c.expectedTypes), len(col.columns))
		require.Equal(t, len(c.expectedTypes), len(col.columnTypeIndex))

		for j, subCol := range col.columns {
			expectedType := c.expectedTypes[j]
			actualType := subCol.Type()
			assert.Equal(t, expectedType, actualType, "case index %d Variant type index %d column type does not match", i, j)

			expectedColumnTypeIndex := uint8(j)
			actualColumnTypeIndex := col.columnTypeIndex[string(actualType)]
			assert.Equal(t, expectedColumnTypeIndex, actualColumnTypeIndex, "case index %d Variant type index %d columnTypeIndex does not match", i, j)
		}
	}
}

func TestColVariant_parse_invalid(t *testing.T) {
	cases := []Type{
		"",
		"variant",
		"variant(array(map(string)), map(string, int64))",
		"variant(array(tuple(string, b int64)), map(string, int64), FakeType)",
	}

	for i, typeName := range cases {
		_, err := (&Variant{name: "vt"}).parse(typeName, nil)
		require.Error(t, err, "expected error for case index %d (\"%s\"), but received nil", i, typeName)
	}
}

func TestColVariant_addColumn(t *testing.T) {
	col := Variant{columnTypeIndex: make(map[string]uint8, 1)}

	col.addColumn(&Int64{})

	require.Equal(t, 1, len(col.columns))
	require.Equal(t, 1, len(col.columnTypeIndex))
	require.Equal(t, Type("int64"), col.columns[0].Type())
	require.Equal(t, uint8(0), col.columnTypeIndex["int64"])
}

func TestColVariant_appendDiscriminatorRow(t *testing.T) {
	col := Variant{}
	var discriminator uint8 = 8

	col.appendDiscriminatorRow(discriminator)

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, discriminator, col.discriminators[0])
}

func TestColVariant_appendNullRow(t *testing.T) {
	col := Variant{}

	col.appendNullRow()

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, NullVariantDiscriminator, col.discriminators[0])
}
