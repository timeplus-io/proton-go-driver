package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2/lib/binary"
)

func compareNestedJson(cur *Json, expect *Json, t *testing.T) bool {
	for key, expect_v := range expect.columns {
		cur_v, ok := cur.columns[key]
		assert.True(t, ok)
		assert.Equal(t, expect_v, cur_v)

		if _, ok := cur_v.(*Json); ok {
			compareNestedJson(cur_v.(*Json), expect_v.(*Json), t)
		}
	}
	return true
}

func TestNestJson(t *testing.T) {
	arr, _ := Type("array(string)").Column()
	arr.Append([]string{"abc", "xyz"})
	arr.Append([]string{"zxc", "rfvbasad"})

	objE, _ := Type("array(uint32)").Column()
	objE.Append([]uint32{12345, 67890})
	objE.Append([]uint32{54213, 56473})

	objF, _ := Type("array(string)").Column()
	objF.Append([]string{"timeplus", "proton"})
	objF.Append([]string{"stream", "SQL"})

	json := &Json{
		columns: map[string]Interface{
			"obj.a":     &UInt64{2, 3},
			"obj.b":     &String{"hhh", "xzc"},
			"obj.c.e":   objE,
			"obj.c.f":   objF,
			"arr":       arr,
			"a.b.b.c":   &Float32{1.0, 3.0},
			"`a.b.b`.c": &Float64{2.0, 4.1},
		},
	}
	expectNestedJson := &Json{
		columns: map[string]Interface{
			"obj": &Json{
				columns: map[string]Interface{
					"a": &UInt64{2, 3},
					"b": &String{"hhh", "xzc"},
					"c": &Json{
						columns: map[string]Interface{
							"e": objE,
							"f": objF,
						},
					},
				},
			},
			"arr": arr,
			"a": &Json{
				columns: map[string]Interface{
					"b": &Json{
						columns: map[string]Interface{
							"b": &Json{
								columns: map[string]Interface{
									"c": &Float32{1.0, 3.0},
								},
							},
						},
					},
				},
			},
			"`a.b.b`": &Json{
				columns: map[string]Interface{
					"c": &Float64{2.0, 4.1},
				},
			},
		},
	}
	nestedJson, err := json.nestJson()
	if !assert.NoError(t, err) {
		return
	}
	compareNestedJson(nestedJson, expectNestedJson, t)
	assert.Equal(t, nestedJson.columns["obj"].(*Json).columns["a"], expectNestedJson.columns["obj"].(*Json).columns["a"])
	assert.Equal(t, nestedJson.columns["obj"].(*Json).columns["b"], expectNestedJson.columns["obj"].(*Json).columns["b"])
	assert.Equal(t, nestedJson.columns["obj"].(*Json).columns["c"].(*Json).columns["e"], expectNestedJson.columns["obj"].(*Json).columns["c"].(*Json).columns["e"])
	assert.Equal(t, nestedJson.columns["obj"].(*Json).columns["c"].(*Json).columns["f"], expectNestedJson.columns["obj"].(*Json).columns["c"].(*Json).columns["f"])
	assert.Equal(t, nestedJson.columns["a"].(*Json).columns["b"].(*Json).columns["b"].(*Json).columns["c"],
		expectNestedJson.columns["a"].(*Json).columns["b"].(*Json).columns["b"].(*Json).columns["c"],
	)
	assert.Equal(t, nestedJson.columns["`a.b.b`"].(*Json).columns["c"], expectNestedJson.columns["`a.b.b`"].(*Json).columns["c"])
}

func TestSerialisation(t *testing.T) {
	var buffer bytes.Buffer
	encoder := binary.NewEncoder(&buffer)
	json1 := map[string]interface{}{
		"obj.a":     uint32(1),
		"obj.b":     []string{"abc", "xyz"},
		"obj.c.e":   []uint32{123544, 123546},
		"obj.c.f":   []string{"stream sql", "timeplus"},
		"a.b.b.c":   float32(1.1),
		"`a.b.b`.c": float64(23.1),
	}
	json2 := map[string]interface{}{
		"obj.a":     uint32(1),
		"obj.b":     []string{"xxcccccc", "abcdefghijklmnopqrstuvwxyz"},
		"obj.c.e":   []uint32{20030705, 987765},
		"obj.c.f":   []string{"proton", "go-driver"},
		"a.b.b.c":   float32(3.1415926),
		"`a.b.b`.c": float64(1.9999999),
	}
	col := &Json{columns: make(map[string]Interface)}
	col.AppendRow(json1)
	col.AppendRow(json2)

	nestedJson, err := col.nestJson()
	if !assert.NoError(t, err) {
		return
	}
	typeString := nestedJson.nestedJsonType()
	if !assert.NoError(t, err) {
		return
	}
	if err := serialisation(nestedJson, typeString, encoder); !assert.NoError(t, err) {
		return
	}

	decoder := binary.NewDecoder(bytes.NewReader(buffer.Bytes()))
	resJson := &Json{columns: make(map[string]Interface)}
	if err := deserialisation(resJson, typeString, []string{}, decoder, 2); !assert.NoError(t, err) {
		return
	}
	for k, v := range col.columns {
		res_v, ok := resJson.columns[k]
		assert.True(t, ok)
		assert.Equal(t, v, res_v)
	}
}
