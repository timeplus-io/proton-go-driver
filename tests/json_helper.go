package tests

import (
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/chcol"
)

var JSONTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

type TestStructAddress struct {
	Street  string `chType:"string"`
	City    string `chType:"string"`
	Country string `chType:"string"`
}

type TestStruct struct {
	Name   string
	Age    int64
	Active bool
	Score  float64

	Tags    []string
	Numbers []int64

	Address TestStructAddress

	KeysNumbers map[string]int64
	Metadata    map[string]interface{}

	Timestamp time.Time `chType:"datetime64(3)"`

	DynamicString chcol.Dynamic
	DynamicInt    chcol.Dynamic
	DynamicMap    chcol.Dynamic
}

// FastTestStruct is a distinctly separate type that implements proton.JSONSerializer and proton.JSONDeserializer
// The struct must be a separate type since the JSON column is unable to ignore the interface implementation.
type FastTestStruct struct {
	ts TestStruct
}

// SerializeProtonJSON implements proton.JSONSerializer for faster struct appending
func (fts *FastTestStruct) SerializeProtonJSON() (*proton.JSON, error) {
	obj := chcol.NewJSON()
	obj.SetValueAtPath("Name", fts.ts.Name)
	obj.SetValueAtPath("Age", fts.ts.Age)
	obj.SetValueAtPath("Active", fts.ts.Active)
	obj.SetValueAtPath("Score", fts.ts.Score)
	obj.SetValueAtPath("Tags", fts.ts.Tags)
	obj.SetValueAtPath("Numbers", fts.ts.Numbers)
	obj.SetValueAtPath("Address.Street", fts.ts.Address.Street)
	obj.SetValueAtPath("Address.City", fts.ts.Address.City)
	obj.SetValueAtPath("Address.Country", fts.ts.Address.Country)
	obj.SetValueAtPath("KeysNumbers", fts.ts.KeysNumbers)
	obj.SetValueAtPath("Metadata.FieldA", fts.ts.Metadata["FieldA"])
	obj.SetValueAtPath("Metadata.FieldB", fts.ts.Metadata["FieldB"])
	obj.SetValueAtPath("Metadata.FieldC.FieldD", fts.ts.Metadata["FieldC"].(map[string]interface{})["FieldD"])
	obj.SetValueAtPath("Timestamp", fts.ts.Timestamp)
	obj.SetValueAtPath("DynamicString", fts.ts.DynamicString)
	obj.SetValueAtPath("DynamicInt", fts.ts.DynamicInt)
	obj.SetValueAtPath("DynamicMap", fts.ts.DynamicMap)

	return obj, nil
}

// DeserializeProtonJSON implements proton.JSONDeserializer for faster struct scanning
func (fts *FastTestStruct) DeserializeProtonJSON(obj *proton.JSON) error {
	var elem interface{}
	elem, _ = proton.ExtractJSONPath(obj, "Name")
	fts.ts.Name, _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "Age")
	fts.ts.Age, _ = elem.(int64)
	elem, _ = proton.ExtractJSONPath(obj, "Active")
	fts.ts.Active, _ = elem.(bool)
	elem, _ = proton.ExtractJSONPath(obj, "Score")
	fts.ts.Score, _ = elem.(float64)
	elem, _ = proton.ExtractJSONPath(obj, "Tags")
	fts.ts.Tags, _ = elem.([]string)
	elem, _ = proton.ExtractJSONPath(obj, "Numbers")
	fts.ts.Numbers, _ = elem.([]int64)
	elem, _ = proton.ExtractJSONPath(obj, "Address.Street")
	fts.ts.Address.Street, _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "Address.City")
	fts.ts.Address.City, _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "Address.Country")
	fts.ts.Address.Country, _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "KeysNumbers")
	fts.ts.KeysNumbers, _ = elem.(map[string]int64)
	fts.ts.Metadata = make(map[string]interface{})
	elem, _ = proton.ExtractJSONPath(obj, "Metadata.FieldA")
	fts.ts.Metadata["FieldA"], _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "Metadata.FieldB")
	fts.ts.Metadata["FieldB"], _ = elem.(int64)
	fts.ts.Metadata["FieldC"] = make(map[string]interface{})
	elem, _ = proton.ExtractJSONPath(obj, "Metadata.FieldC.FieldD")
	fts.ts.Metadata["FieldC"].(map[string]interface{})["FieldD"], _ = elem.(string)
	elem, _ = proton.ExtractJSONPath(obj, "Timestamp")
	fts.ts.Timestamp, _ = elem.(time.Time)
	elem, _ = proton.ExtractJSONPath(obj, "DynamicString")
	fts.ts.DynamicString, _ = elem.(proton.Dynamic)
	elem, _ = proton.ExtractJSONPath(obj, "DynamicInt")
	fts.ts.DynamicInt, _ = elem.(proton.Dynamic)
	elem, _ = proton.ExtractJSONPath(obj, "DynamicMap")
	fts.ts.DynamicMap, _ = elem.(proton.Dynamic)

	return nil
}

func BuildTestJSONPaths() *chcol.JSON {
	ts := BuildTestJSONStruct()
	fts := FastTestStruct{ts: ts}
	jsonObj, _ := fts.SerializeProtonJSON()
	return jsonObj
}

func BuildTestJSONStruct() TestStruct {
	return TestStruct{
		Name:    "JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: TestStructAddress{
			Street:  "Street",
			City:    "City",
			Country: "Country",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": "d",
			},
		},
		Timestamp:     JSONTestDate,
		DynamicString: chcol.NewDynamic("str").WithType("string"),
		DynamicInt:    chcol.NewDynamic(int64(48)).WithType("int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("map(string, string)"),
	}
}

func BuildFastTestJSONStruct() FastTestStruct {
	ts := BuildTestJSONStruct()
	return FastTestStruct{ts: ts}
}
