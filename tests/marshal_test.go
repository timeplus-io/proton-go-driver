package tests

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2/types"
	"gopkg.in/yaml.v3"
	"testing"
	"time"
)

func TestDateTimeMarshal(t *testing.T) {
	datetimes := []types.Datetime{{time.Date(2000, 1, 1, 1, 1, 0, 0, time.Local)}, {time.Date(2000, 1, 1, 1, 1, 0, 1, time.Local)}, {time.Date(2000, 1, 1, 1, 1, 0, 10, time.Local)}}
	datetimestrs := []string{"2000-01-01 01:01:00", "2000-01-01 01:01:00.000000001", "2000-01-01 01:01:00.00000001"}
	for i, datetime := range datetimes {
		str := datetimestrs[i]
		{
			datetimeMap := map[string]types.Datetime{"time": datetime}
			s, err := json.Marshal(datetimeMap)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("{\"time\":\"%s\"}", str), string(s))
		}
		{
			datetimeMap := map[string]types.Datetime{"time": datetime}
			s, err := yaml.Marshal(datetimeMap)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("time: \"%s\"\n", str), string(s))
		}
	}
}

func TestDateTimeUnmarshal(t *testing.T) {
	datetimes := []types.Datetime{{time.Date(2000, 1, 1, 1, 1, 0, 0, time.Local)}, {time.Date(2000, 1, 1, 1, 1, 0, 1, time.Local)}, {time.Date(2000, 1, 1, 1, 1, 0, 10, time.Local)}}
	datetimestrs := []string{"2000-01-01 01:01:00", "2000-01-01 01:01:00.000000001", "2000-01-01 01:01:00.00000001"}
	for i, datetime := range datetimes {
		str := datetimestrs[i]
		{
			var actualDatetimeMap map[string]types.Datetime
			j := fmt.Sprintf(`{"time": "%s"}`, str)
			assert.NoError(t, json.Unmarshal([]byte(j), &actualDatetimeMap))
			assert.Equal(t, datetime, actualDatetimeMap["time"])
		}
		{
			var actualDatetimeMap map[string]types.Datetime
			y := fmt.Sprintf(`"time": "%s"`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDatetimeMap))
			assert.Equal(t, datetime, actualDatetimeMap["time"])
		}
		{
			var actualDatetimeMap map[string]types.Datetime
			y := fmt.Sprintf(`"time": '%s'`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDatetimeMap))
			assert.Equal(t, datetime, actualDatetimeMap["time"])
		}
		{
			var actualDatetimeMap map[string]types.Datetime
			y := fmt.Sprintf(`"time": %s`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDatetimeMap))
			assert.Equal(t, datetime, actualDatetimeMap["time"])
		}
	}
}

func TestDateMarshal(t *testing.T) {
	dates := []types.Date{{time.Date(2000, 1, 10, 1, 1, 1, 1, time.Local)}, {time.Date(2077, 1, 1, 1, 1, 0, 1, time.Local)}, {time.Date(1970, 1, 9, 1, 1, 0, 10, time.Local)}}
	datestrs := []string{"2000-01-10", "2077-01-01", "1970-01-09"}
	for i, date := range dates {
		str := datestrs[i]
		{
			dateMap := map[string]types.Date{"time": date}
			s, err := json.Marshal(dateMap)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("{\"time\":\"%s\"}", str), string(s))
		}
		{
			dateMap := map[string]types.Date{"time": date}
			s, err := yaml.Marshal(dateMap)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("time: \"%s\"\n", str), string(s))
		}
	}
}

func TestDateUnmarshal(t *testing.T) {
	dates := []types.Date{{time.Date(2000, 1, 10, 1, 1, 1, 1, time.Local)}, {time.Date(2077, 1, 1, 1, 1, 0, 1, time.Local)}, {time.Date(1970, 1, 9, 1, 1, 0, 10, time.Local)}}
	datestrs := []string{"2000-01-10", "2077-01-01", "1970-01-09"}
	for i, date := range dates {
		date.Time = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local)
		str := datestrs[i]
		{
			var actualDateMap map[string]types.Date
			j := fmt.Sprintf(`{"time": "%s"}`, str)
			assert.NoError(t, json.Unmarshal([]byte(j), &actualDateMap))
			assert.Equal(t, date, actualDateMap["time"])
		}
		{
			var actualDateMap map[string]types.Date
			y := fmt.Sprintf(`"time": "%s"`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDateMap))
			assert.Equal(t, date, actualDateMap["time"])
		}
		{
			var actualDateMap map[string]types.Date
			y := fmt.Sprintf(`"time": '%s'`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDateMap))
			assert.Equal(t, date, actualDateMap["time"])
		}
		{
			var actualDateMap map[string]types.Date
			y := fmt.Sprintf(`"time": %s`, str)
			assert.NoError(t, yaml.Unmarshal([]byte(y), &actualDateMap))
			assert.Equal(t, date, actualDateMap["time"])
		}
	}
}
