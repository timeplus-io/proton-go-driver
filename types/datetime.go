package types

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

type Datetime struct {
	time.Time
}

const timeFormat = "2006-01-02 15:04:05.999999999"

// MarshalJSON implements json.Marshaler
func (dt Datetime) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", dt.Format(timeFormat))), nil
}

// UnmarshalJSON implements json.Unmarshaler
func (dt *Datetime) UnmarshalJSON(data []byte) error {
	t, err := time.ParseInLocation(`"`+timeFormat+`"`, string(data), time.Local)
	dt.Time = t
	return err
}

// MarshalYAML implements yaml.Marshaler
func (dt Datetime) MarshalYAML() (interface{}, error) {
	return fmt.Sprintf("%s", dt.Format(timeFormat)), nil
}

// UnmarshalYAML implements yaml.Unmarshaler
func (dt *Datetime) UnmarshalYAML(value *yaml.Node) error {
	t, err := time.ParseInLocation(timeFormat, value.Value, time.Local)
	dt.Time = t
	return err
}
