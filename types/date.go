package types

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

type Date struct {
	time.Time
}

const dateFormat = "2006-01-02"

// MarshalJSON implements json.Marshaler
func (d Date) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", d.Format(dateFormat))), nil
}

// UnmarshalJSON implements json.Unmarshaler
func (d *Date) UnmarshalJSON(data []byte) error {
	t, err := time.ParseInLocation(`"`+dateFormat+`"`, string(data), time.Local)
	d.Time = t
	return err
}

// MarshalYAML implements yaml.Marshaler
func (d Date) MarshalYAML() (interface{}, error) {
	return fmt.Sprintf("%s", d.Format(dateFormat)), nil
}

// UnmarshalYAML implements yaml.Unmarshaler
func (d *Date) UnmarshalYAML(value *yaml.Node) error {
	t, err := time.ParseInLocation(dateFormat, value.Value, time.Local)
	d.Time = t
	return err
}
