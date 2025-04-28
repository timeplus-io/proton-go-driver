// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
	proton_tests "github.com/timeplus-io/proton-go-driver/v2/tests"
)

func JSONPathsExample() error {
	ctx := context.Background()

	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"allow_experimental_json_type": true,
		},
	})

	if err != nil {
		return err
	}

	if err := proton_tests.CheckMinServerVersion(conn, 2, 9); err != nil {
		fmt.Print("unsupported proton version for json type")
		return err
	}

	err = conn.Exec(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	err = conn.Exec(ctx, `
		CREATE STREAM go_json_example (product json) ENGINE=Memory
		`)
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertProduct := proton.NewJSON()
	insertProduct.SetValueAtPath("id", proton.NewDynamicWithType(uint64(1234), "uint64"))
	insertProduct.SetValueAtPath("name", "Book")
	insertProduct.SetValueAtPath("tags", []string{"library", "fiction"})
	insertProduct.SetValueAtPath("pricing.price", int64(750))
	insertProduct.SetValueAtPath("pricing.currency", "usd")
	insertProduct.SetValueAtPath("metadata.region", "us")
	insertProduct.SetValueAtPath("metadata.page_count", int64(852))
	insertProduct.SetValueAtPath("created_at", proton.NewDynamicWithType(time.Now().UTC().Truncate(time.Millisecond), "datetime64(3)"))

	if err = batch.Append(insertProduct); err != nil {
		return err
	}

	if err = batch.Send(); err != nil {
		return err
	}

	var selectedProduct proton.JSON

	if err = conn.QueryRow(ctx, "SELECT product FROM go_json_example").Scan(&selectedProduct); err != nil {
		return err
	}

	fmt.Printf("inserted product: %+v\n", insertProduct)
	fmt.Printf("selected product: %+v\n", selectedProduct)
	return nil
}

func JSONStringExample() error {
	ctx := context.Background()

	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"allow_experimental_json_type":              true,
			"output_format_native_write_json_as_string": true,
		},
	})

	if err != nil {
		return err
	}

	if err := proton_tests.CheckMinServerVersion(conn, 2, 9); err != nil {
		fmt.Print("unsupported proton version for json type")
		return err
	}

	err = conn.Exec(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	err = conn.Exec(ctx, `
		CREATE STREAM go_json_example (product json) ENGINE=Memory
		`)
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertProductString := "{\"id\":1234,\"name\":\"Book\",\"tags\":[\"library\",\"fiction\"]," +
		"\"pricing\":{\"price\":750,\"currency\":\"usd\"},\"metadata\":{\"page_count\":852,\"region\":\"us\"}," +
		"\"created_at\":\"2024-12-19T11:20:04.146Z\"}"

	if err = batch.Append(insertProductString); err != nil {
		return err
	}

	if err = batch.Send(); err != nil {
		return err
	}

	var selectedProductString string

	if err = conn.QueryRow(ctx, "SELECT product FROM go_json_example").Scan(&selectedProductString); err != nil {
		return err
	}

	fmt.Printf("inserted product string: %s\n", insertProductString)
	fmt.Printf("selected product string: %s\n", selectedProductString)
	fmt.Printf("inserted product string matches selected product string: %t\n", insertProductString == selectedProductString)
	return nil
}

type ProductPricing struct {
	Price    int64  `json:",omitempty"`
	Currency string `json:",omitempty"`
}

type Product struct {
	ID        proton.Dynamic         `json:"id"`
	Name      string                 `json:"name"`
	Tags      []string               `json:"tags"`
	Pricing   ProductPricing         `json:"pricing"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt time.Time              `json:"created_at" chType:"datetime64(3)"`
}

func NewExampleProduct() *Product {
	return &Product{
		ID:   proton.NewDynamicWithType(uint64(1234), "uint64"),
		Name: "Book",
		Tags: []string{"library", "fiction"},
		Pricing: ProductPricing{
			Price:    750,
			Currency: "usd",
		},
		Metadata: map[string]interface{}{
			"region":     "us",
			"page_count": int64(852),
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

func JSONStructExample() error {
	ctx := context.Background()

	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"allow_experimental_json_type": true,
		},
	})

	if err != nil {
		return err
	}

	if err := proton_tests.CheckMinServerVersion(conn, 2, 9); err != nil {
		fmt.Print("unsupported proton version for json type")
		return err
	}

	err = conn.Exec(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	err = conn.Exec(ctx, `
		CREATE STREAM go_json_example (product json) ENGINE=Memory
		`)
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertProduct := NewExampleProduct()

	if err = batch.Append(insertProduct); err != nil {
		return err
	}

	if err = batch.Send(); err != nil {
		return err
	}

	var selectedProduct Product

	if err = conn.QueryRow(ctx, "SELECT product FROM go_json_example").Scan(&selectedProduct); err != nil {
		return err
	}

	insertProductBytes, err := json.Marshal(insertProduct)
	if err != nil {
		return err
	}

	selectedProductBytes, err := json.Marshal(&selectedProduct)
	if err != nil {
		return err
	}

	fmt.Printf("inserted product: %s\n", string(insertProductBytes))
	fmt.Printf("selected product: %s\n", string(selectedProductBytes))
	fmt.Printf("inserted product matches selected product: %t\n", string(insertProductBytes) == string(selectedProductBytes))
	return nil
}

type FastProductPricing struct {
	Price    int64  `json:",omitempty"`
	Currency string `json:",omitempty"`
}

type FastProduct struct {
	ID        proton.Dynamic     `json:"id"`
	Name      string             `json:"name"`
	Tags      []string           `json:"tags"`
	Pricing   FastProductPricing `json:"pricing"`
	Metadata  map[string]any     `json:"metadata"`
	CreatedAt time.Time          `json:"created_at" chType:"datetime64(3)"`
}

// SerializeProtonJSON implements proton.JSONSerializer for faster struct appending
func (p *FastProduct) SerializeProtonJSON() (*proton.JSON, error) {
	obj := proton.NewJSON()
	obj.SetValueAtPath("id", p.ID)
	obj.SetValueAtPath("name", p.Name)
	obj.SetValueAtPath("tags", p.Tags)
	obj.SetValueAtPath("pricing.price", p.Pricing.Price)
	obj.SetValueAtPath("pricing.currency", p.Pricing.Currency)
	obj.SetValueAtPath("metadata.region", p.Metadata["region"])
	obj.SetValueAtPath("metadata.page_count", p.Metadata["page_count"])
	obj.SetValueAtPath("created_at", p.CreatedAt)

	return obj, nil
}

// DeserializeProtonJSON implements proton.JSONDeserializer for faster struct scanning
func (p *FastProduct) DeserializeProtonJSON(obj *proton.JSON) error {
	var value interface{}
	value, _ = proton.ExtractJSONPath(obj, "id")
	p.ID, _ = value.(proton.Dynamic)
	value, _ = proton.ExtractJSONPath(obj, "name")
	p.Name, _ = value.(string)
	value, _ = proton.ExtractJSONPath(obj, "tags")
	p.Tags, _ = value.([]string)
	value, _ = proton.ExtractJSONPath(obj, "pricing.price")
	p.Pricing.Price, _ = value.(int64)
	value, _ = proton.ExtractJSONPath(obj, "pricing.currency")
	p.Pricing.Currency, _ = value.(string)
	p.Metadata = make(map[string]any, 2)
	value, _ = proton.ExtractJSONPath(obj, "metadata.region")
	p.Metadata["region"], _ = value.(string)
	value, _ = proton.ExtractJSONPath(obj, "metadata.page_count")
	p.Metadata["page_count"], _ = value.(int64)
	value, _ = proton.ExtractJSONPath(obj, "created_at")
	p.CreatedAt, _ = value.(time.Time)

	return nil
}

func NewExampleFastProduct() *FastProduct {
	return &FastProduct{
		ID:   proton.NewDynamicWithType(uint64(1234), "uint64"),
		Name: "Book",
		Tags: []string{"library", "fiction"},
		Pricing: FastProductPricing{
			Price:    750,
			Currency: "usd",
		},
		Metadata: map[string]any{
			"region":     "us",
			"page_count": int64(852),
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

func JSONFastStructExample() error {
	ctx := context.Background()

	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"allow_experimental_json_type": true,
		},
	})

	if err != nil {
		return err
	}

	if err := proton_tests.CheckMinServerVersion(conn, 2, 9); err != nil {
		fmt.Print("unsupported proton version for json type")
		return err
	}

	err = conn.Exec(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	err = conn.Exec(ctx, `
		CREATE STREAM go_json_example (product json) ENGINE=Memory
		`)
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertFastProduct := NewExampleFastProduct()

	if err = batch.Append(insertFastProduct); err != nil {
		return err
	}

	if err = batch.Send(); err != nil {
		return err
	}

	var selectedFastProduct FastProduct

	if err = conn.QueryRow(ctx, "SELECT product FROM go_json_example").Scan(&selectedFastProduct); err != nil {
		return err
	}

	insertFastProductBytes, err := json.Marshal(insertFastProduct)
	if err != nil {
		return err
	}

	selectedFastProductBytes, err := json.Marshal(&selectedFastProduct)
	if err != nil {
		return err
	}

	fmt.Printf("inserted product: %s\n", string(insertFastProductBytes))
	fmt.Printf("selected product: %s\n", string(selectedFastProductBytes))
	fmt.Printf("inserted product matches selected product: %t\n", string(insertFastProductBytes) == string(selectedFastProductBytes))
	return nil
}

func main() {
	fmt.Println("JSONPathsExample")
	if err := JSONPathsExample(); err != nil {
		log.Fatal(err)
	}
	fmt.Println()

	fmt.Println("JSONStringExample")
	if err := JSONStringExample(); err != nil {
		log.Fatal(err)
	}
	fmt.Println()

	fmt.Println("JSONStructExample")
	if err := JSONStructExample(); err != nil {
		log.Fatal(err)
	}
	fmt.Println()

	fmt.Println("JSONFastStructExample")
	if err := JSONFastStructExample(); err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}
