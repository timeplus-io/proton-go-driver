package column

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestUInt128ColumnFactory(t *testing.T) {
	c, err := Type("uint128").Column("col", time.UTC)
	if err != nil {
		t.Fatalf("Column(): %v", err)
	}
	bi, ok := c.(*BigInt)
	if !ok {
		t.Fatalf("expected *BigInt, got %T", c)
	}
	if bi.size != 16 {
		t.Fatalf("expected size=16, got %d", bi.size)
	}
	if bi.signed {
		t.Fatalf("expected unsigned BigInt for uint128")
	}
}

func TestUInt128RoundTripHighBit(t *testing.T) {
	v := new(big.Int).Lsh(big.NewInt(1), 127)
	v.Add(v, big.NewInt(12345))

	raw := make([]byte, 16)
	if err := bigIntToRawUnsigned(raw, v); err != nil {
		t.Fatalf("bigIntToRawUnsigned(): %v", err)
	}
	rawCopy := append([]byte(nil), raw...)

	got := rawToBigUInt(raw)
	if got.Cmp(v) != 0 {
		t.Fatalf("round-trip mismatch: got=%s want=%s", got.String(), v.String())
	}
	if !bytes.Equal(raw, rawCopy) {
		t.Fatalf("rawToBigUInt mutated input")
	}
}

func TestUInt128AppendRejectsNegativeAndOverflow(t *testing.T) {
	col := &BigInt{
		size:   16,
		chType: "uint128",
		signed: false,
	}

	if err := col.AppendRow(big.NewInt(-1)); err == nil {
		t.Fatalf("expected error for negative value")
	}

	tooBig := new(big.Int).Lsh(big.NewInt(1), 128)
	if err := col.AppendRow(tooBig); err == nil {
		t.Fatalf("expected error for out-of-range value")
	}
}

func TestRawToBigIntDoesNotMutateInput(t *testing.T) {
	raw := make([]byte, 16)
	if err := bigIntToRawSigned(raw, big.NewInt(-128)); err != nil {
		t.Fatalf("bigIntToRawSigned(): %v", err)
	}
	rawCopy := append([]byte(nil), raw...)

	_ = rawToBigInt(raw)
	if !bytes.Equal(raw, rawCopy) {
		t.Fatalf("rawToBigInt mutated input")
	}
}
