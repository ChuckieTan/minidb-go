package byteconv_test

import (
	"bytes"
	"encoding/gob"
	"math"
	"minidb-go/util/byteconv"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	type P struct {
		X, Y int64
	}
	// v := [253]P{}
	v := make([]P, 253)
	for i := 0; i < len(v); i++ {
		v[i].X = math.MaxInt64
		v[i].Y = math.MaxInt64
	}
	for i := 0; i < b.N; i++ {
		buff := new(bytes.Buffer)
		byteconv.Encode(buff, v)
	}
}

func BenchmarkGobEncode(b *testing.B) {
	type P struct {
		X, Y int64
	}
	// v := [253]P{}
	v := make([]P, 253)
	for i := 0; i < len(v); i++ {
		v[i].X = math.MaxInt64
		v[i].Y = math.MaxInt64
	}
	network := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		enc := gob.NewEncoder(network)
		enc.Encode(v)
		network.Reset()
	}
	buff := new(bytes.Buffer)
	dec := gob.NewDecoder(buff)
	dec.Decode(buff)
}
