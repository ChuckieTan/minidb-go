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
		buff := bytes.Buffer{}
		byteconv.Encode(&buff, v)
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
	var network bytes.Buffer
	for i := 0; i < b.N; i++ {
		network = bytes.Buffer{}
		enc := gob.NewEncoder(&network)
		enc.Encode(v)
	}
	buff := bytes.Buffer{}
	dec := gob.NewDecoder(&buff)
	dec.Decode(&buff)
}
