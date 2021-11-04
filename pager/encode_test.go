package pager_test

import (
	"math"
	"minidb-go/pager"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	type P struct {
		X, Y int64
	}
	v := [253]P{}
	for i := 0; i < len(v); i++ {
		v[i].X = math.MaxInt64
		v[i].Y = math.MaxInt64
	}
	for i := 0; i < b.N; i++ {
		pager.Encode(v)
	}
}
