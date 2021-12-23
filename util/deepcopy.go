package util

import (
	"bytes"
	"encoding/gob"
)

func DeepCopy(dst, src interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	dec := gob.NewDecoder(&buf)
	if err := enc.Encode(src); err != nil {
		panic(err)
	}
	if err := dec.Decode(dst); err != nil {
		panic(err)
	}
}
