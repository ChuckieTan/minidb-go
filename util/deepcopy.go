package util

import (
	"bytes"
	"encoding/gob"

	log "github.com/sirupsen/logrus"
)

func DeepCopy(dst, src interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	dec := gob.NewDecoder(&buf)
	if err := enc.Encode(src); err != nil {
		log.Fatalf("encode failed: %v", err)
	}
	if err := dec.Decode(dst); err != nil {
		log.Fatalf("decode failed: %v", err)
	}
}
