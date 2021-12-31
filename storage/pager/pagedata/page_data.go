package pagedata

import (
	"io"
)

type PageData interface {
	Encode() []byte
	Decode(r io.Reader) error
	// 返回 PageData 的大小，以字节为单位
	Size() int
}

type PageDataType uint8

const (
	META_DATA PageDataType = iota
	RECORE_DATA
	INDEX_DATA
)
