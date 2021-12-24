package pager

import "io"

type PageData interface {
	Raw() []byte
}

type PageDataType uint8

const (
	META_DATA PageDataType = iota
	RECORE_DATA
	INDEX_DATA
)

func NewPageData(dataType PageDataType) *PageData {
	panic("implement me")
}

func LoadPageData(r io.Reader) *PageData {
	panic("implement me")
}
