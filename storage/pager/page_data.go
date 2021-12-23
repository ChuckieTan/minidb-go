package pager

type PageData interface {
	Raw() []byte
}

func LoadPageData(data []byte) *PageData {
	panic("implement me")
}
