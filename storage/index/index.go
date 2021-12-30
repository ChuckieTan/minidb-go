package index

type KeyType []byte

// Value 的数据类型， 不能小于 32 位 (4 byte)
type ValueType []byte

type Index interface {
	Search(key KeyType) <-chan ValueType
	Insert(key KeyType, value ValueType) error

	KeySize() uint8
	ValueSize() uint8
}
