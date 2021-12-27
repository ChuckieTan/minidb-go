package util

func BytesToUInt32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func BytesToUUID(b []byte) UUID {
	return UUID(BytesToUInt32(b))
}

func Uint32ToBytes(size uint8, i uint32) []byte {
	byteSlice := make([]byte, size)
	byteSlice[0] = byte(i)
	byteSlice[1] = byte(i >> 8)
	byteSlice[2] = byte(i >> 16)
	byteSlice[3] = byte(i >> 24)
	return byteSlice
}

func UUIDToBytes(size uint8, i UUID) []byte {
	return Uint32ToBytes(size, uint32(i))
}
