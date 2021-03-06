package util

func BytesToUInt32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func BytesToInt64(b []byte) int64 {
	ans := int64(0)
	for i := 0; i < 8; i++ {
		ans = ans<<8 | int64(b[i])
	}
	return ans
}

func Uint64ToBytes(i uint64) []byte {
	byteSlice := make([]byte, 8)
	byteSlice[0] = byte(i)
	byteSlice[1] = byte(i >> 8)
	byteSlice[2] = byte(i >> 16)
	byteSlice[3] = byte(i >> 24)
	byteSlice[4] = byte(i >> 32)
	byteSlice[5] = byte(i >> 40)
	byteSlice[6] = byte(i >> 48)
	byteSlice[7] = byte(i >> 56)
	return byteSlice
}

func BytesToUUID(b []byte) UUID {
	return UUID(BytesToUInt32(b))
}

func Uint16ToBytes(i uint16) []byte {
	byteSlice := make([]byte, 2)
	byteSlice[0] = byte(i)
	byteSlice[1] = byte(i >> 8)
	return byteSlice
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
