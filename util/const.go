package util

const (
	// MAX_UUID is the maximum value of UUID.
	MAX_UUID = 1<<32 - 1

	// PageSize is the size of a page.
	PageSize = 8192 // 16KB

	// MAX_PAGE_SIZE is the maximum value of PageSize.'
	MAX_PAGE_SIZE = PageSize - 1

	// MIN_PAGE_SIZE is the minimum value of PageSize.
	MIN_PAGE_SIZE = 0

	// MAX_PAGE_NUM is the maximum value of PageNum.
	MAX_PAGE_NUM = MAX_UUID

	// MAX_OWNER is the maximum value of Owner.
	MAX_OWNER = uint16(1)

	VERSION = "0.0.1"
)
