package util

const (
	// MAX_UUID is the maximum value of UUID.
	MAX_UUID = 1<<32 - 1

	// PAGE_SIZE is the size of a page.
	PAGE_SIZE = 8192 // 16KB

	// MAX_PAGE_SIZE is the maximum value of PageSize.'
	MAX_PAGE_SIZE = PAGE_SIZE - 1

	// MIN_PAGE_SIZE is the minimum value of PageSize.
	MIN_PAGE_SIZE = 0

	// MAX_PAGE_NUM is the maximum value of PageNum.
	MAX_PAGE_NUM = MAX_UUID

	// MAX_TABLE_NUM is the maximum value of TableNum.
	MAX_TABLE_NUM = 1<<8 - 1

	// MAX_INDEX_NUM is the maximum value of IndexNum.
	MAX_INDEX_NUM = 1<<8 - 1

	// MAX_COLUMN_NUM is the maximum value of ColumnNum.
	MAX_COLUMN_NUM = 1<<8 - 1

	// MAX_COLUMN_NAME_LEN is the maximum value of ColumnNameLen.
	MAX_COLUMN_NAME_LEN = 1<<8 - 1

	// BPLUSTREE_ORDER is the order of B+Tree.

	BPLUSTREE_KEY_LEN = 32

	VERSION = "0.0.1"
)
