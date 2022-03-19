package wtinylfu

import (
	"math"
)

type Hashable interface {
	Hash() uint64
}

var SEED []uint64 = []uint64{
	0xc3a5c85c97cb3127, 0xb492b66fbe98f273, 0x9ae16a3b2f90404f, 0xcbf29ce484222325}

type DoubelBitNum struct {
	A, B byte
}

type CountMinSketch struct {
	// 预期存放数据的最大值
	maxEntries int
	// 实际数组的最大值
	tableSize int
	// 数组
	table []DoubelBitNum

	// 进行加 1 的总数
	count int
	// 最大总频率，超过这个频率应该把所有的数值全都除以 2
	maxFrequency int
}

func NewCountMinSketch(maxEntries int) *CountMinSketch {
	ln2 := float64(math.Log(2))
	tableSize := int(-float64(maxEntries)*math.Log(0.01)/(ln2*ln2)) / 2
	return &CountMinSketch{
		maxEntries:   maxEntries,
		tableSize:    tableSize,
		table:        make([]DoubelBitNum, tableSize),
		count:        0,
		maxFrequency: maxEntries * 10,
	}
}

// 根据传入的元素计算出 4 个 hash 值
func (c *CountMinSketch) getIndex(e Hashable) []uint64 {
	var index []uint64
	for i := 0; i < 4; i++ {
		index = append(index, e.Hash()*SEED[i]%uint64(c.tableSize))
	}
	return index
}

// 在数组中进行加 1 操作
func (c *CountMinSketch) Add(e Hashable) {
	c.count++
	if c.count > c.maxFrequency {
		c.reset()
	}
	indexs := c.getIndex(e)
	for _, index := range indexs {
		if index&1 == 1 {
			if c.table[index].A < 16 {
				c.table[index].A++
			}
		} else {
			if c.table[index].B < 16 {
				c.table[index].B++
			}
		}
	}
}

type Ordered interface {
	int | int16 | int32 | int64 | uint | uint16 | uint32 | uint64
}

func min[T Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// 获得数组中的最小值
func (c *CountMinSketch) Count(e Hashable) int {
	indexs := c.getIndex(e)
	count := math.MaxInt
	for _, index := range indexs {
		if index&1 == 1 {
			count = min(count, int(c.table[index].A))
		} else {
			count = min(count, int(c.table[index].B))
		}
	}
	return count
}

// 将所有元素减半
func (c *CountMinSketch) reset() {
	for i := 0; i < c.tableSize; i++ {
		if i&1 == 1 {
			c.table[i].A >>= 1
		} else {
			c.table[i].B >>= 1
		}
	}
	c.count >>= 1
}
