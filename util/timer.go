package util

import (
	"fmt"
	"time"
)

//@brief：耗时统计函数
func TimeCost() func(string) {
	start := time.Now()
	return func(brief string) {
		tc := time.Since(start)
		fmt.Printf("%v: time cost = %v\n", brief, tc)
	}
}
