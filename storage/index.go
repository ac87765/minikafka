package storage

import (
	"encoding/binary"
	"os"
	"sort"
)

// index 是一个 segment 的稀疏索引：offset → 记录在 .log 文件里的字节位置。
// 不是每条记录都有一项，而是每写入 indexInterval 字节左右才记一项。
//
// 磁盘上每项 8 字节：
//
//	| relOffset 4B | position 4B |
//
// relOffset = offset - segment.base。存相对值而不是 8 字节的绝对 offset，
// 每项省 4 字节；代价是单个 segment 内最多 2^32 条记录、文件最大 4GB，对我们足够了。
const indexEntrySize = 8

type indexEntry struct {
	relOffset uint32
	position  uint32
}

type index struct {
	file    *os.File
	entries []indexEntry // 整个索引常驻内存，查找不碰磁盘
}

// createIndex 新建（或清空）一个索引文件。
func createIndex(path string) (*index, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &index{file: f}, nil
}

func (ix *index) add(relOffset, position uint32) error {
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], relOffset)
	binary.BigEndian.PutUint32(buf[4:8], position)
	if _, err := ix.file.Write(buf[:]); err != nil {
		return err
	}
	ix.entries = append(ix.entries, indexEntry{relOffset, position})
	return nil
}

// lookup 返回「不大于 relOffset 的最后一项」的文件位置：从这里开始顺序扫描，一定能找到目标。
// 因为 offset 单调递增，entries 天然有序，可以二分查找。
func (ix *index) lookup(relOffset uint32) uint32 {
	// 找第一个 relOffset 大于目标的项，它前面那一项就是我们要的。
	i := sort.Search(len(ix.entries), func(i int) bool {
		return ix.entries[i].relOffset > relOffset
	})
	if i == 0 {
		return 0 // 目标在第一个索引项之前，从文件开头扫
	}
	return ix.entries[i-1].position
}

func (ix *index) close() error { return ix.file.Close() }
