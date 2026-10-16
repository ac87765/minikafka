package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var ErrOffsetOutOfRange = errors.New("storage: offset out of range")

// segment 是日志的一段：一个 .log 数据文件 + 一个 .index 稀疏索引文件。
// 文件名是这一段的第一条 offset（base），补零到 20 位，这样按文件名排序就是按 offset 排序：
//
//	00000000000000000000.log   00000000000000000000.index
//	00000000000000012000.log   00000000000000012000.index
//
// 只需要记起点：一段的终点就是下一段的起点减一。
type segment struct {
	base uint64 // 本段第一条记录的 offset
	next uint64 // 下一条要分配的 offset

	log  *os.File
	size int64 // .log 当前字节数，也就是下一条记录的写入位置
	idx  *index

	indexInterval   int // 每写这么多字节记一个索引项
	bytesSinceIndex int
}

func segmentPath(dir string, base uint64, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, ext))
}

// openSegment 打开（或新建）base 对应的段。
//
// 打开时把 .log 从头扫一遍，重建索引，顺便算出 next 和 size。
// 扫到坏记录时怎么办，由 truncateTorn 决定：
//
//   - true（活跃段）：崩溃时它正在被写，尾部出现半写记录是正常的。从第一条坏记录处截断，
//     后面的字节全部丢弃。被丢掉的记录一定没有向客户端确认过（前提是确认之前已经 fsync），
//     客户端会重试，所以不违反任何承诺。
//   - false（旧段）：它早就写完并 fsync 过了，不应该有坏记录。真出现了说明是磁盘损坏，
//     不能悄悄删数据，直接报错，交给人来处理。
func openSegment(dir string, base uint64, indexInterval int, truncateTorn bool) (*segment, error) {
	logFile, err := os.OpenFile(segmentPath(dir, base, ".log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	idx, err := createIndex(segmentPath(dir, base, ".index"))
	if err != nil {
		logFile.Close()
		return nil, err
	}
	s := &segment{base: base, next: base, log: logFile, idx: idx, indexInterval: indexInterval}
	if err := s.rebuild(truncateTorn); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

func (s *segment) rebuild(truncateTorn bool) error {
	rd := bufio.NewReader(io.NewSectionReader(s.log, 0, 1<<62))
	for {
		r, err := Decode(rd)
		if err == io.EOF {
			return nil // 干净的结尾
		}
		if err != nil {
			if !truncateTorn {
				return fmt.Errorf("segment %d corrupt at byte %d: %w", s.base, s.size, err)
			}
			// 不能「跳过」坏记录继续往后读：坏记录的 length 不可信，我们根本不知道下一条从哪开始。
			// 也不能「修复」它：缺掉的字节已经丢了。唯一安全的做法是截断。
			log.Printf("storage: segment %d: %v at byte %d, truncating", s.base, err, s.size)
			if err := s.log.Truncate(s.size); err != nil {
				return err
			}
			return s.log.Sync() // 截断本身也要落盘，否则再崩一次又回到老样子
		}
		if err := s.maybeIndex(r.Offset); err != nil {
			return err
		}
		n := r.encodedSize()
		s.size += int64(n)
		s.bytesSinceIndex += n
		s.next = r.Offset + 1
	}
}

// maybeIndex 在写一条记录之前调用：距离上一个索引项够远了（或者是段里第一条），就记一项。
func (s *segment) maybeIndex(offset uint64) error {
	if s.size != 0 && s.bytesSinceIndex < s.indexInterval {
		return nil
	}
	if err := s.idx.add(uint32(offset-s.base), uint32(s.size)); err != nil {
		return err
	}
	s.bytesSinceIndex = 0
	return nil
}

// append 写入一条新记录（忽略 r.Offset，分配下一个 offset），返回分配给它的 offset。
func (s *segment) append(r Record) (uint64, error) {
	r.Offset = s.next
	if err := s.write(r); err != nil {
		return 0, err
	}
	return r.Offset, nil
}

// write 原样写入一条已经带 offset 的记录。压缩时把旧记录搬到新文件，用的就是它：
// offset 必须保持不变，而且可以不连续（被压缩掉的位置留下空洞）。
func (s *segment) write(r Record) error {
	buf, err := Encode(r)
	if err != nil {
		return err
	}
	if err := s.maybeIndex(r.Offset); err != nil {
		return err
	}
	if _, err := s.log.Write(buf); err != nil {
		return err
	}
	s.size += int64(len(buf))
	s.bytesSinceIndex += len(buf)
	s.next = r.Offset + 1
	return nil
}

// read 读出 offset 对应的记录：先用稀疏索引跳到附近，再顺序扫描。
func (s *segment) read(off uint64) (Record, error) {
	if off < s.base || off >= s.next {
		return Record{}, ErrOffsetOutOfRange
	}
	pos := int64(s.idx.lookup(uint32(off - s.base)))
	rd := bufio.NewReader(io.NewSectionReader(s.log, pos, s.size-pos))
	for {
		r, err := Decode(rd)
		if err != nil {
			return Record{}, err
		}
		if r.Offset == off {
			return r, nil
		}
		if r.Offset > off {
			return Record{}, ErrOffsetOutOfRange // 以后做了压缩，这条可能已被删掉
		}
	}
}

// modTime 是 .log 最后一次写入的时间，用来判断这一段是否过期。
func (s *segment) modTime() (time.Time, error) {
	fi, err := s.log.Stat()
	if err != nil {
		return time.Time{}, err
	}
	return fi.ModTime(), nil
}

// scan 按顺序把这一段的每条记录交给 fn。
func (s *segment) scan(fn func(Record) error) error {
	rd := bufio.NewReader(io.NewSectionReader(s.log, 0, s.size))
	for {
		r, err := Decode(rd)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
}

// sync 把这一段在页缓存里的数据刷到磁盘。Write 返回只代表数据进了操作系统的页缓存，
// 断电就没了；fsync 返回才代表真的落盘。
func (s *segment) sync() error { return s.log.Sync() }

func (s *segment) close() error {
	return errors.Join(s.log.Close(), s.idx.close())
}

// remove 关闭并删除这一段的两个文件。删除旧数据的全部代价就是这两次 unlink。
func (s *segment) remove() error {
	return errors.Join(
		s.close(),
		os.Remove(s.log.Name()),
		os.Remove(s.idx.file.Name()),
	)
}
