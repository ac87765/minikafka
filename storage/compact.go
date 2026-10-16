package storage

import (
	"bufio"
	"os"
	"path/filepath"
	"time"
)

// 日志压缩（compaction）：对每个 key 只保留最新的一条记录，旧值删掉。
//
// 压缩之后，从头重放日志得到的「每个 key 的最终值」必须和压缩之前完全一样。
// 这是压缩唯一的正确性标准，compact_test.go 里专门测它。
//
// 规则：
//   - 只压缩非活跃段。活跃段还在被写，碰它会和写入打架。
//   - offset 保持不变。压缩只会在 offset 序列里留下空洞，绝不重新编号（第 1 课讲过为什么）。
//   - 没有 key 的记录不参与压缩，原样保留。
//   - 墓碑不能马上删。设想一个消费者正在用这个日志构建物化视图，它早就读到了 a=1，
//     然后下线了一会儿。如果它回来之前墓碑就被清掉了，它永远不会知道 a 被删了，
//     视图里的 a=1 会一直留着。所以墓碑要保留 TombstoneRetention 这么久，
//     给消费者留出追上来的时间。
//
// 简化：Kafka 在后台线程里压缩，期间不阻塞写入，还会把变小的段合并起来。
// 我们在写锁里做，压缩期间写入会被阻塞，也不合并段。

// CompactStats 是一次压缩的结果。
type CompactStats struct {
	SegmentsRewritten int
	RecordsRemoved    int
}

const cleanedSuffix = ".cleaned"

// Compact 压缩所有非活跃段。
func (l *Log) Compact() (CompactStats, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var stats CompactStats
	old := l.segments[:len(l.segments)-1] // 不含活跃段
	if len(old) == 0 {
		return stats, nil
	}

	// 第一遍：找出每个 key 最新的 offset。
	latest := make(map[string]uint64)
	for _, s := range old {
		err := s.scan(func(r Record) error {
			if r.Key != nil {
				latest[string(r.Key)] = r.Offset
			}
			return nil
		})
		if err != nil {
			return stats, err
		}
	}

	// 第二遍：逐段重写，只留下该留的记录。
	now := l.now()
	for i, s := range old {
		mt, err := s.modTime()
		if err != nil {
			return stats, err
		}
		tombstoneExpired := now.Sub(mt) >= l.cfg.TombstoneRetention
		keep := func(r Record) bool {
			if r.Key == nil {
				return true
			}
			if latest[string(r.Key)] != r.Offset {
				return false // 有更新的值，这条已经过时
			}
			return !(r.Tombstone && tombstoneExpired)
		}

		cleaned, removed, err := l.rewriteSegment(s, mt, keep)
		if err != nil {
			return stats, err
		}
		if removed == 0 {
			continue
		}
		l.segments[i] = cleaned
		stats.SegmentsRewritten++
		stats.RecordsRemoved += removed
	}
	return stats, nil
}

// rewriteSegment 把 s 里 keep 返回 true 的记录写进新文件，再原子地替换掉旧文件。
// 如果没有记录需要删除，什么都不做，返回 (s, 0, nil)。
//
// 崩溃安全靠「先写临时文件、fsync、再 rename」：
//   - rename 之前崩溃：旧文件完好，留下一个 .cleaned 临时文件，下次 Open 时删掉。
//   - rename 之后崩溃：新文件已经完整落盘，直接用。
//
// 任何时刻磁盘上都有一份完整的数据，不存在「一半旧、一半新」的状态。
func (l *Log) rewriteSegment(s *segment, modTime time.Time, keep func(Record) bool) (*segment, int, error) {
	logPath := s.log.Name()
	tmpPath := logPath + cleanedSuffix
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return nil, 0, err
	}
	defer os.Remove(tmpPath) // rename 成功之后这里会失败，忽略即可

	w := bufio.NewWriter(tmp)
	removed := 0
	err = s.scan(func(r Record) error {
		if !keep(r) {
			removed++
			return nil
		}
		buf, err := Encode(r)
		if err != nil {
			return err
		}
		_, err = w.Write(buf)
		return err
	})
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil || removed == 0 {
		return s, 0, err
	}

	// 保留原来的修改时间：按时间删除旧段、判断墓碑是否过期，都依赖它。
	if err := os.Chtimes(tmpPath, modTime, modTime); err != nil {
		return nil, 0, err
	}
	if err := s.close(); err != nil {
		return nil, 0, err
	}
	if err := os.Rename(tmpPath, logPath); err != nil {
		return nil, 0, err
	}
	if err := syncDir(l.dir); err != nil { // rename 本身也要落盘
		return nil, 0, err
	}
	cleaned, err := openSegment(l.dir, s.base, l.cfg.IndexInterval, false)
	if err != nil {
		return nil, 0, err
	}
	return cleaned, removed, nil
}

// removeLeftoverCleaned 删除上次压缩崩溃时留下的临时文件。
func removeLeftoverCleaned(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, "*"+cleanedSuffix))
	if err != nil {
		return err
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
