package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config 控制分段和索引的参数。零值字段会被换成默认值。
type Config struct {
	// MaxSegmentBytes：一段写到这么大就滚动到新段。
	// 必须小于 4GB，因为索引里的 position 只有 4 字节。
	MaxSegmentBytes int64
	// MaxSegmentAge：活跃段存在超过这么久，即使没写满也滚动。0 表示不按时间滚动。
	MaxSegmentAge time.Duration
	// IndexInterval：每写这么多字节记一个索引项。
	IndexInterval int
	// TombstoneRetention：墓碑在压缩时至少保留这么久才会被清理（见 compact.go）。
	TombstoneRetention time.Duration
}

const (
	defaultMaxSegmentBytes    = 1 << 30        // 1GB，和 Kafka 默认值一样
	defaultIndexInterval      = 4096           // 4KB，和 Kafka 默认值一样
	defaultTombstoneRetention = 24 * time.Hour // 和 Kafka 的 delete.retention.ms 默认值一样
	maxSegmentBytesLimit      = 1<<32 - 1
)

// Log 是由多个 segment 组成的追加日志。
// segments 按 base offset 从小到大排列，最后一个是活跃段，只有它会被写入。
//
// 并发：一个写者，多个读者。写操作拿写锁，读操作拿读锁。
type Log struct {
	mu       sync.RWMutex
	dir      string
	cfg      Config
	segments []*segment
	// activeSince 是活跃段开始的时间，用于按时间滚动。
	activeSince time.Time
	now         func() time.Time // 测试里可以换成假时钟
}

// Open 打开 dir 下已有的日志，或者新建一个空日志。
func Open(dir string, cfg Config) (*Log, error) {
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if cfg.MaxSegmentBytes > maxSegmentBytesLimit {
		return nil, fmt.Errorf("storage: MaxSegmentBytes %d exceeds 4GB index limit", cfg.MaxSegmentBytes)
	}
	if cfg.IndexInterval == 0 {
		cfg.IndexInterval = defaultIndexInterval
	}
	if cfg.TombstoneRetention == 0 {
		cfg.TombstoneRetention = defaultTombstoneRetention
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := removeLeftoverCleaned(dir); err != nil {
		return nil, err
	}

	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		bases = []uint64{0}
	}
	l := &Log{dir: dir, cfg: cfg, now: time.Now}
	for i, base := range bases {
		// 只有最后一段（崩溃时的活跃段）允许截断半写记录。
		isLast := i == len(bases)-1
		s, err := openSegment(dir, base, cfg.IndexInterval, isLast)
		if err != nil {
			l.Close()
			return nil, err
		}
		l.segments = append(l.segments, s)
	}
	l.activeSince = l.now()
	return l, nil
}

// listSegmentBases 从文件名解析出所有段的 base offset，并排好序。
func listSegmentBases(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("storage: unexpected file %s: %w", filepath.Join(dir, name), err)
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func (l *Log) active() *segment { return l.segments[len(l.segments)-1] }

// Append 写入一条记录，返回它的 offset。key 可以为 nil。
func (l *Log) Append(key, value []byte) (uint64, error) {
	return l.append(Record{Key: key, Value: value})
}

// Delete 写入一个墓碑，表示 key 被删除。
// 只追加的日志不能修改旧记录，「删除」只能是追加一条「它被删了」的记录。
func (l *Log) Delete(key []byte) (uint64, error) {
	return l.append(Record{Key: key, Tombstone: true})
}

func (l *Log) append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.shouldRoll(int64(r.encodedSize())) {
		if err := l.roll(); err != nil {
			return 0, err
		}
	}
	return l.active().append(r)
}

// shouldRoll 决定写下一条记录之前要不要先开新段。
// 空段永远不滚动，否则一条超大记录会让我们不停地创建空段。
func (l *Log) shouldRoll(recordSize int64) bool {
	a := l.active()
	if a.size == 0 {
		return false
	}
	if a.size+recordSize > l.cfg.MaxSegmentBytes {
		return true // 写进去就超了：保证每段都不超过 MaxSegmentBytes
	}
	return l.cfg.MaxSegmentAge > 0 && l.now().Sub(l.activeSince) >= l.cfg.MaxSegmentAge
}

// roll 关闭当前活跃段的写入资格，以「下一个 offset」为 base 开一个新段。
func (l *Log) roll() error {
	// 旧段从此不再写入。先把它刷盘，保证「非活跃段不会有半写记录」这个前提成立，
	// 这样重启时只需检查最后一段。
	if err := l.active().sync(); err != nil {
		return err
	}
	s, err := openSegment(l.dir, l.active().next, l.cfg.IndexInterval, true)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSince = l.now()
	return nil
}

// Read 读出 offset 对应的记录。
func (l *Log) Read(off uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 两级查找：先按 base 二分找到段，再用段内稀疏索引定位。
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].base > off
	})
	if i == 0 {
		return Record{}, ErrOffsetOutOfRange // 比最老的段还老：已被删除
	}
	return l.segments[i-1].read(off)
}

// OldestOffset 是还能读到的最小 offset。
func (l *Log) OldestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].base
}

// NextOffset 是下一条记录会得到的 offset。
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active().next
}

// DeleteBefore 删除最后写入时间早于 cutoff 的旧段（按时间的保留策略），返回删了几段。
//
// 两条规则：
//   - 活跃段永远不删，它还在被写。这就是为什么需要按时间滚动：
//     流量很小的 topic 活跃段一直写不满，不强制滚动的话，里面几个月前的数据永远删不掉。
//   - 从最老的段开始删，遇到第一个没过期的就停，保证剩下的 offset 是连续的。
func (l *Log) DeleteBefore(cutoff time.Time) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0
	for len(l.segments) > 1 {
		s := l.segments[0]
		mt, err := s.modTime()
		if err != nil {
			return n, err
		}
		if !mt.Before(cutoff) {
			break
		}
		if err := s.remove(); err != nil {
			return n, err
		}
		l.segments = l.segments[1:]
		n++
	}
	return n, nil
}

// Sync 把活跃段刷盘。调用方在向客户端确认写入之前调用它，才能保证「确认过的写入不丢」。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active().sync()
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var errs []error
	for _, s := range l.segments {
		errs = append(errs, s.close())
	}
	return errors.Join(errs...)
}
