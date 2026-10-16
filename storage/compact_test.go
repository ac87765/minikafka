package storage

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

// replay 从头读一遍日志，构建「每个 key 的最终值」。这就是一个最简单的物化视图。
func replay(t *testing.T, l *Log) map[string]string {
	t.Helper()
	state := map[string]string{}
	for off := l.OldestOffset(); off < l.NextOffset(); off++ {
		r, err := l.Read(off)
		if errors.Is(err, ErrOffsetOutOfRange) {
			continue // 被压缩掉的空洞
		}
		if err != nil {
			t.Fatalf("read %d: %v", off, err)
		}
		if r.Tombstone {
			delete(state, string(r.Key))
		} else {
			state[string(r.Key)] = string(r.Value)
		}
	}
	return state
}

func countRecords(t *testing.T, l *Log) int {
	t.Helper()
	n := 0
	for off := l.OldestOffset(); off < l.NextOffset(); off++ {
		if _, err := l.Read(off); err == nil {
			n++
		}
	}
	return n
}

// 最重要的性质：压缩前后重放得到的状态完全一样。随机写入、覆盖、删除，反复验证。
func TestCompactionPreservesState(t *testing.T) {
	for _, retention := range []time.Duration{time.Nanosecond, time.Hour} { // 墓碑立刻清理 / 保留
		for seed := uint64(0); seed < 20; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			l, err := Open(t.TempDir(), Config{MaxSegmentBytes: 200, TombstoneRetention: retention})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 300; i++ {
				key := []byte(fmt.Sprintf("k%d", rng.IntN(10)))
				if rng.IntN(5) == 0 {
					l.Delete(key)
				} else {
					l.Append(key, []byte(fmt.Sprintf("v%d", i)))
				}
			}
			before := replay(t, l)
			nBefore, next := countRecords(t, l), l.NextOffset()

			time.Sleep(2 * time.Nanosecond)
			if _, err := l.Compact(); err != nil {
				t.Fatal(err)
			}
			if after := replay(t, l); !maps.Equal(before, after) {
				t.Fatalf("seed %d retention %v: state changed\nbefore %v\nafter  %v", seed, retention, before, after)
			}
			if l.NextOffset() != next {
				t.Fatalf("NextOffset changed from %d to %d", next, l.NextOffset())
			}
			if countRecords(t, l) >= nBefore {
				t.Fatalf("seed %d: compaction removed nothing", seed)
			}
			l.Close()
		}
	}
}

func TestCompactionKeepsLatestAndOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{MaxSegmentBytes: 100}
	l, _ := Open(dir, cfg)
	defer func() { l.Close() }()
	// 每条记录 21+1+2=24B，每段 4 条
	l.Append([]byte("a"), []byte("a1")) // 0
	l.Append([]byte("b"), []byte("b1")) // 1
	l.Append([]byte("a"), []byte("a2")) // 2
	l.Append(nil, []byte("nk"))         // 3 没有 key，永远保留
	l.Append([]byte("a"), []byte("a3")) // 4（第二段）
	l.Append([]byte("c"), []byte("c1")) // 5
	l.Append([]byte("b"), []byte("b2")) // 6
	l.Append([]byte("c"), []byte("c2")) // 7
	l.Append([]byte("z"), []byte("z1")) // 8（活跃段，不压缩）

	stats, err := l.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RecordsRemoved != 4 { // 0,1,2,5
		t.Fatalf("removed %d records, want 4", stats.RecordsRemoved)
	}
	for _, off := range []uint64{0, 1, 2, 5} {
		if _, err := l.Read(off); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("offset %d should be compacted away, got %v", off, err)
		}
	}
	want := map[uint64]string{3: "nk", 4: "a3", 6: "b2", 7: "c2", 8: "z1"}
	for off, v := range want {
		r, err := l.Read(off)
		if err != nil || string(r.Value) != v {
			t.Fatalf("offset %d: got %q, %v; want %q", off, r.Value, err, v)
		}
	}
	// 压缩后重启：带空洞的段也能正确重建索引，offset 接着往下分配
	l.Close()
	l, _ = Open(dir, cfg)
	for off, v := range want {
		if r, err := l.Read(off); err != nil || string(r.Value) != v {
			t.Fatalf("after reopen offset %d: got %q, %v; want %q", off, r.Value, err, v)
		}
	}
	off, _ := l.Append([]byte("a"), []byte("a4"))
	if off != 9 {
		t.Fatalf("append after compaction got offset %d, want 9", off)
	}
}

func TestTombstoneRetention(t *testing.T) {
	l, _ := Open(t.TempDir(), Config{MaxSegmentBytes: 100, TombstoneRetention: time.Hour})
	defer l.Close()
	l.Append([]byte("a"), []byte("a1")) // 0
	l.Delete([]byte("a"))               // 1 墓碑
	l.Append([]byte("b"), []byte("b1")) // 2
	l.Append([]byte("b"), []byte("b2")) // 3
	l.Append([]byte("x"), []byte("x1")) // 4 活跃段

	l.Compact()
	if r, err := l.Read(1); err != nil || !r.Tombstone {
		t.Fatalf("fresh tombstone must survive compaction: %+v, %v", r, err)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("value shadowed by tombstone should be removed, got %v", err)
	}

	// 一小时之后，墓碑也该清理了。修改时间要在重写后保持不变，否则墓碑永远「很新」。
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(l.segments[0].log.Name(), old, old)
	l.Compact()
	if _, err := l.Read(1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("expired tombstone should be removed, got %v", err)
	}
	if mt, _ := l.segments[0].modTime(); !mt.Equal(old) {
		t.Fatalf("rewrite changed segment mtime to %v, want %v", mt, old)
	}
}

// 模拟压缩到一半崩溃：临时文件写了一半，rename 还没发生。重启后旧数据完好，临时文件被清理。
func TestCrashDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg)
	appendN(t, l, 0, 10)
	l.Close()

	leftover := segmentPath(dir, 0, ".log") + cleanedSuffix
	os.WriteFile(leftover, []byte("half-written garbage"), 0o644)

	l, err := Open(dir, smallCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	checkRead(t, l, 0, 10)
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("leftover %s should be removed on open", leftover)
	}
}
