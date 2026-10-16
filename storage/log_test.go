package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func appendN(t *testing.T, l *Log, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		off, err := l.Append(nil, []byte(fmt.Sprintf("value-%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if off != uint64(i) {
			t.Fatalf("append %d got offset %d", i, off)
		}
	}
}

func checkRead(t *testing.T, l *Log, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		r, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if want := fmt.Sprintf("value-%04d", i); string(r.Value) != want {
			t.Fatalf("read %d: got %q want %q", i, r.Value, want)
		}
	}
}

// 每条记录 21B 头 + 10B 值 = 31B，一段最多 100B，也就是每段 3 条。
var smallCfg = Config{MaxSegmentBytes: 100, IndexInterval: 32}

func TestLogRollsBySize(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	appendN(t, l, 0, 10)
	checkRead(t, l, 0, 10)

	if got := len(l.segments); got != 4 { // 3+3+3+1
		t.Fatalf("got %d segments, want 4", got)
	}
	for _, s := range l.segments {
		if s.size > smallCfg.MaxSegmentBytes {
			t.Fatalf("segment %d is %d bytes, exceeds max", s.base, s.size)
		}
	}
	// 文件名就是 base offset
	for _, name := range []string{"00000000000000000000.log", "00000000000000000003.log", "00000000000000000009.log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
}

func TestLogReopen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg)
	appendN(t, l, 0, 10)
	l.Close()

	l, err := Open(dir, smallCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.NextOffset() != 10 {
		t.Fatalf("NextOffset after reopen = %d, want 10", l.NextOffset())
	}
	appendN(t, l, 10, 15)
	checkRead(t, l, 0, 15)
}

func TestLogRollsByTime(t *testing.T) {
	l, _ := Open(t.TempDir(), Config{MaxSegmentAge: time.Hour})
	defer l.Close()
	clock := time.Now()
	l.now = func() time.Time { return clock }
	l.activeSince = clock

	appendN(t, l, 0, 2)
	clock = clock.Add(2 * time.Hour) // 远没写满 1GB，但已经过了一小时
	appendN(t, l, 2, 3)
	if got := len(l.segments); got != 2 {
		t.Fatalf("got %d segments, want 2 after time roll", got)
	}
	checkRead(t, l, 0, 3)
}

func TestLogRetention(t *testing.T) {
	l, _ := Open(t.TempDir(), smallCfg)
	defer l.Close()
	appendN(t, l, 0, 10) // 4 段：base 0, 3, 6, 9

	// 把前两段伪装成 8 天前写的
	old := time.Now().Add(-8 * 24 * time.Hour)
	for _, s := range l.segments[:2] {
		os.Chtimes(s.log.Name(), old, old)
	}
	n, err := l.DeleteBefore(time.Now().Add(-7 * 24 * time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("deleted %d segments, err=%v; want 2", n, err)
	}
	if l.OldestOffset() != 6 {
		t.Fatalf("OldestOffset = %d, want 6", l.OldestOffset())
	}
	if _, err := l.Read(2); err != ErrOffsetOutOfRange {
		t.Fatalf("read deleted offset: want ErrOffsetOutOfRange, got %v", err)
	}
	checkRead(t, l, 6, 10)

	// 活跃段即使过期也不能删
	for _, s := range l.segments {
		os.Chtimes(s.log.Name(), old, old)
	}
	l.DeleteBefore(time.Now())
	if len(l.segments) != 1 || l.NextOffset() != 10 {
		t.Fatalf("active segment must survive retention; segments=%d next=%d", len(l.segments), l.NextOffset())
	}
}
