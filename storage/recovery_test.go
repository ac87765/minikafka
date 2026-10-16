package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func lastSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	bases, err := listSegmentBases(dir)
	if err != nil || len(bases) == 0 {
		t.Fatal(err)
	}
	return segmentPath(dir, bases[len(bases)-1], ".log")
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// 模拟断电：最后一条记录只写了一部分。在它的每一个字节位置截断都试一遍。
// 每一次重启后都必须：前 9 条完好，半写的第 10 条消失，新写入从 offset 9 继续。
func TestRecoverTornWriteAtEveryByte(t *testing.T) {
	// 先算出第 10 条记录在文件里的起止位置
	base := t.TempDir()
	l, _ := Open(base, Config{})
	appendN(t, l, 0, 9)
	l.Close()
	path := lastSegmentFile(t, base)
	start := fileSize(t, path)
	l, _ = Open(base, Config{})
	appendN(t, l, 9, 10)
	l.Close()
	end := fileSize(t, path)
	full, _ := os.ReadFile(path)

	for cut := start + 1; cut < end; cut++ {
		dir := t.TempDir()
		p := filepath.Join(dir, filepath.Base(path))
		os.WriteFile(p, full[:cut], 0o644)

		l, err := Open(dir, Config{})
		if err != nil {
			t.Fatalf("cut at %d: open failed: %v", cut, err)
		}
		if l.NextOffset() != 9 {
			t.Fatalf("cut at %d: NextOffset=%d, want 9", cut, l.NextOffset())
		}
		if got := fileSize(t, p); got != start {
			t.Fatalf("cut at %d: file size %d after recovery, want %d", cut, got, start)
		}
		checkRead(t, l, 0, 9)
		appendN(t, l, 9, 12) // 截断之后能正常继续写
		checkRead(t, l, 0, 12)
		l.Close()
	}
}

// 活跃段中间出现位翻转：从坏记录开始全部截断（后面的记录也找不到边界了）。
func TestRecoverCorruptionInActiveSegment(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{})
	appendN(t, l, 0, 10)
	l.Close()

	path := lastSegmentFile(t, dir)
	data, _ := os.ReadFile(path)
	recSize := len(data) / 10
	data[5*recSize+headerSize] ^= 0xFF // 破坏第 6 条（offset 5）的值
	os.WriteFile(path, data, 0o644)

	l, err := Open(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.NextOffset() != 5 {
		t.Fatalf("NextOffset=%d, want 5", l.NextOffset())
	}
	checkRead(t, l, 0, 5)
}

// 旧段损坏不是半写，是磁盘坏了：必须报错，不能悄悄丢数据。
func TestCorruptionInOldSegmentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg)
	appendN(t, l, 0, 10) // 多个段
	l.Close()

	first := segmentPath(dir, 0, ".log")
	data, _ := os.ReadFile(first)
	data[len(data)-1] ^= 0xFF
	os.WriteFile(first, data, 0o644)

	_, err := Open(dir, smallCfg)
	if err == nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt opening log with corrupt old segment, got %v", err)
	}
}
