package storage

import (
	"fmt"
	"testing"
)

func TestSegmentAppendRead(t *testing.T) {
	dir := t.TempDir()
	s, err := openSegment(dir, 100, 64, true) // base=100，每 64 字节一个索引项
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		off, err := s.append(Record{Value: []byte(fmt.Sprintf("value-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
		if off != uint64(100+i) {
			t.Fatalf("got offset %d, want %d", off, 100+i)
		}
	}
	// 稀疏：索引项应该明显少于记录数
	if got := len(s.idx.entries); got == 0 || got >= n {
		t.Fatalf("index has %d entries for %d records, want sparse", got, n)
	}
	for i := 0; i < n; i++ {
		r, err := s.read(uint64(100 + i))
		if err != nil {
			t.Fatalf("read %d: %v", 100+i, err)
		}
		if want := fmt.Sprintf("value-%d", i); string(r.Value) != want {
			t.Fatalf("read %d: got %q, want %q", 100+i, r.Value, want)
		}
	}
	for _, off := range []uint64{99, 100 + n} {
		if _, err := s.read(off); err != ErrOffsetOutOfRange {
			t.Fatalf("read %d: want ErrOffsetOutOfRange, got %v", off, err)
		}
	}
}

func TestSegmentReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSegment(dir, 0, 64, true)
	for i := 0; i < 20; i++ {
		s.append(Record{Value: []byte(fmt.Sprintf("v%d", i))})
	}
	s.close()

	s, err := openSegment(dir, 0, 64, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s.next != 20 {
		t.Fatalf("after reopen next=%d, want 20", s.next)
	}
	off, _ := s.append(Record{Value: []byte("v20")})
	if off != 20 {
		t.Fatalf("append after reopen got offset %d, want 20", off)
	}
	r, err := s.read(7)
	if err != nil || string(r.Value) != "v7" {
		t.Fatalf("read 7 after reopen: %q, %v", r.Value, err)
	}
}
