package storage

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	for _, in := range []Record{
		{Offset: 42, Value: []byte("hello, 世界")},
		{Offset: 43, Key: []byte("user-1"), Value: []byte("alice")},
		{Offset: 44, Key: []byte("user-1"), Tombstone: true},
	} {
		buf, err := Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := Decode(bytes.NewReader(buf))
		if err != nil {
			t.Fatal(err)
		}
		if out.Offset != in.Offset || !bytes.Equal(out.Key, in.Key) ||
			!bytes.Equal(out.Value, in.Value) || out.Tombstone != in.Tombstone {
			t.Fatalf("got %+v, want %+v", out, in)
		}
	}
}

func TestDecodeMultipleThenEOF(t *testing.T) {
	var file bytes.Buffer
	for i := uint64(0); i < 3; i++ {
		buf, _ := Encode(Record{Offset: i, Value: []byte{byte(i)}})
		file.Write(buf)
	}
	for i := uint64(0); i < 3; i++ {
		r, err := Decode(&file)
		if err != nil || r.Offset != i {
			t.Fatalf("record %d: %+v, %v", i, r, err)
		}
	}
	if _, err := Decode(&file); err != io.EOF {
		t.Fatalf("want io.EOF at clean end, got %v", err)
	}
}

// 模拟断电：在每一个字节位置截断，都必须被识别为半写，而不是读出一条「看起来正常」的记录。
func TestTornWriteAtEveryByte(t *testing.T) {
	buf, _ := Encode(Record{Offset: 7, Key: []byte("k"), Value: []byte("some value")})
	for cut := 1; cut < len(buf); cut++ {
		_, err := Decode(bytes.NewReader(buf[:cut]))
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("cut at %d: want io.ErrUnexpectedEOF, got %v", cut, err)
		}
	}
}

// 模拟磁盘位翻转：任何一个比特出错都必须被发现。
func TestBitFlipAnywhereIsDetected(t *testing.T) {
	buf, _ := Encode(Record{Offset: 7, Key: []byte("k"), Value: []byte("some value")})
	for i := range buf {
		for bit := 0; bit < 8; bit++ {
			bad := bytes.Clone(buf)
			bad[i] ^= 1 << bit
			_, err := Decode(bytes.NewReader(bad))
			// length 字段被改大时，可能表现为「数据不够」，这也算被发现。
			if !errors.Is(err, ErrCorrupt) && err != io.ErrUnexpectedEOF {
				t.Fatalf("flip byte %d bit %d went undetected, err=%v", i, bit, err)
			}
		}
	}
}

func TestTooLarge(t *testing.T) {
	if _, err := Encode(Record{Value: make([]byte, MaxValueSize+1)}); err != ErrTooLarge {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	if _, err := Encode(Record{Tombstone: true}); err != ErrNoKey {
		t.Fatalf("tombstone without key: want ErrNoKey, got %v", err)
	}
}
