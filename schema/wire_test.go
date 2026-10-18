package schema_test

import (
	"bytes"
	"testing"

	"github.com/ac87765/minikafka/pb"
	"google.golang.org/protobuf/proto"
)

// Protobuf 的线上格式是一串「字段」，每个字段 = tag + 内容。
//
//	tag = 字段编号 << 3 | wire type
//
// wire type 只有几种：0 = varint（整数、bool），2 = LEN（长度前缀 + 字节，用于 bytes、
// string、嵌套消息、repeated）。注意字节里没有字段名，也没有消息名。
func TestWireFormatByHand(t *testing.T) {
	r := &pb.Record{Offset: 1, Key: []byte("k"), Value: []byte("hi"), TimestampMs: 300}
	got, err := proto.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x08, 0x01, // 字段 1 (offset)，1<<3|0 = 0x08；值 1
		0x12, 0x01, 'k', // 字段 2 (key)，2<<3|2 = 0x12；长度 1；"k"
		0x1a, 0x02, 'h', 'i', // 字段 3 (value)，3<<3|2 = 0x1a；长度 2；"hi"
		// 字段 4 (tombstone) 是 false，也就是零值：proto3 不写零值，一个字节都不占
		0x28, 0xac, 0x02, // 字段 5 (timestamp_ms)，5<<3|0 = 0x28；300 的 varint
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("编码结果\n got  % x\n want % x", got, want)
	}
}

// varint：每个字节用低 7 位存数据，最高位为 1 表示「后面还有」。小的数占的字节少。
// 300 = 0b1_0010_1100，低 7 位 0101100 加续位得 0xac，剩下的 10 得 0x02。
func TestVarintSizes(t *testing.T) {
	for _, tc := range []struct {
		offset uint64
		size   int // 包括 1 字节的 tag
	}{
		{0, 0}, // 零值不写
		{1, 2},
		{127, 2},
		{128, 3},
		{1 << 32, 6},
		{1<<64 - 1, 11}, // 最坏情况 10 字节，比阶段 1 定长的 8 字节还多
	} {
		b, _ := proto.Marshal(&pb.Record{Offset: tc.offset})
		if len(b) != tc.size {
			t.Errorf("offset=%d 编码后 %d 字节，期望 %d", tc.offset, len(b), tc.size)
		}
	}
}
