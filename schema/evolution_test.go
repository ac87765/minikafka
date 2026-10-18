package schema_test

import (
	"testing"

	"github.com/ac87765/minikafka/pb"
	recordbroken "github.com/ac87765/minikafka/pb/compat/broken"
	recordv1 "github.com/ac87765/minikafka/pb/compat/v1"
	"google.golang.org/protobuf/proto"
)

// DDIA 第 4 章的两个方向（容易记反）：
//
//	向后兼容 backward：新代码能读旧代码写的数据。
//	向前兼容 forward： 旧代码能读新代码写的数据。
//
// 滚动升级时两者必须同时成立：集群里新旧节点并存，数据在它们之间来回流动；
// 磁盘上的旧段还会被新代码读很多年。

// 向后兼容：v2 读 v1 写的数据。v1 根本不知道 timestamp_ms 和 headers，
// 字节里没有它们，v2 解出来就是零值。所以新字段的零值必须有合理含义（这里是「不知道」）。
func TestBackwardCompatNewReadsOld(t *testing.T) {
	old := &recordv1.Record{Offset: 42, Key: []byte("user-1"), Value: []byte("alice"), Tombstone: false}
	data, err := proto.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}

	var cur pb.Record
	if err := proto.Unmarshal(data, &cur); err != nil {
		t.Fatalf("新代码读旧数据失败：%v", err)
	}
	if cur.Offset != 42 || string(cur.Key) != "user-1" || string(cur.Value) != "alice" || cur.Tombstone {
		t.Fatalf("v1 已有的字段读错了：%v", &cur)
	}
	if cur.TimestampMs != 0 || len(cur.Headers) != 0 {
		t.Fatalf("v1 数据里没有新字段，应该是零值：%v", &cur)
	}
}

// 向前兼容：v1 读 v2 写的数据。v1 不认识字段 5 和 6，但 tag 里有 wire type，
// 它知道这个字段有多长，于是能跳过去，接着解析后面的字段。
func TestForwardCompatOldReadsNew(t *testing.T) {
	data := marshalV2(t)

	var old recordv1.Record
	if err := proto.Unmarshal(data, &old); err != nil {
		t.Fatalf("旧代码读新数据失败：%v", err)
	}
	if old.Offset != 7 || string(old.Key) != "k" || string(old.Value) != "v" {
		t.Fatalf("v1 认识的字段读错了：%v", &old)
	}
	// 不认识的字段没有被扔掉，而是原样存在 unknown fields 里。
	if len(old.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("v1 应该保留不认识的字段")
	}
}

// DDIA 图 4-7 的陷阱：旧代码读了新数据、改了一下、再写回去，新字段还在吗？
// 如果旧代码一直拿着 protobuf 对象，unknown fields 会跟着写回去，新字段还在。
func TestOldCodeRewritePreservesNewFields(t *testing.T) {
	data := marshalV2(t)

	var old recordv1.Record
	if err := proto.Unmarshal(data, &old); err != nil {
		t.Fatal(err)
	}
	old.Value = []byte("v-updated-by-old-code") // 旧代码只改它认识的字段
	rewritten, err := proto.Marshal(&old)
	if err != nil {
		t.Fatal(err)
	}

	var cur pb.Record
	if err := proto.Unmarshal(rewritten, &cur); err != nil {
		t.Fatal(err)
	}
	if string(cur.Value) != "v-updated-by-old-code" {
		t.Fatalf("旧代码的修改丢了：%q", cur.Value)
	}
	if cur.TimestampMs != 1700000000000 || len(cur.Headers) != 1 {
		t.Fatalf("旧代码写回后，新字段应该原样保留：%v", &cur)
	}
}

// 同一个陷阱的另一面：旧代码把 protobuf 对象转成自己的结构体（应用代码里很常见），
// 再从结构体转回去写，unknown fields 就在这一步丢了。protobuf 库保护不了你。
func TestOldCodeViaDomainStructLosesNewFields(t *testing.T) {
	data := marshalV2(t)

	var old recordv1.Record
	if err := proto.Unmarshal(data, &old); err != nil {
		t.Fatal(err)
	}
	type domainRecord struct { // 旧代码自己的模型，只有它认识的字段
		Offset     uint64
		Key, Value []byte
	}
	m := domainRecord{old.Offset, old.Key, old.Value}
	rewritten, _ := proto.Marshal(&recordv1.Record{Offset: m.Offset, Key: m.Key, Value: m.Value})

	var cur pb.Record
	if err := proto.Unmarshal(rewritten, &cur); err != nil {
		t.Fatal(err)
	}
	if cur.TimestampMs != 0 || len(cur.Headers) != 0 {
		t.Fatalf("预期新字段已经丢失（这正是要演示的问题），实际：%v", &cur)
	}
}

// 错误的演进：编号 5 原来是 int64 timestamp_ms，被复用成 string producer_id。
// 两个方向都不报错，数据就这样悄悄没了。这比报错更糟糕。
func TestReusedFieldNumberSilentlyLosesData(t *testing.T) {
	// v2 写的时间戳，被「错误的新版本」读。
	var broken recordbroken.Record
	if err := proto.Unmarshal(marshalV2(t), &broken); err != nil {
		t.Fatalf("没想到会报错：%v", err)
	}
	if broken.ProducerId != "" {
		t.Fatalf("producer_id 不应该有值：%q", broken.ProducerId)
	}
	// 字段 5 的 wire type 是 varint，而 broken 期望 LEN，对不上，只好当作不认识的字段。

	// 反过来：broken 写的 producer_id，被 v2 读。
	data, _ := proto.Marshal(&recordbroken.Record{Offset: 1, ProducerId: "producer-a"})
	var cur pb.Record
	if err := proto.Unmarshal(data, &cur); err != nil {
		t.Fatalf("没想到会报错：%v", err)
	}
	if cur.TimestampMs != 0 {
		t.Fatalf("timestamp_ms 不应该有值：%d", cur.TimestampMs)
	}
}

// 把同一个编号从 int64 改成 int32 呢？wire type 都是 varint，能解析，但大的值会被截断。
// 这就是「不许改类型」的另一个原因：有时候不是丢数据，而是得到一个错误的数。
func TestWidthChangeTruncates(t *testing.T) {
	data, _ := proto.Marshal(&pb.Record{TimestampMs: 1700000000000})
	// 手工模拟一个把 timestamp_ms 声明成 int32 的读者：取 varint 后转 int32。
	v, n := decodeVarint(data[1:])
	if n == 0 {
		t.Fatal("varint 解析失败")
	}
	if got := int32(v); int64(got) == 1700000000000 {
		t.Fatal("int32 不可能装下这个时间戳")
	}
}

func marshalV2(t *testing.T) []byte {
	t.Helper()
	data, err := proto.Marshal(&pb.Record{
		Offset:      7,
		Key:         []byte("k"),
		Value:       []byte("v"),
		TimestampMs: 1700000000000,
		Headers:     []*pb.Header{{Key: "trace-id", Value: []byte("abc")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// decodeVarint 是 varint 的手写解码，和 TestVarintSizes 里的注释对照着看。
func decodeVarint(b []byte) (uint64, int) {
	var v uint64
	for i, c := range b {
		if i == 10 {
			return 0, 0
		}
		v |= uint64(c&0x7f) << (7 * i)
		if c&0x80 == 0 {
			return v, i + 1
		}
	}
	return 0, 0
}
