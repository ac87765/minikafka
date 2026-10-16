package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// 一条记录在磁盘上的格式（大端序）：
//
//	+--------+-----------+----------+-----------+-------------+-----+-------+
//	| crc 4B | offset 8B | attrs 1B | keyLen 4B | valueLen 4B | key | value |
//	+--------+-----------+----------+-----------+-------------+-----+-------+
//	         |<--------------------- crc 覆盖的范围 ---------------------->|
//
// crc 放在最前面，覆盖它后面的所有字节，所以任何一个字节坏了都能被发现。
// attrs 是标志位，目前只用了最低位表示墓碑（tombstone，即删除标记）。
// keyLen 为 0 表示这条记录没有 key，压缩时不会被删除。
const (
	crcSize      = 4
	offsetSize   = 8
	attrsSize    = 1
	keyLenSize   = 4
	valueLenSize = 4
	headerSize   = crcSize + offsetSize + attrsSize + keyLenSize + valueLenSize

	offsetPos   = crcSize
	attrsPos    = offsetPos + offsetSize
	keyLenPos   = attrsPos + attrsSize
	valueLenPos = keyLenPos + keyLenSize

	attrTombstone = 1 << 0

	// 长度上限不只是业务限制：长度字段坏了可能变成一个巨大的数，
	// 解码时必须先检查，否则会在校验 crc 之前就去分配几个 GB 的内存。
	MaxKeySize   = 64 << 10
	MaxValueSize = 1 << 20
)

var (
	ErrCorrupt  = errors.New("storage: corrupt record")
	ErrTooLarge = errors.New("storage: record too large")
	ErrNoKey    = errors.New("storage: tombstone requires a key")
)

// Castagnoli 多项式（CRC-32C），Kafka、ext4、iSCSI 都用它，现代 CPU 有硬件指令加速。
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record 是日志中的一条记录。
// Offset 是它在日志里的永久编号，和它在文件里排第几条无关。
type Record struct {
	Offset    uint64
	Key       []byte // nil 表示没有 key
	Value     []byte
	Tombstone bool // true 表示「Key 被删除了」，此时 Value 为空
}

func (r Record) encodedSize() int { return headerSize + len(r.Key) + len(r.Value) }

// Encode 把记录编码成磁盘格式。
func Encode(r Record) ([]byte, error) {
	if len(r.Key) > MaxKeySize || len(r.Value) > MaxValueSize {
		return nil, ErrTooLarge
	}
	if r.Tombstone && len(r.Key) == 0 {
		return nil, ErrNoKey
	}
	buf := make([]byte, r.encodedSize())
	binary.BigEndian.PutUint64(buf[offsetPos:], r.Offset)
	if r.Tombstone {
		buf[attrsPos] = attrTombstone
	}
	binary.BigEndian.PutUint32(buf[keyLenPos:], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(buf[valueLenPos:], uint32(len(r.Value)))
	copy(buf[headerSize:], r.Key)
	copy(buf[headerSize+len(r.Key):], r.Value)
	binary.BigEndian.PutUint32(buf, crc32.Checksum(buf[crcSize:], crcTable))
	return buf, nil
}

// Decode 从 rd 读出一条记录。返回的错误有三种含义：
//
//	io.EOF              正好读到干净的结尾，没有更多记录
//	io.ErrUnexpectedEOF 记录只写了一半（半写，torn write）
//	ErrCorrupt          字节都在，但内容对不上（crc 不符或长度不合理）
//
// 崩溃恢复时，后两种都意味着「从这条记录开始截断」。
func Decode(rd io.Reader) (Record, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return Record{}, err // io.EOF 或 io.ErrUnexpectedEOF
	}
	keyLen := binary.BigEndian.Uint32(hdr[keyLenPos:])
	valueLen := binary.BigEndian.Uint32(hdr[valueLenPos:])
	if keyLen > MaxKeySize || valueLen > MaxValueSize {
		return Record{}, ErrCorrupt
	}
	body := make([]byte, keyLen+valueLen)
	if _, err := io.ReadFull(rd, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // 头部完整但后面一个字节都没有，也是半写
		}
		return Record{}, err
	}
	h := crc32.New(crcTable)
	h.Write(hdr[crcSize:])
	h.Write(body)
	if h.Sum32() != binary.BigEndian.Uint32(hdr[:crcSize]) {
		return Record{}, ErrCorrupt
	}
	r := Record{
		Offset:    binary.BigEndian.Uint64(hdr[offsetPos:]),
		Tombstone: hdr[attrsPos]&attrTombstone != 0,
		Value:     body[keyLen:],
	}
	if keyLen > 0 {
		r.Key = body[:keyLen]
	}
	return r, nil
}
