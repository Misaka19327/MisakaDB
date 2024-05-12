package storage

import (
	"encoding/binary"
	"hash/crc32"
)

// entry编码结构：
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//         |--------------------------crc check---------------------------|

// header长度：
// crc32	typ    kSize	vSize	expiredAt
//  4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)

// MaxEntryHeaderLength 规定Entry头部信息最长长度为25 同时整个Entry长度不能小于25
const MaxEntryHeaderLength = 25

// EntryType 标识entry类型
type EntryType byte

const (
	TypeDelete EntryType = iota + 1 // 标识记录删除信息的entry
	TypeRecord                      // 标识记录信息的entry
)

// 因为整个数据库的操作 增删改查 体现在文件上的只有删除和新增两种（改可以通过新增的方式进行覆盖）

// Entry 具体一个Entry所记录的信息
type Entry struct {
	Key       []byte    // 键
	Value     []byte    // 值
	EntryType EntryType // entry类型标识
	ExpiredAt int64     // 过期时间 这里放时间戳
}

// entryHeaderInfo 一个entry所对应的header信息
type entryHeaderInfo struct {
	crc         uint32    // 校验和 对剩余部分进行检验
	entryType   EntryType // entry类型标识
	keyLength   uint32
	valueLength uint32
	expiredAt   int64 // 过期时间 这里放时间戳
}

// Encode 将entry转换为byte数组 另外返回写入内容的长度 byte数组强制必须大于25 如果不够就用0凑足成25 否则读取文件时不够25会EOF
func (e *Entry) Encode() ([]byte, int) {
	if e == nil {
		return nil, 0
	}

	header := make([]byte, MaxEntryHeaderLength)

	// 先放头信息里除了校验和之外的其他东西
	header[4] = byte(e.EntryType)
	index := 5
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	// 再放key和value
	size := index + len(e.Key) + len(e.Value)
	if size < MaxEntryHeaderLength {
		size = MaxEntryHeaderLength
	}
	buffer := make([]byte, size)
	copy(buffer[:index], header[:])
	copy(buffer[index:], e.Key)
	copy(buffer[index+len(e.Key):], e.Value)

	// 最后放crc校验和
	crc := crc32.ChecksumIEEE(buffer[4 : index+len(e.Key)+len(e.Value)])
	binary.LittleEndian.PutUint32(buffer[:4], crc)
	return buffer, size
}

// 将给定的byte数组解码为entryHeaderInfo 即Entry的头信息 返回该头信息和头信息的字节长度
func decodeEntryHeader(input []byte) (*entryHeaderInfo, int64) {
	if len(input) <= 4 {
		return nil, 0
	}
	result := &entryHeaderInfo{
		crc:       binary.LittleEndian.Uint32(input[:4]),
		entryType: EntryType(input[4]),
	}
	index := 5
	kSize, n := binary.Varint(input[index:])
	result.keyLength = uint32(kSize)
	index += n

	vSize, n := binary.Varint(input[index:])
	result.valueLength = uint32(vSize)
	index += n

	e, n := binary.Varint(input[index:])
	result.expiredAt = e

	return result, int64(index + n)
}

// getEntryCRC 给定字节数组和Entry 计算该数组+Entry键+Entry值的crc校验和
func getEntryCRC(entry *Entry, h []byte) uint32 {
	if entry == nil {
		return 0
	}
	result := crc32.ChecksumIEEE(h)
	result = crc32.Update(result, crc32.IEEETable, entry.Key)
	result = crc32.Update(result, crc32.IEEETable, entry.Value)
	return result
}
