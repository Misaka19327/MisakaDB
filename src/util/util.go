package util

import (
	"MisakaDB/src/logger"
	"encoding/binary"
)

func EncodeKeyAndField(key string, field string) []byte {
	header := make([]byte, 10)
	index := 0
	index += binary.PutVarint(header, int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(field)))
	result := make([]byte, index+len(key)+len(field))
	copy(result[:index], header[:])
	copy(result[index:], key)
	copy(result[index+len(key):], field)
	return result
}

func DecodeKeyAndField(input []byte) (key string, field string, e error) {
	index := 0
	kSize, n := binary.Varint(input)
	if n <= 0 {
		return "", "", logger.DecodeKeyAndFieldFailed
	}
	index += n
	fSize, n := binary.Varint(input[index:])
	if n <= 0 {
		return "", "", logger.DecodeKeyAndFieldFailed
	}
	index += n
	key = string(input[index : int64(index)+kSize])
	index += int(kSize)
	field = string(input[index : int64(index)+fSize])
	e = nil
	return
}
