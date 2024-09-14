package util

import (
	"MisakaDB/logger"
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

// EncodeKeyAndField 为了Hash等类型提供 将Key和Field编码在一起形成一个新的Key
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

// DecodeKeyAndField 为了Hash等类型提供 将由EncodeKeyAndField编码的结果解码为key和field
func DecodeKeyAndField(input []byte) (key string, field string, e error) {
	index := 0
	kSize, n := binary.Varint(input)
	if n <= 0 {
		logger.GenerateErrorLog(false, false, logger.DecodeKeyAndFieldFailed.Error(), TurnByteArrayToString(input))
		return "", "", logger.DecodeKeyAndFieldFailed
	}
	index += n
	fSize, n := binary.Varint(input[index:])
	if n <= 0 {
		logger.GenerateErrorLog(false, false, logger.DecodeKeyAndFieldFailed.Error(), TurnByteArrayToString(input))
		return "", "", logger.DecodeKeyAndFieldFailed
	}
	index += n
	key = string(input[index : int64(index)+kSize])
	index += int(kSize)
	field = string(input[index : int64(index)+fSize])
	e = nil
	return
}

// TurnByteArrayToString 将byte数组转换为string 更好的判断问题所在
func TurnByteArrayToString(input []byte) string {
	result := ""
	for _, v := range input {
		result += strconv.Itoa(int(v)) + " "
	}
	return result
}

// TurnByteArray2ToString 将byte二维数组转换为string
func TurnByteArray2ToString(input [][]byte) string {
	var result []string
	for _, v := range input {
		result = append(result, string(v))
	}
	return strings.Join(result, " ")
}

// CalcTimeUnix 根据传入的单位和具体的数值 计算过期时间戳 单位支持ex - 秒和px - 毫秒两种
func CalcTimeUnix(unit string, t int) (int64, error) {
	switch unit {
	case "ex":
		return time.Now().Add(time.Duration(t) * time.Second).UnixMilli(), nil
	case "px":
		return time.Now().Add(time.Duration(t) * time.Millisecond).UnixMilli(), nil
	}
	return 0, logger.TimeUnitIsNotSupported
}
