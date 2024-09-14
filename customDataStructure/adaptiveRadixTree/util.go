package adaptiveRadixTree

import (
	"fmt"
	"strings"
)

func replaceRef[T Value](oldNode **artNode[T], newNode *artNode[T]) {
	*oldNode = newNode
}

func replaceNode[T Value](oldNode *artNode[T], newNode *artNode[T]) {
	*oldNode = *newNode
}

// longestCommonPrefix 索引从 depth 开始 比较 l1 和 l2 的 key 返回两者间相同的长度
// 返回的长度不包括 depth
func longestCommonPrefix[T Value](l1 *leaf[T], l2 *leaf[T], depth uint32) uint32 {
	l1Key, l2Key := l1.key, l2.key
	index, limit := depth, min(uint32(len(l1Key)), uint32(len(l2Key)))
	for ; index < limit; index++ {
		if l1Key[index] != l2Key[index] {
			break
		}
	}

	return index - depth
}

// numToBinIncludeLeadingZero 将传入的数字转换成带前导0的长度16的二进制表示的字符串
func numToBinIncludeLeadingZero(n uint16) string {
	result := fmt.Sprintf("%b", n)
	strBuilder := &strings.Builder{}
	if len(result) < 16 {
		for i := 0; i < 16-len(result); i++ {
			strBuilder.WriteString("0")
		}
	}
	strBuilder.WriteString(result)
	return strBuilder.String()
}
