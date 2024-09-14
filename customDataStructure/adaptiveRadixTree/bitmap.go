package adaptiveRadixTree

import "strings"

// bitmap 对位图进行封装 把读写逻辑都封进来
// 其底层为 uint16 的切片
type bitmap struct {
	data            []uint16
	effectiveLength int
}

// newBitmap 获取一个新的 bitmap length 用于指定 bitmap 的有效长度
//
// 如果 length 不足16也是用 uint16 进行存储
func newBitmap(length int) *bitmap {
	arrayLength := length / 16
	if length%16 != 0 {
		arrayLength += 1
	}
	return &bitmap{
		data:            make([]uint16, arrayLength),
		effectiveLength: length,
	}
}

// set1 将指定的数位设置为1 如果指定的数位超过了初始化 bitmap 时指定的长度 将不做任何操作
func (b *bitmap) set1(index int) {
	if index >= b.effectiveLength {
		return
	}
	arrayIndex := index / 16
	i := index % 16
	if i == 0 {
		b.data[arrayIndex] |= 1
		return
	}
	b.data[arrayIndex] |= 1 << i
}

// set0 将指定的数位设置为0 如果指定的数位超过了初始化 bitmap 时指定的长度 将不做任何操作
func (b *bitmap) set0(index int) {
	if index >= b.effectiveLength {
		return
	}
	arrayIndex := index / 16
	i := index % 16
	if i == 0 {
		b.data[arrayIndex] &= 65534 // 655534 = 1111111111111110
		return
	}
	b.data[arrayIndex] &= ^(1 << i)
}

// get 指定数位是否为1 如果指定的数位超过了初始化直接返回 false
func (b *bitmap) get(index int) bool {
	if index >= b.effectiveLength {
		return false
	}
	arrayIndex := index / 16
	i := index % 16
	return (b.data[arrayIndex] & (1 << i)) != 0
}

// getNum 获取当前位图中数位为1的位的数量
func (b *bitmap) getNum() int {
	count := 0
	s := b.string()
	for i := range s {
		if s[i] == 49 {
			count += 1
		}
	}

	return count
}

func (b *bitmap) string() string {
	strBuilder := &strings.Builder{}
	for i := range b.data {
		if b.data[i] == 0 {
			strBuilder.WriteString("0 ")
		} else {
			strBuilder.WriteString(numToBinIncludeLeadingZero(b.data[i]))
			strBuilder.WriteString(" ")
		}
	}

	return strBuilder.String()
}
