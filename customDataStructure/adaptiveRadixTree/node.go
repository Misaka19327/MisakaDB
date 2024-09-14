package adaptiveRadixTree

import (
	"bytes"
	"math/bits"
	"unsafe"
)

type artNode[T Value] struct {
	reference unsafe.Pointer
	kind      Kind
}

// 鉴于 Goland 在低能耗下的检查实在不靠谱 写这么一个检查一下是否实现接口
var _ Node[Kind] = &artNode[Kind]{}

type nodePrototype[T Value] struct {
	prefixLen   uint32
	prefix      prefix
	childrenNum uint16
	zeroChild   *artNode[T] // 这东西是给正好断在别的键的中间的键用的 比如说 apple 和 appleWatch apple对应的叶子节点就放在这
}

type node4[T Value] struct {
	nodePrototype[T]
	children [node4Max]*artNode[T]
	keys     [node4Max]byte
	isExist  *bitmap
}

type node16[T Value] struct {
	nodePrototype[T]
	children [node16Max]*artNode[T]
	keys     [node16Max]byte
	isExist  *bitmap
}

type node48[T Value] struct {
	nodePrototype[T]
	children [node48Max]*artNode[T]
	keys     [node256Max]byte
	isExist  *bitmap
}

type node256[T Value] struct {
	nodePrototype[T]
	children [node256Max]*artNode[T]
}

type leaf[T Value] struct {
	key   Key
	value T
}

// 子节点类型转换

func (a *artNode[T]) node() *nodePrototype[T] {
	return (*nodePrototype[T])(a.reference)
}

func (a *artNode[T]) node4() *node4[T] {
	return (*node4[T])(a.reference)
}

func (a *artNode[T]) node16() *node16[T] {
	return (*node16[T])(a.reference)
}

func (a *artNode[T]) node48() *node48[T] {
	return (*node48[T])(a.reference)
}

func (a *artNode[T]) node256() *node256[T] {
	return (*node256[T])(a.reference)
}

func (a *artNode[T]) leaf() *leaf[T] {
	return (*leaf[T])(a.reference)
}

func (a *artNode[T]) isLeaf() bool {
	return a.kind == Leaf
}

// Node 接口实现

func (a *artNode[T]) Key() Key {
	if a.isLeaf() {
		return a.leaf().key
	}
	return nil
}

func (a *artNode[T]) Value() (value T) {
	if a.isLeaf() {
		return a.leaf().value
	}

	return
}

func (a *artNode[T]) Kind() Kind {
	return a.kind
}

// setPrefix 为当前节点设置前缀 前缀实际内容来自参数 key 前缀实际长度由参数 prefixLen 控制
func (a *artNode[T]) setPrefix(key Key, prefixLen uint32) *artNode[T] {
	node := a.node()
	node.prefixLen = prefixLen
	for i := uint32(0); i < min(prefixLen, MaxPrefixLength); i++ {
		node.prefix[i] = key[i]
	}

	return a
}

// match 给定的 key 从 offset 位置开始 和调用者的前缀开始匹配 返回第一个不同的字符在调用者的前缀中的索引
//
// 如果 offset 大于 key 的长度 直接返回0
//
// 如果能够匹配到最后 无论是 key 还是 调用者的前缀到尽头 返回的都是最后一个能够匹配的字符在调用者的前缀中的索引+1
//
// 因为匹配对于调用者的前缀来说是从头开始 对于传入的 key 是从 offset 开始 所以返回值有一个统一的含义 即上述两者能够匹配的字符数量
func (a *artNode[T]) match(key Key, offset uint32) uint32 {
	index := uint32(0)
	if len(key)-int(offset) < 0 {
		return index
	}

	node := a.node()

	limit := min(min(node.prefixLen, MaxPrefixLength), uint32(len(key))-offset)
	for ; index < limit; index++ {
		if node.prefix[index] != key[index+offset] {
			return index
		}
	}

	return index
}

// matchDeep 用当前节点下的最小字典序的叶子节点的键和传入的键 从 offset 开始进行比较 找到第一个出现不同的索引
//
// 注意 这个第一个出现不同的索引 对于传入的 key 是从 offset 开始从0计算的 对于调用者的前缀来说也是
func (a *artNode[T]) matchDeep(key Key, offset uint32) uint32 {
	mismatchIndex := a.match(key, offset)
	if mismatchIndex < MaxPrefixLength {
		return mismatchIndex
	}
	// 当前节点的前缀能够全部匹配给定的 key 和 value 需要去叶子节点继续匹配

	minimumLeaf := a.findMinimumKey()
	limit := min(uint32(len(minimumLeaf.key)), uint32(len(key))) - offset
	for ; mismatchIndex < limit; mismatchIndex++ {
		if minimumLeaf.key[mismatchIndex+offset] != key[mismatchIndex+offset] { // 这块之前敲错了 != 敲成 == 了
			break
		}
	}
	return mismatchIndex
}

// findMinimumKey 找到字典序最小的键所对应的叶子节点
func (a *artNode[T]) findMinimumKey() *leaf[T] {
	switch a.kind {
	case Leaf:
		return a.leaf()
	case Node4:
		node := a.node4()
		if node.zeroChild != nil {
			return node.zeroChild.findMinimumKey()
		} else if node.children[0] != nil {
			return node.children[0].findMinimumKey()
		}
	case Node16:
		node := a.node16()
		if node.zeroChild != nil {
			return node.zeroChild.findMinimumKey()
		} else if node.children[0] != nil {
			return node.children[0].findMinimumKey()
		}
	case Node48:
		node := a.node48()
		if node.zeroChild != nil {
			return node.zeroChild.findMinimumKey()
		}
		index := 0
		/*
			在 node48 中 isExist 是一个长度为4个字节的位图 如果有一位为1则说明对应的 children 和 keys 数组内的值有效
			因为这是寻找键字典序最小的叶子节点 所以直接找最小的有效的子节点

			node.isExist[index >> 6] 找到位图的特定字节 index 右移6位相当于除64
			1 <<(index%64) 如果 index 在对应字节的这一位为1 生成对应的掩码 如果 index 为65 则生成第二个字节的第一位为1的掩码 即为00000001
			两个结果按位相与 拿到 index 对应的值 如果是0说明无意义 如果是1说明有子节点
		*/
		for !node.isExist.get(index) {
			index += 1
		}
		if node.children[index] != nil {
			return node.children[index].findMinimumKey()
		}
	case Node256:
		node := a.node256()
		if node.zeroChild != nil {
			return node.zeroChild.findMinimumKey()
		} else {
			index := 0
			for ; node.children[index] == nil; index++ {
				// 找到第一个不是空的子节点
			}
			return node.children[index].findMinimumKey()
		}
	}
	return nil
}

// findMaximumKey 找到字典序最大的键所对应的叶子节点
func (a *artNode[T]) findMaximumKey() *leaf[T] {
	switch a.kind {
	case Leaf:
		return a.leaf()
	case Node4:
		node := a.node4()
		if node.children[node.childrenNum-1] != nil {
			return node.children[node.childrenNum-1].findMaximumKey()
		}
	case Node16:
		node := a.node16()
		if node.children[node.childrenNum-1] != nil {
			return node.children[node.childrenNum-1].findMaximumKey()
		}
	case Node48:
		// 和 findMinimumKey 一样 只不过是反着来
		node := a.node48()
		index := 255
		for !node.isExist.get(index) {
			index -= 1
		}
		if node.children[index] != nil {
			return node.children[index].findMaximumKey()
		}
	case Node256:
		node := a.node256()
		index := 255
		for node.children[index] != nil {
			index -= 1
		}
		return node.children[index].findMaximumKey()
	}
	return nil
}

// index 按给定的字节 寻找其子节点在 children 数组中的位置
func (a *artNode[T]) index(c byte) int {
	switch a.kind {
	case Node4:
		node := a.node4()
		for index := 0; index < int(node.childrenNum); index++ {
			if node.keys[index] == c {
				return index
			}
		}
	case Node16:
		node := a.node16()
		bitfield := uint(0)
		for index := uint(0); index < node16Max; index++ {
			if node.keys[index] == c {
				bitfield |= 1 << index
			}
		}
		mask := (1 << node.childrenNum) - 1
		bitfield &= uint(mask)
		if bitfield != 0 {
			return bits.TrailingZeros(bitfield)
		}
		// todo 存疑
		// 在 for 循环处直接返回 index 也看不出来哪里有问题
	case Node48:
		node := a.node48()
		if node.isExist.get(int(c)) {
			if index := int(node.keys[c]); index >= 0 { // 这块之前敲错了 敲成了 index > 0
				return index
			}
		}
	case Node256:
		return int(c)
	default:
		return -1
	}
	return -1
}

// findChildByChar 按给定的字节 找到对应的子节点 如果 valid 为 false 则认为给定的字节为0
func (a *artNode[T]) findChildByChar(c byte, valid bool) **artNode[T] {
	node := a.node()
	if !valid {
		return &node.zeroChild
	}

	index := a.index(c)
	if index != -1 {
		switch a.kind {
		case Node4:
			return &a.node4().children[index]
		case Node16:
			return &a.node16().children[index]
		case Node48:
			return &a.node48().children[index]
		case Node256:
			return &a.node256().children[index]
		default:
		}
	}
	var nodeNotFound *artNode[T]
	return &nodeNotFound // 没有找到对应子节点 这个写法会引起内存逃逸 一定会带来性能损失 但是没招了 只能这样了
}

// copyMeta 将子节点数量 前缀长度和前缀 从 src 中 copy 出来 返回值和调用者是同一个指针
func (a *artNode[T]) copyMeta(src *artNode[T]) *artNode[T] {
	if src == nil {
		return a
	}

	dst := a.node()
	srcNode := src.node()

	dst.childrenNum = srcNode.childrenNum
	dst.prefixLen = srcNode.prefixLen

	for i, limit := uint32(0), min(MaxPrefixLength, dst.prefixLen); i < limit; i++ {
		dst.prefix[i] = srcNode.prefix[i]
	}

	return a
}

// grow 节点向上转换函数 如果调用者是 node4 返回的就是一个新的 node16 节点 如果没得转换则返回 nil
func (a *artNode[T]) grow() *artNode[T] {
	switch a.kind {
	case Node4:
		newNode := newNode16[T]().copyMeta(a)
		dst := newNode.node16()
		src := a.node4()
		dst.zeroChild = src.zeroChild

		for i := 0; i < int(src.childrenNum); i++ {
			if src.isExist.get(i) {
				dst.keys[i] = src.keys[i]
				dst.isExist.set1(i)
				dst.children[i] = src.children[i]
			}
		}
		return newNode
	case Node16:
		newNode := newNode48[T]().copyMeta(a)
		dst := newNode.node48()
		src := a.node16()
		dst.zeroChild = src.zeroChild

		var newNodeChildIndex byte
		for i := uint16(0); i < src.childrenNum; i++ {
			if src.isExist.get(int(i)) { // src 的 index 对应的子节点有效
				char := src.keys[i]
				dst.keys[char] = newNodeChildIndex
				dst.isExist.set1(int(char)) // dst 的是否有效的位图上进行标记
				dst.children[newNodeChildIndex] = src.children[i]
				newNodeChildIndex += 1
			}
		}

		return newNode
	case Node48:
		newNode := newNode256[T]().copyMeta(a)
		dst := newNode.node256()
		src := a.node48()
		dst.zeroChild = src.zeroChild

		for i := 0; i < node256Max; i++ {
			if src.isExist.get(i) {
				dst.children[i] = src.children[src.keys[i]]
			}
		}

		return newNode
	default:
		return nil
	}
}

// shrink 节点向下转换函数 如果调用者是 node16 返回的就是一个新的 node4 节点 如果没得转换则返回 nil
// 注意 node4 仍然可以向下转换 它会和其唯一的子节点进行合并 并且合并前缀 如果其唯一子节点为叶子节点 会直接返回该叶子节点
func (a *artNode[T]) shrink() *artNode[T] {
	switch a.kind {
	case Node4:
		src := a.node4()
		dst := src.children[0]
		if dst == nil {
			dst = src.zeroChild
		}

		if dst.isLeaf() {
			return dst
		}

		// 开始合并前缀

		// 先是 key 中的那个字节也要加入前缀
		curPrefixLen := src.prefixLen
		if curPrefixLen < MaxPrefixLength {
			src.prefix[curPrefixLen] = src.keys[0]
			curPrefixLen += 1
		}

		// 将 dst 的前缀追加到 src 中
		dstNode := dst.node()
		if curPrefixLen < MaxPrefixLength {
			childPrefixLen := min(dstNode.prefixLen, MaxPrefixLength-curPrefixLen)
			for i := uint32(0); i < childPrefixLen; i++ {
				src.prefix[curPrefixLen+i] = dstNode.prefix[i]
			}
			curPrefixLen += childPrefixLen
		}

		// 此时 src 中的前缀就是最终要写入 dst 的
		for i := uint32(0); i < min(MaxPrefixLength, curPrefixLen); i++ {
			dstNode.prefix[i] = src.prefix[i]
		}
		// 因为是和唯一的子节点进行合并 所以前缀长度直接加即可 那个+1指的是 src 中的 key 中的那个字节
		dstNode.prefixLen += src.prefixLen + 1

		return dst
	case Node16:
		newNode := newNode4[T]().copyMeta(a)
		dst := newNode.node4()
		src := a.node16()
		dst.zeroChild = src.zeroChild
		dst.childrenNum = 0 // 重新开始计子节点的数量

		for i := 0; i < node4Max; i++ {
			dst.keys[i] = src.keys[i]
			if src.isExist.get(i) {
				dst.isExist.set1(i)
				dst.childrenNum += 1
			}
			dst.children[i] = src.children[i]
		}

		return newNode
	case Node48:
		newNode := newNode16[T]().copyMeta(a)
		dst := newNode.node16()
		src := a.node48()
		dst.zeroChild = src.zeroChild
		dst.childrenNum = 0 // 重新开始计子节点的数量

		for i, index := range src.keys {
			if !src.isExist.get(i) {
				continue
			}

			if child := src.children[index]; child != nil {
				dst.children[dst.childrenNum] = child
				dst.keys[dst.childrenNum] = byte(i)
				dst.isExist.set1(int(dst.childrenNum))
				dst.childrenNum += 1
			}
		}

		return newNode
	case Node256:
		newNode := newNode48[T]().copyMeta(a)
		dst := newNode.node48()
		src := a.node256()
		dst.zeroChild = src.zeroChild
		dst.childrenNum = 0 // 重新开始计子节点的数量

		for index, childNode := range src.children {
			if childNode != nil {
				dst.children[dst.childrenNum] = childNode
				dst.keys[byte(index)] = byte(dst.childrenNum)
				dst.isExist.set1(index)
				dst.childrenNum += 1
			}
		}

		return newNode
	default:
		return nil
	}
}

// 添加子节点函数

// addChild4 为 node4 类型提供的 添加子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为添加子节点的过程中节点是否向上转换过
func (a *artNode[T]) addChild4(c byte, isValid bool, child *artNode[T]) bool {
	parentNode := a.node4()

	// 是否需要向上转换
	if parentNode.childrenNum >= node4Max {
		newNode := a.grow()
		newNode.addChild(c, isValid, child)
		replaceNode[T](a, newNode)
		return true
	}

	// 给定字节是否为0
	if !isValid {
		parentNode.zeroChild = child
		return false
	}

	// 找到插入的位置
	i := uint16(0)
	for ; i < parentNode.childrenNum; i++ {
		if c < parentNode.keys[i] {
			break
		}
	}

	// 该位置之后的内容全部后移 保持存储的字典序
	limit := parentNode.childrenNum - i
	for j := limit; limit > 0 && j > 0; j-- {
		parentNode.keys[i+j] = parentNode.keys[i+j-1]
		if parentNode.isExist.get(int(i + j - 1)) {
			parentNode.isExist.set1(int(i + j))
		} else {
			parentNode.isExist.set0(int(i + j))
		}
		parentNode.children[i+j] = parentNode.children[i+j-1]
	}
	parentNode.keys[i] = c
	parentNode.children[i] = child
	parentNode.isExist.set1(int(i))
	parentNode.childrenNum += 1
	return false
}

// addChild16 为 node16 类型提供的 添加子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为添加子节点的过程中节点是否向上转换过
func (a *artNode[T]) addChild16(c byte, isValid bool, child *artNode[T]) bool {
	parentNode := a.node16()

	if parentNode.childrenNum >= node16Max {
		newNode := a.grow()
		newNode.addChild(c, isValid, child)
		replaceNode[T](a, newNode)
		return true
	}

	if !isValid {
		parentNode.zeroChild = child
		return false
	}

	index := parentNode.childrenNum
	bitfield := uint(0)
	for i := uint16(0); i < node16Max; i++ {
		if parentNode.keys[i] > c {
			bitfield |= 1 << i
		}
	}
	mask := (1 << parentNode.childrenNum) - 1
	bitfield &= uint(mask)
	if bitfield != 0 {
		index = uint16(bits.TrailingZeros(bitfield))
	}

	for i := parentNode.childrenNum; i > index; i-- {
		parentNode.keys[i] = parentNode.keys[i-1]
		parentNode.children[i] = parentNode.children[i-1]
		//fmt.Println(fmt.Sprintf("parm: %d", c))
		//fmt.Println(fmt.Sprintf("log1: %b", parentNode.isExist)) // 1111
		//fmt.Println(fmt.Sprintf("log2: %b", parentNode.isExist &^ (1 << i))) // 1111
		//fmt.Println(fmt.Sprintf("log3: %b", parentNode.isExist & (1 << (i - 1)))) // 1000
		//fmt.Println(fmt.Sprintf("log4: %b", (parentNode.isExist & (1 << (i - 1))) << 1)) // 0001 0000
		//fmt.Println(fmt.Sprintf("log5: %b", parentNode.isExist &^ (1 << i) | ((parentNode.isExist & (1 << (i - 1))) << 1))) // 0001 1111
		//parentNode.isExist = parentNode.isExist &^ (1 << i) | ((parentNode.isExist & (1 << (i - 1))) << 1)

		if parentNode.isExist.get(int(i - 1)) {
			parentNode.isExist.set1(int(i))
		} else {
			parentNode.isExist.set0(int(i))
		}
		//parentNode.isExist.set0(int(i - 1))
		//parentNode.isExist.set1(int(i))
		//parentNode.isExist = parentNode.isExist &^ (1 << i - 1)
		//parentNode.isExist |= 1 << i
		/*
			parentNode.isExist 类型为 uint16
			假设 parentNode.isExist 原本是 0000 0000 0000 1111 i = 4 期望得到 0000 0000 0001 1111

			&^ 这是置零运算符 a &^ b a 会将 b 中所有为 1 的位置所对应的自己数值全部置零
			a &^ b = (a ^ b) & a
			第一步 parentNode.isExist &^ (1 << i) 即为 0000 0000 0000 1111 和 0000 0000 0001 0000 进行置零 得到 0000 0000 0000 1111

			第二步 parentNode.isExist & (1 << (i - 1)) 即为 0000 0000 0000 1111 和 0000 0000 0000 1000 相与 得到 0000 0000 0000 1000
			第三步 ((parentNode.isExist & (1 << (i - 1))) << 1) 即为 0000 0000 0000 1000 左移一位 得到 0000 0000 0001 0000
			最后 parentNode.isExist & ^(1 << i) | ((parentNode.isExist & (1 << (i - 1))) << 1)
			即为 0000 0000 0000 1111 和 0000 0000 0001 0000 相或 得到 0000 0000 0001 1111

			还是有问题 如果期望是全部置1 下面的 parentNode.isExist |= 1 << index 是干啥的?
		*/
	}

	parentNode.keys[index] = c
	parentNode.children[index] = child
	parentNode.isExist.set1(int(index))
	parentNode.childrenNum += 1
	return false
}

// addChild48 为 node48 类型提供的 添加子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为添加子节点的过程中节点是否向上转换过
func (a *artNode[T]) addChild48(c byte, isValid bool, child *artNode[T]) bool {
	parentNode := a.node48()
	if parentNode.childrenNum >= node48Max {
		newNode := a.grow()
		newNode.addChild(c, isValid, child)
		replaceNode[T](a, newNode)
		return true
	}

	if !isValid {
		parentNode.zeroChild = child
		return false
	}

	index := byte(0)
	for parentNode.children[index] != nil {
		index += 1
	}

	parentNode.keys[c] = index
	parentNode.isExist.set1(int(c))
	//parentNode.isExist[c >> 6] |= (1 << (c % 64))
	parentNode.children[index] = child
	parentNode.childrenNum += 1
	return false
}

// addChild256 为 node256 类型提供的 添加子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为添加子节点的过程中节点是否向上转换过 这里默认不会再向上转换 不过字节也就256个 不会存不下
func (a *artNode[T]) addChild256(c byte, isValid bool, child *artNode[T]) bool {
	parentNode := a.node256()
	if !isValid {
		parentNode.zeroChild = child
	} else {
		parentNode.children[c] = child
		parentNode.childrenNum += 1
	}
	return false
}

// addChild 添加子节点 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为添加子节点的过程中节点是否向上转换过
func (a *artNode[T]) addChild(c byte, isValid bool, child *artNode[T]) bool {
	switch a.kind {
	case Node4:
		return a.addChild4(c, isValid, child)
	case Node16:
		return a.addChild16(c, isValid, child)
	case Node48:
		return a.addChild48(c, isValid, child)
	case Node256:
		return a.addChild256(c, isValid, child)
	default:
		return false
	}
}

// 删除子节点函数

// removeChild4 为 node4 类型提供的 删除子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为该节点下的 childrenNum 字段
//
// 注意 仅对于 node4 类型 返回值会根据 zeroChild 字段是否为空来决定是否+1
//
// 这一区别的原因在于 对于 shrink 函数而言 node4 类型只会和唯一的子节点进行合并
// 别的类型在向下转换的时候 子节点数量不会少到只有一个 zeroChild 字段直接 copy 过去就好
//
// 而对于 node4 类型来说 childNum 字段为1时有可能 zeroChild 字段为空 该节点下确实只有一个子节点
// 也有可能是 zeroChild 字段有一个子节点 加起来一共俩子节点
//
// 有俩子节点的时候调用 shrink 必然丢失那个在 children 数组里的子节点 所以 zeroChild 字段必须纳入统计
func (a *artNode[T]) removeChild4(c byte, isValid bool) uint16 {
	parentNode := a.node4()
	if !isValid {
		parentNode.zeroChild = nil
	} else if index := a.index(c); index >= 0 { // 找对应下标
		// 删除
		parentNode.childrenNum -= 1

		parentNode.keys[index] = 0
		parentNode.children[index] = nil
		parentNode.isExist.set0(index)
		//parentNode.isExist[index] = 0

		// 子节点和对应的值前移 维护子节点的字典序
		for i := uint16(index); i <= parentNode.childrenNum && i+1 < node4Max; i++ {
			parentNode.keys[i] = parentNode.keys[i+1]
			parentNode.children[i] = parentNode.children[i+1]
			parentNode.isExist.set1(int(i))
			//parentNode.isExist[i] = parentNode.isExist[i + 1]
		}

		parentNode.keys[parentNode.childrenNum] = 0
		parentNode.children[parentNode.childrenNum] = nil
		parentNode.isExist.set0(int(parentNode.childrenNum))
		//parentNode.isExist[parentNode.childrenNum] = 0
	}
	numChildren := parentNode.childrenNum
	if parentNode.zeroChild != nil {
		numChildren += 1
	}
	return numChildren
}

// removeChild16 为 node16 类型提供的 删除子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为该节点下的 childrenNum 字段
func (a *artNode[T]) removeChild16(c byte, isValid bool) uint16 {
	parentNode := a.node16()
	if !isValid {
		parentNode.zeroChild = nil
	} else if index := a.index(c); index >= 0 {
		parentNode.childrenNum -= 1

		parentNode.keys[index] = 0
		parentNode.children[index] = nil
		parentNode.isExist.set0(index)
		//parentNode.isExist = parentNode.isExist &^ 1 << index

		for i := uint16(index); i <= parentNode.childrenNum && i+1 < node16Max; i++ {
			parentNode.keys[i] = parentNode.keys[i+1]
			//parentNode.isExist = parentNode.isExist &^ (1 << i + 1) // i + 1 位置置零
			//parentNode.isExist |= i << i                            // i 位置置一

			// 下面这是原来的代码 我自己写一版试一下
			//parentNode.isExist = parentNode.isExist &^ (1 << i) | (parentNode.isExist & (1 << (i + 1))) >> 1
			// 举例 1011 -> 0111 i = 2
			//                           1011            0100                 1011          1000
			//                                    1011                               1000
			//                                    1011                                              0100
			//                                                     1111
			// 这里也有问题 该消除的反而没有消掉
			parentNode.children[i] = parentNode.children[i+1]
			parentNode.isExist.set1(int(i))
		}

		parentNode.keys[parentNode.childrenNum] = 0
		parentNode.children[parentNode.childrenNum] = nil
		parentNode.isExist.set0(int(parentNode.childrenNum))
		//parentNode.isExist = parentNode.isExist &^ (1 << parentNode.childrenNum)
	}

	return parentNode.childrenNum
}

// removeChild48 为 node48 类型提供的 删除子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为该节点下的 childrenNum 字段
func (a *artNode[T]) removeChild48(c byte, isValid bool) uint16 {
	parentNode := a.node48()
	if !isValid {
		parentNode.zeroChild = nil
	} else if index := a.index(c); index >= 0 && parentNode.children[index] != nil {
		parentNode.children[index] = nil
		parentNode.keys[c] = 0
		parentNode.isExist.set0(int(c))
		//parentNode.isExist[index >> 6] = parentNode.isExist[index >> 6] &^ (1 << (c % 64))
		parentNode.childrenNum -= 1
	}
	return parentNode.childrenNum
}

// removeChild256 为 node256 类型提供的 删除子节点的函数 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为该节点下的 childrenNum 字段
func (a *artNode[T]) removeChild256(c byte, isValid bool) uint16 {
	parentNode := a.node256()
	if !isValid {
		parentNode.zeroChild = nil
	} else if index := a.index(c); parentNode.children[index] != nil {
		parentNode.children[index] = nil
		parentNode.childrenNum -= 1
	}
	return parentNode.childrenNum
}

// removeChild 删除子节点 如果 valid 为 false 则认为给定的字节为0
// 返回值的含义为删除子节点的过程中节点是否向下转换过
func (a *artNode[T]) removeChild(c byte, isValid bool) bool {
	var numChildren uint16
	var minChildren uint16

	deleted := false
	switch a.kind {
	case Node4:
		numChildren = a.removeChild4(c, isValid)
		minChildren = node4Min
		deleted = true
	case Node16:
		numChildren = a.removeChild16(c, isValid)
		minChildren = node16Min
		deleted = true
	case Node48:
		numChildren = a.removeChild48(c, isValid)
		minChildren = node48Min
		deleted = true
	case Node256:
		numChildren = a.removeChild256(c, isValid)
		minChildren = node256Min
		deleted = true
	default:
		return false
	}
	//_ = fmt.Sprintf("%d, %d, %t", numChildren, minChildren, deleted)
	if deleted && numChildren < minChildren {
		newNode := a.shrink()
		replaceNode(a, newNode)
		return true
	}

	return false
}

// 叶子节点函数

// allMatchKey 仅对于叶子节点 比较其存储的键和给定的键是否相同
func (l *leaf[T]) allMatchKey(key Key) bool {
	if key == nil || len(key) != len(l.key) {
		return false
	}
	return bytes.Compare(l.key[:len(key)], key) == 0
}

// allMatchKey 仅对于叶子节点 检查给定的键是否为存储的键的前缀
func (l *leaf[T]) prefixMatchKey(key Key) bool {
	if key == nil || len(l.key) < len(key) {
		return false
	}
	return bytes.Compare(l.key[:len(key)], key) == 0
}
