package adaptiveRadixTree

type tree[T Value] struct {
	version int // 给迭代器用的 记录树经过多少次修改
	// 准确来说是树的结构经过多少次修改 因为更新值并不会使 version + 1

	root *artNode[T]
	size int
}

var _ Tree[Kind] = &tree[Kind]{}

// New 获取一棵新的树
func New[T Value]() Tree[T] {
	return newTree[T]()
}

func newTree[T Value]() *tree[T] {
	return &tree[T]{}
}

// Insert 根据指定的键插入新值 如果键在树中已经有值 就更新值 返回旧的值 并且返回 true
func (t *tree[T]) Insert(key Key, value T) (oldValue T, isUpdated bool) {
	if len(key) == 0 {
		isUpdated = false
		return // 底层类型为 any 的类型不支持 nil 也不支持 0 可以通过具名返回值这种方式来让编译器自动给它一个零值
	}
	oldValue, isUpdated = t.recursiveInsert(&t.root, key, value, 0)
	if !isUpdated {
		t.version += 1
		t.size += 1
	}
	return
}

// Delete 根据指定的键删除对应的值 并且返回被删除的旧值 如果键不存在将返回 false
func (t *tree[T]) Delete(key Key) (deleteValue T, isDeleted bool) {
	if t == nil || len(key) == 0 {
		isDeleted = false
		return
	}

	deleteValue, isDeleted = t.recursiveDelete(&t.root, key, 0)
	if isDeleted {
		t.version += 1
		t.size -= 1
	}
	return
}

// Search 根据指定的键寻找对应的值
func (t *tree[T]) Search(key Key) (value T, isFound bool) {
	current := t.root
	depth := uint32(0)
	for current != nil {
		if current.isLeaf() {
			currentLeaf := current.leaf()
			if currentLeaf.allMatchKey(key) {
				return currentLeaf.value, true
			}
			isFound = false
			return
		}

		currentNode := current.node()

		if currentNode.prefixLen > 0 {
			prefixLen := current.match(key, depth)
			if prefixLen != min(currentNode.prefixLen, MaxPrefixLength) {
				isFound = false
				return
			}
			depth += currentNode.prefixLen
		}

		next := current.findChildByChar(key.getByteByIndex(int(depth)), key.indexIsValid(int(depth)))
		if *next != nil {
			current = *next
		} else {
			current = nil
		}
		depth += 1
	}

	isFound = false
	return
}

// Minimum 获取当前树上字典序最小的键所对应的值
func (t *tree[T]) Minimum() (min T, isFound bool) {
	if t == nil || t.root == nil {
		isFound = false
		return
	}
	leafNode := t.root.findMinimumKey()

	return leafNode.value, true
}

// Maximum 获取当前树上字典序最大的键所对应的值
func (t *tree[T]) Maximum() (max T, isFound bool) {
	if t == nil || t.root == nil {
		isFound = false
		return
	}
	leafNode := t.root.findMaximumKey()

	return leafNode.value, true
}

// Size 获取树上键值对数量
func (t *tree[T]) Size() int {
	if t == nil || t.root == nil {
		return 0
	}
	return t.size
}

// recursiveInsert 以递归的方式插入新节点 如果插入的键已经有值 将更新值 返回旧值和 true
func (t *tree[T]) recursiveInsert(currentNode **artNode[T], key Key, value T, depth uint32) (oldValue T, isUpdated bool) {
	// 终止条件
	current := *currentNode
	if current == nil {
		newLeaf := newLeaf[T](key, value)
		replaceRef(currentNode, newLeaf)
		isUpdated = false
		return
	}

	// 如果当前节点是叶子节点 取决于键是否完全匹配决定是否更新值或者分拆叶子节点
	if current.isLeaf() {
		currentLeaf := current.leaf()
		if currentLeaf.allMatchKey(key) { // 键是否已经存在
			oldValue := currentLeaf.value
			currentLeaf.value = value
			return oldValue, true
		}
		// 叶子节点需要分拆
		newLeafNode := newLeaf[T](key, value)
		newLeaf := newLeafNode.leaf()
		leafsLCP := longestCommonPrefix(currentLeaf, newLeaf, depth)

		newNode := newNode4[T]()
		newNode.setPrefix(key[depth:], leafsLCP)
		depth += leafsLCP
		newNode.addChild(currentLeaf.key.getByteByIndex(int(depth)), currentLeaf.key.indexIsValid(int(depth)), current)
		newNode.addChild(newLeaf.key.getByteByIndex(int(depth)), newLeaf.key.indexIsValid(int(depth)), newLeafNode)
		replaceRef(currentNode, newNode)

		isUpdated = false
		return
	}

	curNode := current.node()
	if curNode.prefixLen > 0 {
		prefixMismatchIndex := current.matchDeep(key, depth) // 用字典序最小的叶子节点和当前的键进行比较 找到两者第一次出现不同的索引 以确定是否需要分拆当前节点
		if prefixMismatchIndex >= curNode.prefixLen {
			// 当前节点存储的前缀不包括第一次出现不同的字节 不需要分拆 继续向下找
			depth += curNode.prefixLen
			goto NextNode
		}

		// 第一次出现不同的字节就在当前节点所存储的前缀中 需要分拆
		newNode := newNode4[T]()
		newNode4 := newNode.node4()
		newNode4.prefixLen = prefixMismatchIndex
		for i := 0; i < int(min(MaxPrefixLength, prefixMismatchIndex)); i++ {
			newNode4.prefix[i] = curNode.prefix[i]
		}

		if curNode.prefixLen < MaxPrefixLength {
			curNode.prefixLen -= prefixMismatchIndex + 1
			newNode.addChild(curNode.prefix[prefixMismatchIndex], true, current)

			for i, limit := uint32(0), min(curNode.prefixLen, MaxPrefixLength); i < limit; i++ {
				curNode.prefix[i] = curNode.prefix[i+prefixMismatchIndex+1] // 把共同的前缀给去掉
			}
		} else {
			curNode.prefixLen -= prefixMismatchIndex + 1
			l := current.findMinimumKey()
			newNode.addChild(l.key.getByteByIndex(int(depth+prefixMismatchIndex)), l.key.indexIsValid(int(depth+prefixMismatchIndex)), current)

			for i, limit := uint32(0), min(curNode.prefixLen, MaxPrefixLength); i < limit; i++ {
				curNode.prefix[i] = l.key[depth+prefixMismatchIndex+i+1]
			}
		}

		newNode.addChild(key.getByteByIndex(int(depth+prefixMismatchIndex)), key.indexIsValid(int(depth+prefixMismatchIndex)), newLeaf[T](key, value))
		replaceRef(currentNode, newNode)

		isUpdated = false
		return
	}

NextNode:
	nextNode := current.findChildByChar(key.getByteByIndex(int(depth)), key.indexIsValid(int(depth)))
	if *nextNode != nil {
		return t.recursiveInsert(nextNode, key, value, depth+1)
	}

	current.addChild(key.getByteByIndex(int(depth)), key.indexIsValid(int(depth)), newLeaf[T](key, value))

	isUpdated = false
	return
}

// recursiveDelete 以递归的方式删除 key 所对应的节点 depth 既是树的深度 也是键要开始匹配的范围
func (t *tree[T]) recursiveDelete(curNode **artNode[T], key Key, depth uint32) (deleteValue T, isDeleted bool) {
	if *curNode == nil {
		isDeleted = false
		return
	}

	currentNodePointer := *curNode
	if currentNodePointer.isLeaf() {
		currentLeaf := currentNodePointer.leaf()
		if currentLeaf.allMatchKey(key) { // 找到叶子节点 并且完全匹配
			replaceRef(curNode, nil)
			return currentLeaf.value, true
		}

		isDeleted = false
		return
	}

	currentNode := currentNodePointer.node()
	if currentNode.prefixLen > 0 {
		prefixLen := currentNodePointer.match(key, depth)
		if prefixLen != min(currentNode.prefixLen, MaxPrefixLength) {
			isDeleted = false
			return
		}
		// 前缀匹配成功了 向下接着找子节点
		depth += currentNode.prefixLen
	}

	next := currentNodePointer.findChildByChar(key.getByteByIndex(int(depth)), key.indexIsValid(int(depth)))
	if *next == nil {
		isDeleted = false
		return
	}

	if (*next).isLeaf() {
		nextLeaf := (*next).leaf()
		if nextLeaf.allMatchKey(key) {
			currentNodePointer.removeChild(key.getByteByIndex(int(depth)), key.indexIsValid(int(depth)))
			return nextLeaf.value, true
		}

		isDeleted = false
		return
	}

	return t.recursiveDelete(next, key, depth+1)
}

// getByteByIndex 根据指定索引获取字节数组中的值
func (k Key) getByteByIndex(index int) byte {
	if index < 0 || index >= len(k) {
		return 0
	}
	return k[index]
}

// indexIsValid 判断指定索引在字节数组中是否有效
func (k Key) indexIsValid(index int) bool {
	return index >= 0 && index < len(k)
}
