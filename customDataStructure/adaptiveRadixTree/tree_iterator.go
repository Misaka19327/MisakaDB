package adaptiveRadixTree

// iteratorLevel 记录迭代器路径的
type iteratorLevel[T Value] struct {
	node       *artNode[T]
	childIndex int
}

// iterator 迭代器
type iterator[T Value] struct {
	version int // 用于和树的 version 进行比较 从而确定树的结构是否有变动

	tree       *tree[T]
	nextNode   *artNode[T]
	depthLevel int
	path       []*iteratorLevel[T]
}

// iteratorWithOption 带有选项的迭代器
type iteratorWithOption[T Value] struct {
	options  int
	nextNode Node[T]
	e        error
	iterator *iterator[T]
}

// traverseOption 将传入的选项们进行合并 如果未指定选项 则默认为 TraverseLeaf
func traverseOption(options ...int) int {
	option := 0
	for i := range options {
		option |= options[i]
	}
	option &= TraverseAll
	if option == 0 {
		return TraverseLeaf
	}
	return option
}

// modifyCallbackFunc 根据选项对 callback 函数进行修饰
func modifyCallbackFunc[T Value](option int, callback Callback[T]) Callback[T] {
	if option == TraverseAll {
		return callback
	}

	return func(node Node[T]) (isContinue bool) {
		if option&TraverseLeaf == TraverseLeaf && node.Kind() == Leaf {
			return callback(node)
		} else if option&TraverseNode == TraverseNode && node.Kind() != Leaf {
			return callback(node)
		}

		return true
	}
}

// ForEach 对树进行遍历 对于每个被遍历到的 符合条件的节点调用 callback 函数 默认情况下遍历只遍历叶子节点
func (t *tree[T]) ForEach(callback Callback[T], options ...int) {
	option := traverseOption(options...)
	t.recursiveForEach(t.root, modifyCallbackFunc[T](option, callback))
}

// recursiveForEach 以递归的方式进行遍历
func (t *tree[T]) recursiveForEach(currentNode *artNode[T], callback Callback[T]) traverseAction {
	if currentNode == nil {
		return Continue
	}

	if !callback(currentNode) {
		return Stop
	}

	switch currentNode.kind {
	case Leaf:
	case Node4:
		return t.forEachChildNode(currentNode.node().zeroChild, currentNode.node4().children[:], callback)
	case Node16:
		return t.forEachChildNode(currentNode.node().zeroChild, currentNode.node16().children[:], callback)
	case Node48:
		node := currentNode.node48()
		if node.zeroChild != nil {
			if t.recursiveForEach(node.zeroChild, callback) == Stop {
				return Stop
			}
		}

		for char, index := range node.keys {
			if !node.isExist.get(char) {
				continue
			}

			child := node.children[index]
			if child != nil {
				if t.recursiveForEach(child, callback) == Stop {
					return Stop
				}
			}
		}
	case Node256:
		return t.forEachChildNode(currentNode.node().zeroChild, currentNode.node256().children[:], callback)
	}

	return Continue
}

// forEachChildNode 对给定的子节点们分别调用 recursiveForEach
func (t *tree[T]) forEachChildNode(nullChild *artNode[T], children []*artNode[T], callback Callback[T]) traverseAction {
	if nullChild != nil {
		if t.recursiveForEach(nullChild, callback) == Stop {
			return Stop
		}
	}

	for _, child := range children {
		if child != nil && child != nullChild {
			if t.recursiveForEach(child, callback) == Stop {
				return Stop
			}
		}
	}

	return Continue
}

// ForEachWithPrefix 对树进行遍历 对于每个被遍历到的 前缀和指定的键匹配的 符合条件的节点调用 callback 函数 默认情况下遍历只遍历叶子节点
func (t *tree[T]) ForEachWithPrefix(keyPrefix Key, callback Callback[T], options ...int) {
	option := traverseOption(options...)
	t.forEachWithPrefix(t.root, keyPrefix, modifyCallbackFunc[T](option, callback))
}

// forEachWithPrefix 对 ForEachWithPrefix 的逻辑的封装 采用循环 + 递归的方式进行遍历
//
// 它会以循环先找到能够和指定的键完全匹配的前缀所对应的节点 之后对该节点以递归的方式遍历它的所有子节点
func (t *tree[T]) forEachWithPrefix(current *artNode[T], keyPrefix Key, callback Callback[T]) traverseAction {
	if current == nil {
		return Continue
	}

	depth := uint32(0)
	for current != nil {
		if current.isLeaf() {
			currentLeaf := current.leaf()
			if currentLeaf.prefixMatchKey(keyPrefix) {
				if !callback(current) {
					return Stop
				}
			}
			break
		}

		if depth == uint32(len(keyPrefix)) { // 递归已经匹配到尽头
			minLeaf := current.findMinimumKey()
			if minLeaf.prefixMatchKey(keyPrefix) {
				if t.recursiveForEach(current, callback) == Stop {
					return Stop
				}
			}
			break
		}

		currentNode := current.node()
		if currentNode.prefixLen > 0 {
			prefixLen := current.matchDeep(keyPrefix, depth)
			if prefixLen > currentNode.prefixLen {
				prefixLen = currentNode.prefixLen
			}

			if prefixLen == 0 { // 没有匹配前缀
				break
			} else if depth+prefixLen == uint32(len(keyPrefix)) { // 给定的前缀刚好在当前节点匹配完
				return t.recursiveForEach(current, callback)
			}

			depth += currentNode.prefixLen
		}

		next := current.findChildByChar(keyPrefix.getByteByIndex(int(depth)), keyPrefix.indexIsValid(int(depth)))
		if *next == nil {
			break
		}
		current = *next
		depth += 1
	}

	return Continue
}

const nullIndex = -1

// Iterator 获取一个新的迭代器 默认只遍历叶子节点
func (t *tree[T]) Iterator(options ...int) Iterator[T] {
	option := traverseOption(options...)

	i := &iterator[T]{
		version:    t.version,
		tree:       t,
		nextNode:   t.root,
		depthLevel: 0,
		path:       []*iteratorLevel[T]{{t.root, nullIndex}},
	}

	if option&TraverseAll == TraverseAll {
		return i
	}

	bufferI := &iteratorWithOption[T]{
		options:  option,
		iterator: i,
	}

	return bufferI
}

// HasNext 是否存在下一个节点 对于当前迭代器来说 具体的搜索行为发生在 Next 中
func (i *iterator[T]) HasNext() bool {
	return i.tree != nil && i.nextNode != nil
}

// Next 获取下一个节点 如果下一个节点不存在或者树结构被修改 就返回 NoMoreNodeErr 或者 TreeIsModifiedErr
//
// 对于当前迭代器来说 具体的搜索行为发生在 Next 中
func (i *iterator[T]) Next() (Node[T], error) {
	if !i.HasNext() {
		return nil, NoMoreNodeErr
	}

	e := i.isTreeModified()
	if e != nil {
		return nil, e
	}

	current := i.nextNode
	i.next()

	return current, nil
}

// isTreeModified 检查树结构是否被修改
func (i *iterator[T]) isTreeModified() error {
	if i.version == i.tree.version {
		return nil
	}
	return TreeIsModifiedErr
}

// next 根据当前迭代器的情况 获取下一个节点并且存入迭代器
func (i *iterator[T]) next() {
	for {
		var nextNode *artNode[T]
		nextNodeIndex := nullIndex

		currentNode := i.path[i.depthLevel].node
		currentChildIndex := i.path[i.depthLevel].childIndex

		switch currentNode.kind {
		case Leaf:
		case Node4:
			nextNodeIndex, nextNode = nextChild[T](currentChildIndex, currentNode.node().zeroChild, currentNode.node4().children[:])
		case Node16:
			nextNodeIndex, nextNode = nextChild[T](currentChildIndex, currentNode.node().zeroChild, currentNode.node16().children[:])
		case Node48:
			currentNode48 := currentNode.node48()
			nullChild := currentNode48.zeroChild

			if currentChildIndex == nullIndex {
				if nullChild == nil {
					currentChildIndex = 0
				} else {
					nextNodeIndex = 0
					nextNode = nullChild
					break
				}
			}

			for j := currentChildIndex; j < len(currentNode48.keys); j++ { // 这块写错了 应该是 currentNode48.keys 写成 currentNode48.children
				if !currentNode48.isExist.get(j) {
					continue
				}

				child := currentNode48.children[currentNode48.keys[j]]
				if child != nil && child != nullChild {
					nextNodeIndex = j + 1
					nextNode = child
					break
				}
			}
		case Node256:
			nextNodeIndex, nextNode = nextChild[T](currentChildIndex, currentNode.node().zeroChild, currentNode.node256().children[:])
		}

		if nextNode == nil { // 在当前节点没搜索到
			// 尝试向上回退一层再搜索
			if i.depthLevel > 0 {
				i.depthLevel -= 1
			} else {
				// 没得回退 结束
				i.nextNode = nil
				return
			}
		} else {
			i.path[i.depthLevel].childIndex = nextNodeIndex // 更新上层节点到当前层节点的路径(即将要搜索的子节点的索引)
			i.nextNode = nextNode

			// 这个重新声明的切片 是为了下面 i.path 能够直接通过索引赋值
			if i.depthLevel+1 >= cap(i.path) {
				newPath := make([]*iteratorLevel[T], i.depthLevel+2)
				copy(newPath, i.path)
				i.path = newPath
			}

			i.depthLevel += 1 // 到下一层
			// 这里之所以是直接赋值而不是 append 是因为要考虑树的同层节点
			// 注意考虑的树的同层节点而不是同层子节点
			i.path[i.depthLevel] = &iteratorLevel[T]{
				node:       nextNode,
				childIndex: nullIndex, // 一个节点从 nullChild 开始搜索
			}
			return
		}
	}
}

// nextChild 用 childIndex 确定给定的子节点们是否还存在未搜索到的子节点
func nextChild[T Value](childIndex int, nullChild *artNode[T], children []*artNode[T]) (nextChildIndex int, nextNode *artNode[T]) {
	if childIndex == nullIndex {
		if nullChild != nil {
			return 0, nullChild
		}

		childIndex = 0
	}

	for i := childIndex; i < len(children); i++ {
		child := children[i]
		if child != nil && child != nullChild {
			return i + 1, child
		}
	}

	return 0, nil
}

// HasNext 是否存在下一个节点 对于当前迭代器来说 具体的搜索行为发生在 HasNext 中
func (bi *iteratorWithOption[T]) HasNext() bool {
	for bi.iterator.HasNext() {
		bi.nextNode, bi.e = bi.iterator.Next()
		if bi.e != nil {
			return true // 这里只有直接返回 true 才能让用户通过 Next 拿到错误
		}
		if bi.options&TraverseLeaf == TraverseLeaf && bi.nextNode.Kind() == Leaf {
			return true
		} else if bi.options&TraverseNode == TraverseNode && bi.nextNode.Kind() != Leaf {
			return true
		}
	}

	bi.nextNode = nil
	bi.e = nil
	return false
}

// Next 获取下一个节点 如果下一个节点不存在或者树结构被修改 就返回 NoMoreNodeErr 或者 TreeIsModifiedErr
//
// 对于当前迭代器来说 具体的搜索行为发生在 HasNext 中
func (bi *iteratorWithOption[T]) Next() (Node[T], error) {
	return bi.nextNode, bi.e
}
