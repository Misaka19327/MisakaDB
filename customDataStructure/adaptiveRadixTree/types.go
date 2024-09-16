package adaptiveRadixTree

import (
	"errors"
)

// Key 键的类型 其底层为 []byte 类型
type Key []byte

// Value 值的泛型 存入树的值键值对的值必须符合该泛型所定义的类型
type Value interface {
	any

	// String 将 Value 转换为 String 的方法 如果不需要打印树 直接写一个空函数即可
	String() string
}

type Kind int

type prefix [MaxPrefixLength]byte

// Callback 在对树进行遍历时 每个节点都会传入该函数类型的实际函数 并且根据返回值决定是否继续遍历
type Callback[T Value] func(node Node[T]) (isContinue bool)

const (
	Leaf Kind = iota
	Node4
	Node16
	Node48
	Node256
)

type traverseAction int

const (
	Stop traverseAction = iota
	Continue
)

const (
	TraverseLeaf = 1
	TraverseNode = 2
	TraverseAll  = TraverseLeaf | TraverseNode
)

type Node[T Value] interface {
	Kind() Kind
	Key() Key
	Value() T
}

// Iterator 迭代器实例
type Iterator[T Value] interface {
	// HasNext 是否存在下一个节点
	HasNext() bool
	// Next 获取下一个节点 如果下一个节点不存在或者树结构被修改 就返回 NoMoreNodeErr 或者 TreeIsModifiedErr
	Next() (node Node[T], e error)
}

// Tree 树的接口
type Tree[T Value] interface {
	// Insert 根据指定的键插入新值 如果键在树中已经有值 就更新值 返回旧的值 并且返回 true
	Insert(key Key, value T) (oldValue T, isUpdated bool)
	// Delete 根据指定的键删除对应的值 并且返回被删除的旧值 如果键不存在将返回 false
	Delete(key Key) (deleteValue T, isDeleted bool)
	// Search 根据指定的键寻找对应的值
	Search(key Key) (value T, isFound bool)

	// ForEach 对树进行遍历 对于每个被遍历到的 符合条件的节点调用 callback 函数 默认情况下遍历只遍历叶子节点
	ForEach(callback Callback[T], options ...int)
	// ForEachWithPrefix 对树进行遍历 对于每个被遍历到的 前缀和指定的键匹配的 符合条件的节点调用 callback 函数 默认情况下遍历只遍历叶子节点
	ForEachWithPrefix(keyPrefix Key, callback Callback[T], options ...int)
	// Iterator 获取一个新的迭代器 默认只遍历叶子节点
	Iterator(options ...int) Iterator[T]

	// Minimum 获取当前树上字典序最小的键所对应的值
	Minimum() (min T, isFound bool)
	// Maximum 获取当前树上字典序最大的键所对应的值
	Maximum() (max T, isFound bool)

	// Size 获取树上键值对数量
	Size() int
}

var (
	NoMoreNodeErr     = errors.New("Tree Has No More Node! ")
	TreeIsModifiedErr = errors.New("Tree is Modified! ")
)
