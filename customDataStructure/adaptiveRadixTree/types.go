package adaptiveRadixTree

import "errors"

type Key []byte

type Value interface {
	any

	String() string
}

type Kind int

type prefix [MaxPrefixLength]byte

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

type Iterator[T Value] interface {
	HasNext() bool
	Next() (node Node[T], e error)
}

type Tree[T Value] interface {
	Insert(key Key, value T) (oldValue T, isUpdated bool)
	Delete(key Key) (deleteValue T, isDeleted bool)
	Search(key Key) (value T, isFound bool)

	ForEach(callback Callback[T], options ...int)
	ForEachWithPrefix(keyPrefix Key, callback Callback[T], options ...int)
	Iterator(options ...int) Iterator[T]

	Minimum() (min T, isFound bool)
	Maximum() (max T, isFound bool)

	Size() int
}

var (
	NoMoreNodeErr     = errors.New("Tree Has No More Node! ")
	TreeIsModifiedErr = errors.New("Tree is Modified! ")
)
