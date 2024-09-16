package adaptiveRadixTree

import (
	"unsafe"
)

func newNode4[T Value]() *artNode[T] {
	return &artNode[T]{
		reference: unsafe.Pointer(&node4[T]{
			isExist: newBitmap(4),
		}),
		kind: Node4,
	}
}

func newNode16[T Value]() *artNode[T] {
	return &artNode[T]{
		reference: unsafe.Pointer(&node16[T]{
			isExist: newBitmap(16),
		}),
		kind: Node16,
	}
}

func newNode48[T Value]() *artNode[T] {
	return &artNode[T]{
		reference: unsafe.Pointer(&node48[T]{
			isExist: newBitmap(256),
		}),
		kind: Node48,
	}
}

func newNode256[T Value]() *artNode[T] {
	return &artNode[T]{
		reference: unsafe.Pointer(new(node256[T])),
		kind:      Node256,
	}
}

func newLeaf[T Value](key Key, value T) *artNode[T] {
	clonedKey := make(Key, len(key))
	copy(clonedKey, key)
	return &artNode[T]{
		reference: unsafe.Pointer(&leaf[T]{
			key:   clonedKey,
			value: value,
		}),
		kind: Leaf,
	}
}
