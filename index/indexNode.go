package index

// IndexNode 所有索引具体存储的节点 内部存储了文件ID 偏移值和过期时间戳
type IndexNode struct {
	value     []byte
	fileID    uint32
	offset    int64
	expiredAt int64
}

// assertStringValue 断言传入的值为*IndexNode 自定义数据结构内存储的值类型为any 需要这个方法来转换一次
func assertIndexNodePointer(value any) *IndexNode {
	return value.(*IndexNode)
}
