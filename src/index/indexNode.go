package index

// IndexNode 所有索引具体存储的节点 内部存储了文件ID 偏移值和过期时间戳
type IndexNode struct {
	value     []byte
	fileID    uint32
	offset    int64
	expiredAt int64
}
