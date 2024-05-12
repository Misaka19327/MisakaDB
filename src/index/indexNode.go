package index

// IndexNode 所有索引具体存储的节点 内部存储了文件ID 偏移值和过期时间戳
type IndexNode struct {
	fileID    uint32
	offset    int64
	expiredAt int64
}

// attention 我犹豫了一下内存中要不要存入值 后来决定不存值了 如果内存存值的话这和缓存有啥区别
// 反正改的话也好改 因为所有初始化IndexNode的位置必定会有对应value
