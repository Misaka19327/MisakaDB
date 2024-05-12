package file

// FileWriter 定义封装文件操作的基本行为 因为bitcask模型只有对文件的读 追加和整理 所以这里并不涉及对文件的已经写入的数据进行修改
type FileWriter interface {
	Write(input []byte, offset int) error // 在文件的指定位置进行写入
	Read(buf []byte, offset int) error    // 在文件的指定位置进行读取
	Sync() error                          // 强制内存和文件同步一次 以保证一致性
	Delete() error                        // 删除文件
	Close() error                         // 关闭文件
	Length() (int64, error)               // 返回该文件的长度
}

// attention 实现FileWriter接口的结构体不需要并发安全 因为它和index一一对应 而index会带一个读写锁
