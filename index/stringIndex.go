package index

import (
	"MisakaDB/customDataStructure/skipList"
	"MisakaDB/logger"
	"MisakaDB/storage"
	"errors"
	"sync"
	"time"
)

type StringIndex struct {
	index        *skipList.SkipList
	mutex        sync.RWMutex
	activeFile   *storage.RecordFile
	archivedFile map[uint32]*storage.RecordFile

	fileIOMode     storage.FileIOType
	baseFolderPath string
	fileMaxSize    int64
	syncDuration   time.Duration
}

// BuildStringIndex 给定当前活跃文件和归档文件 重新构建String类型的索引 该方法只会在数据库启动时被调用 如果不存在旧的文件 则新建一个活跃文件
func BuildStringIndex(activeFile *storage.RecordFile, archivedFile map[uint32]*storage.RecordFile, fileIOMode storage.FileIOType, baseFolderPath string, fileMaxSize int64, syncDuration time.Duration) (*StringIndex, error) {

	result := &StringIndex{
		index:          skipList.NewSkipList(),
		activeFile:     activeFile,
		archivedFile:   archivedFile,
		fileIOMode:     fileIOMode,
		baseFolderPath: baseFolderPath,
		fileMaxSize:    fileMaxSize,
		syncDuration:   syncDuration,
	}

	var (
		e           error
		offset      int64
		fileLength  int64
		entryLength int64
		entry       *storage.Entry
	)

	// 如果活跃文件都读取不到的话 肯定也没有归档文件了 直接返回即可
	if activeFile == nil {
		result.activeFile, e = storage.NewRecordFile(result.fileIOMode, storage.String, 1, result.baseFolderPath, result.fileMaxSize)
		if e != nil {
			return nil, e
		}
		result.archivedFile = make(map[uint32]*storage.RecordFile) // 如果activeFile为空 那么传进来的archivedFile一定也为空 这时再赋值会报错
		result.archivedFile[1] = result.activeFile
		result.activeFile.StartSyncRoutine(syncDuration) // 开始定时同步
		return result, nil
	}

	// 有归档文件的话 挨个读取归档文件 构建索引
	// 读取所有归档文件的entry 因为活跃文件也在这个归档文件里 所以不再单独读取活跃文件
	for i := uint32(1); i <= uint32(len(archivedFile)); i++ {
		recordFile, ok := archivedFile[i]
		if ok == false {
			continue
		}
		offset = 0
		fileLength, e = recordFile.Length()
		if e != nil {
			return nil, e
		}
		for offset < fileLength {
			entry, entryLength, e = recordFile.ReadIntoEntry(offset)
			if e != nil {
				return nil, e
			}
			e = result.handleEntry(entry, recordFile.GetFileID(), offset)
			if e != nil {
				return nil, e
			}
			offset += entryLength
		}
	}
	result.activeFile.StartSyncRoutine(syncDuration)

	return result, nil
}

// CloseIndex 关闭Hash索引 同时停止定时Sync 关闭文件
func (si *StringIndex) CloseIndex() error {
	si.mutex.Lock()
	defer si.mutex.Unlock()
	for _, v := range si.archivedFile {
		if v.IsSyncing {
			v.StopSyncRoutine()
		}
		e := v.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// Set 给定key和value 设定值 如果key存在则为更新值
func (si *StringIndex) Set(key string, value string, expiredAt int64) error {
	entry := &storage.Entry{
		EntryType: storage.TypeRecord,
		ExpiredAt: expiredAt,
		Key:       []byte(key),
		Value:     []byte(value),
	}
	indexNode := &IndexNode{
		expiredAt: expiredAt,
	}

	si.mutex.Lock()
	defer si.mutex.Unlock()

	// 写入文件
	offset, e := si.writeEntry(entry)
	if e != nil {
		return e
	}
	indexNode.offset = offset
	indexNode.fileID = si.activeFile.GetFileID()
	indexNode.value = []byte(value)

	// 写入索引
	e = si.index.AddNode(key, indexNode)
	if e != nil {

	}
	return nil
}

// Get 根据给定的key尝试获取value
func (si *StringIndex) Get(key string) (string, error) {

	si.mutex.RLock()

	value, e := si.index.QueryNode(key)
	if e != nil {
		si.mutex.RUnlock()
		return "", logger.KeyIsNotExisted
	}
	indexNode := assertIndexNodePointer(value)

	// 如果过期时间为-1则说明永不过期
	if indexNode.expiredAt < time.Now().UnixMilli() && indexNode.expiredAt != -1 {
		// todo 时间戳错误的使用了秒为单位 只修了这里和util里的 别的地方没看 记得下次全部修正为毫秒的
		logger.GenerateInfoLog(logger.ValueIsExpired.Error() + key)
		// 读取的Entry过期 删索引
		si.mutex.RUnlock()
		si.mutex.Lock()
		e = si.index.DeleteNode(key)
		si.mutex.Unlock()
		if e != nil {
			logger.GenerateErrorLog(false, false, e.Error(), si.index.ToString(), key)
			return "", e
		}
		return "", logger.ValueIsExpired
	}

	si.mutex.RUnlock()
	return string(indexNode.value), nil
}

// GetRange 返回key中字符串值的子字符
func (si *StringIndex) GetRange(key string, start int, end int) (string, error) {
	if start > end {
		return "", logger.ParameterIsNotAllowed
	}
	value, e := si.Get(key)
	if e != nil {
		return "", e
	}
	return value[start:end], nil
}

// GetSet 先按key获取旧的值 然后再设置新的值并且返回旧值
func (si *StringIndex) GetSet(key string, newValue string) (string, error) {

	si.mutex.RLock()

	// 先尝试Get
	value, e := si.index.QueryNode(key)
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), key)
		return "", e
	}
	indexNode := assertIndexNodePointer(value)

	// 如果过期时间为-1则说明永不过期
	if indexNode.expiredAt < time.Now().Unix() && indexNode.expiredAt != -1 {
		logger.GenerateInfoLog(logger.ValueIsExpired.Error() + key)
		// 读取的Entry过期 删索引
		si.mutex.RUnlock()
		si.mutex.Lock()
		e = si.index.DeleteNode(key)
		si.mutex.Unlock()
		if e != nil {
			logger.GenerateErrorLog(false, false, e.Error(), si.index.ToString(), key)
			return "", e
		}
		return "", logger.ValueIsExpired
	}

	si.mutex.RUnlock()

	entry := &storage.Entry{
		EntryType: storage.TypeRecord,
		ExpiredAt: indexNode.expiredAt,
		Key:       []byte(key),
		Value:     []byte(newValue),
	}
	oldValue := string(indexNode.value)

	// 再尝试Set
	si.mutex.Lock()

	// 先写入文件
	offset, e := si.writeEntry(entry)
	if e != nil {
		return "", e
	}
	// 再更新indexNode
	indexNode.value = []byte(newValue)
	indexNode.offset = offset
	indexNode.fileID = si.activeFile.GetFileID()

	si.mutex.Unlock()

	// 之后就不再需要写入索引了
	return oldValue, nil
}

// SetNX 只有在key不存在时设置key的值
func (si *StringIndex) SetNX(key string, value string, expiredAt int64) error {
	_, e := si.Get(key)
	if e != nil {
		return si.Set(key, value, expiredAt)
	} else {
		return logger.KeyIsExisted
	}
}

// Append 在key存在的情况下 向其已经存在的value追加一个字符串
func (si *StringIndex) Append(key string, appendValue string) error {

	si.mutex.RLock()
	value, e := si.index.QueryNode(key)
	if e != nil {
		si.mutex.RUnlock()
		return logger.KeyIsNotExisted
	}
	indexNode := assertIndexNodePointer(value)

	si.mutex.RUnlock()

	entry := &storage.Entry{
		EntryType: storage.TypeRecord,
		ExpiredAt: indexNode.expiredAt,
		Key:       []byte(key),
		Value:     append(indexNode.value, []byte(appendValue)...),
	}

	// 再尝试Set
	si.mutex.Lock()

	// 先写入文件
	offset, e := si.writeEntry(entry)
	if e != nil {
		return e
	}
	// 再更新indexNode
	indexNode.value = append(indexNode.value, []byte(appendValue)...)
	indexNode.offset = offset
	indexNode.fileID = si.activeFile.GetFileID()

	si.mutex.Unlock()
	return nil
}

// Del 如果key存在 则删除key对应的value
func (si *StringIndex) Del(key string) error {
	si.mutex.RLock()
	value, e := si.index.QueryNode(key)
	if e != nil {
		si.mutex.RUnlock()
		return logger.KeyIsNotExisted
	}
	indexNode := assertIndexNodePointer(value)
	si.mutex.RUnlock()

	entry := &storage.Entry{
		EntryType: storage.TypeDelete,
		Key:       []byte(key),
		Value:     indexNode.value,
		ExpiredAt: 0,
	}

	si.mutex.Lock()
	_, e = si.writeEntry(entry)
	if e != nil {
		return e
	}
	e = si.index.DeleteNode(key)
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), key)
		return e
	}
	si.mutex.Unlock()
	return nil
}

// writeEntry 尝试将entry写入文件 如果活跃文件写满则自动新开一个文件继续尝试写入 如果写入成功则返回nil和写入前的offset
func (si *StringIndex) writeEntry(entry *storage.Entry) (int64, error) {
	offset := si.activeFile.GetOffset()
	e := si.activeFile.WriteEntryIntoFile(entry)
	// 如果文件已满
	if errors.Is(e, logger.FileBytesIsMaxedOut) {
		// 先结束旧文件的定时同步
		si.activeFile.StopSyncRoutine()
		// 开一个新的文件 这个新的活跃文件的序号自动在之前的活跃文件上 + 1
		si.activeFile, e = storage.NewRecordFile(si.fileIOMode, storage.String, si.activeFile.GetFileID()+1, si.baseFolderPath, si.fileMaxSize)
		if e != nil {
			return 0, e
		}
		// 这个新的活跃文件写入归档文件映射中
		si.archivedFile[si.activeFile.GetFileID()] = si.activeFile
		// 先开启定时同步
		si.activeFile.StartSyncRoutine(si.syncDuration)
		// 再尝试写入
		offset = si.activeFile.GetOffset()
		e = si.activeFile.WriteEntryIntoFile(entry)
		if e != nil {
			return 0, e
		}
	} else if e != nil {
		return 0, e
	}
	return offset, nil
}

// handleEntry 接收Entry 并且写入String索引 注意它只对索引进行操作
func (si *StringIndex) handleEntry(entry *storage.Entry, fileID uint32, offset int64) error {
	var e error

	si.mutex.Lock()
	defer si.mutex.Unlock()

	switch entry.EntryType {
	case storage.TypeDelete:
		{
			e = si.index.DeleteNode(string(entry.Key))
			if e != nil {
				logger.GenerateErrorLog(false, false, e.Error(), si.index.ToString(), string(entry.Key), string(entry.Value))
			}
		}
	case storage.TypeRecord:

		// 这里之所以只对RecordEntry进行检查 是因为对map的delete函数 如果传入的map本身就是空或者要删除的键找不到值 它就直接返回了 并不会报错
		// 所以DeleteEntry不需要过期检查 过期就过期吧 过期了也只是no-op而已
		// 如果过期时间为-1则说明永不过期
		if entry.ExpiredAt < time.Now().Unix() && entry.ExpiredAt != -1 {
			// attention 过期logger
			return nil
		}

		e = si.index.AddNode(string(entry.Key), &IndexNode{
			value:     entry.Value,
			expiredAt: entry.ExpiredAt,
			fileID:    fileID,
			offset:    offset,
		})
		if e != nil {
			logger.GenerateErrorLog(false, false, e.Error(), si.index.ToString(), string(entry.Key), string(entry.Value))
		}
	}
	return nil
}
