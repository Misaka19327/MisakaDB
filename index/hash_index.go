package index

import (
	"MisakaDB/logger"
	"MisakaDB/storage"
	"MisakaDB/util"
	"errors"
	"sync"
	"time"
)

type HashIndex struct {
	index        map[string]map[string]*IndexNode
	mutex        sync.RWMutex
	activeFile   *storage.RecordFile
	archivedFile map[uint32]*storage.RecordFile

	fileIOMode     storage.FileIOType
	baseFolderPath string
	fileMaxSize    int64
}

// BuildHashIndex 给定当前活跃文件和归档文件 重新构建索引 该方法只会在数据库启动时被调用 如果不存在旧的文件 则新建一个活跃文件
func BuildHashIndex(activeFile *storage.RecordFile, archivedFile map[uint32]*storage.RecordFile, fileIOMode storage.FileIOType, baseFolderPath string, fileMaxSize int64) (*HashIndex, error) {
	result := &HashIndex{
		activeFile:     activeFile,
		archivedFile:   archivedFile,
		fileIOMode:     fileIOMode,
		baseFolderPath: baseFolderPath,
		fileMaxSize:    fileMaxSize,
		index:          make(map[string]map[string]*IndexNode),
	}

	var offset int64
	var fileLength int64
	var entryLength int64
	var entry *storage.Entry
	var e error

	// 如果活跃文件都读取不到的话 肯定也没有归档文件了 直接返回即可
	if activeFile == nil {
		result.activeFile, e = storage.NewRecordFile(result.fileIOMode, storage.Hash, 1, result.baseFolderPath, result.fileMaxSize)
		if e != nil {
			return nil, e
		}
		result.archivedFile = make(map[uint32]*storage.RecordFile) // 如果activeFile为空 那么传进来的archivedFile一定也为空 这时再赋值会报错
		result.archivedFile[1] = result.activeFile
		return result, nil
	}

	// 读取所有归档文件的entry 因为活跃文件也在这个归档文件里 所以不再单独读取活跃文件
	for i := uint32(1); i <= uint32(len(archivedFile)); i++ {
		recordFile, ok := archivedFile[uint32(i)]
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

	return result, nil
}

func (hi *HashIndex) CloseIndex() error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()
	for _, v := range hi.archivedFile {
		e := v.Sync()
		if e != nil {
			return e
		}
		e = v.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// HSet 给定key field value 设定值 如果key field都存在即为更新值
func (hi *HashIndex) HSet(key string, field string, value string, expiredAt int64) error {
	entry := &storage.Entry{
		EntryType: storage.TypeRecord,
		ExpiredAt: expiredAt,
		Key:       util.EncodeKeyAndField(key, field),
		Value:     []byte(value),
	}
	indexNode := &IndexNode{
		expiredAt: expiredAt,
	}

	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	// 写入文件 同时记录offset和fileID
	offset, e := hi.writeEntry(entry)
	if e != nil {
		return e
	}
	indexNode.offset = offset
	indexNode.fileID = hi.activeFile.GetFileID()
	indexNode.value = []byte(value)

	// 最后写入索引
	if _, ok := hi.index[key]; ok {
		hi.index[key][field] = indexNode
	} else {
		hi.index[key] = make(map[string]*IndexNode)
		hi.index[key][field] = indexNode
	}
	return nil
}

// HSetNX 同HSet 但是只有在field不存在时才能写入 否则返回
func (hi *HashIndex) HSetNX(key string, field string, value string, expiredAt int64) error {
	// HExist和HSet都已加锁 所以这里没有锁操作
	ok, e := hi.HExist(key, field)
	if e != nil {
		return e
	}
	if ok == true {
		logger.GenerateErrorLog(false, false, logger.FieldIsExisted.Error(), key, field, value)
		return logger.FieldIsExisted
	} else {
		return hi.HSet(key, field, value, expiredAt)
	}
}

// HGet 根据给定的key和field获取尝试value
func (hi *HashIndex) HGet(key string, field string) (string, error) {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()

	_, ok := hi.index[key]
	if ok != true {
		logger.GenerateErrorLog(false, false, logger.KeyIsNotExisted.Error(), key, field)
		return "", logger.KeyIsNotExisted
	}

	indexNode, ok := hi.index[key][field]
	if ok != true {
		logger.GenerateErrorLog(false, false, logger.FieldIsNotExisted.Error(), key, field)
		return "", logger.FieldIsNotExisted
	}

	// 如果过期时间为-1则说明永不过期
	if indexNode.expiredAt < time.Now().Unix() && indexNode.expiredAt != -1 {
		logger.GenerateInfoLog(logger.ValueIsExpired.Error() + " {" + field + ": " + string(indexNode.value) + "}")
		// 读取的Entry过期 删索引
		delete(hi.index[key], field)
		return "", logger.ValueIsExpired
	}

	return string(indexNode.value), nil
}

// HDel 根据key和field尝试删除键值对 如果deleteField为true 则认为删的是hash里面的键值对 反之则认为删除的是整个hash
func (hi *HashIndex) HDel(key string, field string, deleteField bool) error {

	fieldIsExist, e := hi.HExist(key, field) // 查key和field
	if e != nil {
		return e
	}

	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	if deleteField {
		// 删键值对
		if fieldIsExist != true { // 查field
			logger.GenerateErrorLog(false, false, logger.FieldIsNotExisted.Error(), key, field)
			return logger.FieldIsNotExisted
		}
		entry := &storage.Entry{
			EntryType: storage.TypeDelete,
			Key:       util.EncodeKeyAndField(key, field),
			Value:     []byte{},
			ExpiredAt: 0,
		}
		// 尝试写入删除Entry 删除Entry不需要记录offset
		_, e = hi.writeEntry(entry)
		if e != nil {
			return e
		}
		// 然后修改索引
		delete(hi.index[key], field)
		return nil
	} else {
		// 删hash
		entry := &storage.Entry{
			EntryType: storage.TypeDelete,
			Key:       util.EncodeKeyAndField(key, ""),
			Value:     []byte{},
			ExpiredAt: 0,
		}
		// 尝试写入删除Entry 删除Entry不需要记录offset
		_, e = hi.writeEntry(entry)
		if e != nil {
			return e
		}
		// 然后修改索引
		delete(hi.index, key)
		return nil
	}
}

// HLen 根据给定的key 寻找field的个数
func (hi *HashIndex) HLen(key string) (int, error) {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()

	_, ok := hi.index[key]
	if ok != true {
		logger.GenerateErrorLog(false, false, logger.KeyIsNotExisted.Error(), key)
		return 0, logger.KeyIsNotExisted
	}
	return len(hi.index[key]), nil
}

// HExist 根据给定的key和field判断field是否存在
func (hi *HashIndex) HExist(key string, field string) (bool, error) {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()
	_, ok := hi.index[key]
	if ok != true {
		logger.GenerateErrorLog(false, false, logger.KeyIsNotExisted.Error(), key)
		return false, logger.KeyIsNotExisted
	}
	_, ok = hi.index[key][field]
	return ok, nil
}

// HStrLen 根据给定的key和field 确定value的长度
func (hi *HashIndex) HStrLen(key string, field string) (int, error) {
	// 不加锁原因同HSetNX
	value, e := hi.HGet(key, field)
	if e != nil {
		return 0, e
	}
	return len(value), nil
}

// writeEntry 尝试将entry写入文件 如果活跃文件写满则自动新开一个文件继续尝试写入 如果写入成功则返回nil和写入前的offset
func (hi *HashIndex) writeEntry(entry *storage.Entry) (int64, error) {
	offset := hi.activeFile.GetOffset()
	e := hi.activeFile.WriteEntryIntoFile(entry)
	// 如果文件已满
	if errors.Is(e, logger.FileBytesIsMaxedOut) {
		// 开一个新的文件 这个新的活跃文件的序号自动在之前的活跃文件上 + 1
		hi.activeFile, e = storage.NewRecordFile(hi.fileIOMode, storage.Hash, hi.activeFile.GetFileID()+1, hi.baseFolderPath, hi.fileMaxSize)
		if e != nil {
			return 0, e
		}
		// 这个新的活跃文件写入归档文件映射中
		hi.archivedFile[hi.activeFile.GetFileID()] = hi.activeFile
		// 再尝试写入
		offset = hi.activeFile.GetOffset()
		e = hi.activeFile.WriteEntryIntoFile(entry)
		if e != nil {
			return 0, e
		}
	} else if e != nil {
		return 0, e
	}
	//e = hi.activeFile.Sync()
	//if e != nil {
	//	return 0, e
	//}
	return offset, nil
}

// handleEntry 接收Entry 并且写入索引
// attention 它只对索引进行操作
func (hi *HashIndex) handleEntry(entry *storage.Entry, fileID uint32, offset int64) error {

	key, field, e := util.DecodeKeyAndField(entry.Key)
	if e != nil {
		return e
	}

	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	switch entry.EntryType {
	case storage.TypeDelete:
		{
			if field != "" { // 一个是按key删掉整个hash 一个实按field删一个value
				delete(hi.index[key], field)
			} else {
				delete(hi.index, key)
			}
		}
	case storage.TypeRecord:

		// attention 这里之所以只对RecordEntry进行检查 是因为对map的delete函数 如果传入的map本身就是空或者要删除的键找不到值 它就直接返回了 并不会报错
		// 所以DeleteEntry不需要过期检查 过期就过期吧 过期了也只是no-op而已
		// 如果过期时间为-1则说明永不过期
		if entry.ExpiredAt < time.Now().Unix() && entry.ExpiredAt != -1 {
			// attention 过期logger
			return nil
		}

		if _, ok := hi.index[key]; ok {
			hi.index[key][field] = &IndexNode{
				value:     entry.Value,
				expiredAt: entry.ExpiredAt,
				fileID:    fileID,
				offset:    offset,
			}
		} else {
			hi.index[key] = make(map[string]*IndexNode)
			hi.index[key][field] = &IndexNode{
				value:     entry.Value,
				expiredAt: entry.ExpiredAt,
				fileID:    fileID,
				offset:    offset,
			}
		}
	}
	return nil
}
