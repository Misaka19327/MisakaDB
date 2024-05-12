package index

import (
	"MisakaDB"
	"MisakaDB/src/logger"
	"MisakaDB/src/storage"
	"MisakaDB/src/util"
	"errors"
	"sync"
	"time"
)

type HashIndex struct {
	index           map[string]map[string]*IndexNode
	mutex           sync.RWMutex
	activeFile      *storage.RecordFile
	archivedFile    map[uint32]*storage.RecordFile
	archivedFileNum uint32
}

// BuildIndex 给定当前活跃文件和归档文件 重新构建索引 该方法只会在数据库启动时被调用
func BuildIndex(activeFile *storage.RecordFile, archivedFile map[uint32]*storage.RecordFile, archivedFileNum uint32) (*HashIndex, error) {
	result := &HashIndex{
		activeFile:      activeFile,
		archivedFile:    archivedFile,
		archivedFileNum: archivedFileNum,
	}

	var offset int64
	var fileLength int64
	var entryLength int64
	var entry *storage.Entry
	var e error

	// 先读取已经归档的entry
	for i := uint32(1); i <= archivedFileNum; i++ {
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
			e = result.handleEntry(entry, recordFile.GetFileID(), offset)
			if e != nil {
				return nil, e
			}
			offset += entryLength
		}
	}

	// 再读取还在active文件里的entry
	offset = 0
	fileLength, e = activeFile.Length()
	if e != nil {
		return nil, e
	}
	for offset < fileLength {
		entry, entryLength, e = activeFile.ReadIntoEntry(offset)
		e = result.handleEntry(entry, activeFile.GetFileID(), offset)
		if e != nil {
			return nil, e
		}
		offset += entryLength
	}

	return result, nil
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

	// 写入文件 如果文件过大无法写入 就新开一个文件继续存储 同时保存文件编号和文件偏移值
	indexNode.offset = hi.activeFile.GetOffset()
	e := hi.activeFile.WriteEntryIntoFile(entry)
	if errors.Is(e, logger.FileBytesIsMaxedOut) {
		hi.archivedFile[hi.activeFile.GetFileID()] = hi.activeFile
		hi.activeFile, e = storage.NewRecordFile(storage.TraditionalIOFile, storage.Hash, hi.archivedFileNum+1, MisakaDB.MisakaDataBaseFolderPath, MisakaDB.RecordFileMaxSize)
		if e != nil {
			return e
		}
		indexNode.offset = hi.activeFile.GetOffset()
		e = hi.activeFile.WriteEntryIntoFile(entry)
		if e != nil {
			return e
		}
		hi.archivedFileNum += 1
	} else if e != nil {
		return e
	}
	indexNode.fileID = hi.activeFile.GetFileID()

	// 最后写入索引
	hi.index[key][field] = indexNode
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
		return "", logger.KeyIsNotExisted
	}

	indexNode, ok := hi.index[key][field]
	if ok != true {
		return "", logger.FieldIsNotExisted
	}
	var result *storage.Entry
	var e error

	// 先判断这个node所对应的值是不是指向activeFile 然后再去对应位置取
	if indexNode.fileID == hi.activeFile.GetFileID() {
		result, _, e = hi.activeFile.ReadIntoEntry(indexNode.offset)
		if e != nil {
			return "", e
		}
	} else {
		result, _, e = hi.archivedFile[indexNode.fileID].ReadIntoEntry(indexNode.offset)
		if e != nil {
			return "", e
		}
	}

	return string(result.Value), nil
}

// HDel 根据key和field尝试删除键值对 如果deleteField为true 则认为删的是hash里面的键值对 反之则认为删除的是整个hash
func (hi *HashIndex) HDel(key string, field string, deleteField bool) error {

	fieldIsExist, e := hi.HExist(key, field) // 查key
	if e != nil {
		return e
	}

	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	if deleteField {
		// 删键值对
		if fieldIsExist != true { // 查field
			return logger.FieldIsNotExisted
		}
		entry := &storage.Entry{
			EntryType: storage.TypeDelete,
			Key:       util.EncodeKeyAndField(key, field),
			Value:     []byte{},
			ExpiredAt: 0,
		}
		// 尝试写入删除Entry
		e = hi.activeFile.WriteEntryIntoFile(entry)
		if errors.Is(e, logger.FileBytesIsMaxedOut) {
			hi.archivedFile[hi.activeFile.GetFileID()] = hi.activeFile
			hi.activeFile, e = storage.NewRecordFile(storage.TraditionalIOFile, storage.Hash, hi.archivedFileNum+1, MisakaDB.MisakaDataBaseFolderPath, MisakaDB.RecordFileMaxSize)
			if e != nil {
				return e
			}
			e = hi.activeFile.WriteEntryIntoFile(entry)
			if e != nil {
				return e
			}
			hi.archivedFileNum += 1
		} else if e != nil {
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
		// 尝试写入删除Entry
		e = hi.activeFile.WriteEntryIntoFile(entry)
		if errors.Is(e, logger.FileBytesIsMaxedOut) {
			hi.archivedFile[hi.activeFile.GetFileID()] = hi.activeFile
			hi.activeFile, e = storage.NewRecordFile(storage.TraditionalIOFile, storage.Hash, hi.archivedFileNum+1, MisakaDB.MisakaDataBaseFolderPath, MisakaDB.RecordFileMaxSize)
			if e != nil {
				return e
			}
			e = hi.activeFile.WriteEntryIntoFile(entry)
			if e != nil {
				return e
			}
			hi.archivedFileNum += 1
		} else if e != nil {
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

// handleEntry 接收Entry 并且写入索引
// attention 它只对索引进行操作
func (hi *HashIndex) handleEntry(entry *storage.Entry, fileID uint32, offset int64) error {
	if entry.ExpiredAt < time.Now().Unix() {
		// attention 过期logger
		return nil
	}

	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	key, field, e := util.DecodeKeyAndField(entry.Key)
	if e != nil {
		return e
	}
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
		hi.index[key][field] = &IndexNode{
			expiredAt: entry.ExpiredAt,
			fileID:    fileID,
			offset:    offset,
		}
	}
	return nil
}
