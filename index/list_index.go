package index

import (
	"MisakaDB/logger"
	"MisakaDB/storage"
	"MisakaDB/util"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

/*
为了解决每个元素的过期问题

我在后台启动一个协程 该协程会监控一个 channel

每次插入新值类的操作之后都启动一个协程 该协程等待过期时间后会向该 channel 发送是哪个 key 的哪个元素过期了

后台的这个协程通过 channel 获取该信息后立即更新 index 同时向文件中写入过期的 entry

这样在还原列表的时候我就能准确把握过期和别的操作的时间顺序

除此之外 还原列表之后我还是需要对所有的元素再遍历一次 因为有些过期是有可能发生在数据库关闭期间的 而且就算是没过期 也需要使其加入上面的过期机制中

这套方法的超时误差大概在2ms左右 而且还存在一个问题 就是 expiredChanMonitor 函数本身执行是需要时间的 如果有大量的元素在同一时间过期 同时还有读写请求的话

势必会造成协程之间的抢锁 而为了避免饥饿现象 锁应该会分配给读写请求 所以做不到绝对的安全
*/

type expiredInfo struct {
	key         []byte
	expiredNode *indexNode
}

type ListIndex struct {
	index        map[string][]*indexNode
	mutex        sync.RWMutex
	activeFile   *storage.RecordFile
	archivedFile map[uint32]*storage.RecordFile

	fileIOMode     storage.FileIOType
	baseFolderPath string
	fileMaxSize    int64
	syncDuration   time.Duration

	expiredAtChan chan *expiredInfo
	closeMonitor  chan int
}

// BuildListIndex 给定当前活跃文件和归档文件 重新构建List类型的索引 该方法只会在数据库启动时被调用 如果不存在旧的文件 则新建一个活跃文件
func BuildListIndex(activeFile *storage.RecordFile, archivedFile map[uint32]*storage.RecordFile, fileIOMode storage.FileIOType, baseFolderPath string, fileMaxSize int64, syncDuration time.Duration) (*ListIndex, error) {

	result := &ListIndex{
		index:          make(map[string][]*indexNode),
		activeFile:     activeFile,
		archivedFile:   archivedFile,
		fileIOMode:     fileIOMode,
		baseFolderPath: baseFolderPath,
		fileMaxSize:    fileMaxSize,
		syncDuration:   syncDuration,
		expiredAtChan:  make(chan *expiredInfo, 1),
		closeMonitor:   make(chan int, 1),
	}

	var (
		e           error
		offset      int64
		fileLength  int64
		entryLength int64
		entry       *storage.Entry
	)

	if activeFile == nil {
		result.activeFile, e = storage.NewRecordFile(fileIOMode, storage.List, 1, result.baseFolderPath, result.fileMaxSize)
		if e != nil {
			return nil, e
		}
		result.archivedFile = make(map[uint32]*storage.RecordFile)
		result.archivedFile[1] = result.activeFile
		result.activeFile.StartSyncRoutine(result.syncDuration)
		goto ReadFileFished
	}

	for i := uint32(0); i < uint32(len(archivedFile)); i++ {
		recordFile, ok := archivedFile[i]
		if !ok {
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

ReadFileFished:
	result.refreshList()
	result.activeFile.StartSyncRoutine(result.syncDuration)
	go result.expiredChanMonitor()

	return result, nil
}

// expiredChanMonitor 监控 channel 如果有过期消息发过来就处理 具体包括修改 index 向文件中写入过期 entry
func (li *ListIndex) expiredChanMonitor() {
	var expiredNode *expiredInfo
	var e error
	var i int
	for {
		select {
		case expiredNode = <-li.expiredAtChan:
			li.mutex.RLock()

			key := string(expiredNode.key)

			targetList, ok := li.index[key]
			if !ok {
				li.mutex.RUnlock()
				continue
			}

			i = 0
			for ; i < len(targetList); i++ {
				if targetList[i] == expiredNode.expiredNode { // 指针比较总比字节数组循环快吧
					// 找到过期值
					break
				}
			}

			li.mutex.RUnlock()
			li.mutex.Lock()

			// 写入文件
			_, e = li.writeEntry(&storage.Entry{
				Key:       expiredNode.key,
				Value:     expiredNode.expiredNode.value,
				EntryType: storage.TypeListExpired,
				ExpiredAt: 0,
			})
			if e != nil {
				logger.GenerateErrorLog(false, false, e.Error())
				continue
			}

			// 正式开始删除
			for ; i < len(targetList)-1; i++ {
				targetList[i] = targetList[i+1]
			}
			// 写回去
			li.index[key] = targetList[:len(targetList)-1]

			li.mutex.Unlock()
		case <-li.closeMonitor:
			return
		}
	}
}

// delayExpiredMessage 延迟发送过期消息到 channel 中
//
// 比如说列表里有1 2 3 4这几个元素 如果 index 指定为2过期 那么过期处理后列表是1 2 4
func (li *ListIndex) delayExpiredMessage(key []byte, expiredNode *indexNode) {
	time.Sleep(time.Until(time.UnixMilli(expiredNode.expiredAt)))
	li.expiredAtChan <- &expiredInfo{
		key:         key,
		expiredNode: expiredNode,
	}
}

// CloseIndex 关闭 String 索引 同时停止定时Sync 关闭文件
func (li *ListIndex) CloseIndex() (err error) {
	defer func() {
		if e := recover(); e != nil {
			panicString := fmt.Sprintf("%s", e)
			logger.GenerateErrorLog(true, false, panicString)
			err = errors.New(panicString)
		}
	}()

	li.mutex.Lock()
	defer li.mutex.Unlock()
	for _, v := range li.archivedFile {
		if v.IsSyncing {
			v.StopSyncRoutine()
		}
		e := v.Close()
		if e != nil {
			if errors.Is(e, os.ErrClosed) {
				continue
			}
			return e
		}
	}
	li.closeMonitor <- 1
	close(li.closeMonitor)
	close(li.expiredAtChan)
	return nil
}

// writeEntry 向文件中写入 entry 使其持久化
func (li *ListIndex) writeEntry(entry *storage.Entry) (int64, error) {
	offset := li.activeFile.GetOffset()
	e := li.activeFile.WriteEntryIntoFile(entry)
	if errors.Is(e, logger.FileBytesIsMaxedOut) {
		li.activeFile.StopSyncRoutine()
		li.activeFile, e = storage.NewRecordFile(li.fileIOMode, storage.List, li.activeFile.GetFileID()+1, li.baseFolderPath, li.fileMaxSize)
		if e != nil {
			return 0, e
		}
		li.archivedFile[li.activeFile.GetFileID()] = li.activeFile
		li.activeFile.StartSyncRoutine(li.syncDuration)
		offset = li.activeFile.GetOffset()
		e = li.activeFile.WriteEntryIntoFile(entry)
		if e != nil {
			return 0, e
		}
	} else if e != nil {
		return 0, e
	}
	return offset, nil
}

// handleEntry 从文件中还原列表时 按 entry 对 index 进行操作
func (li *ListIndex) handleEntry(entry *storage.Entry, fileID uint32, offset int64) error {
	//li.mutex.Lock()
	//defer li.mutex.Unlock()
	// handleEntry 只会在初始化还原列表的时候被顺序调用 所以不用锁也没事

	// 切片的自动扩容只会发生在 append 函数中

	targetSlice, ok := li.index[string(entry.Key)]
	if !ok && entry.EntryType != storage.TypeLPush {
		return logger.KeyIsNotExisted
	}

	switch entry.EntryType {
	case storage.TypeDelete: // 对应 lrem
		// entry 里面的值是要删的元素的 index
		removeIndex, e := strconv.Atoi(string(entry.Key))
		if e != nil {
			return e
		}
		targetSlice = append(targetSlice[:removeIndex], targetSlice[removeIndex+1:]...)

	case storage.TypeRecord: // 对应 lset
		// 需要解析 index
		value, indexString, e := util.DecodeKeyAndField(entry.Value)
		if e != nil {
			return e
		}
		index, e := strconv.Atoi(indexString)
		if e != nil {
			return e
		}
		targetSlice[index].value = []byte(value)
		targetSlice[index].expiredAt = entry.ExpiredAt
		targetSlice[index].offset = offset
		targetSlice[index].fileID = fileID
		return nil
	case storage.TypeLInsert:
		// 需要解析 index
		value, indexString, e := util.DecodeKeyAndField(entry.Value)
		if e != nil {
			return e
		}
		index, e := strconv.Atoi(indexString)
		if e != nil {
			return e
		}
		targetSlice = append(targetSlice, targetSlice[len(targetSlice)-1])
		for i := len(targetSlice) - 2; i > index; i-- {
			targetSlice[i] = targetSlice[i-1]
		}
		targetSlice[index] = &indexNode{
			value:     []byte(value),
			fileID:    fileID,
			offset:    offset,
			expiredAt: entry.ExpiredAt,
		}
	case storage.TypeLPop:
		if len(targetSlice) == 1 || len(targetSlice) == 0 {
			delete(li.index, string(entry.Key))
			return nil
		}
		targetSlice = append(targetSlice[1:])
		return nil
	case storage.TypeLPush:
		if targetSlice == nil {
			targetSlice = []*indexNode{{
				value:     entry.Value,
				fileID:    fileID,
				offset:    offset,
				expiredAt: entry.ExpiredAt,
			}}
			li.index[string(entry.Key)] = targetSlice
			return nil
		}
		targetSlice = append(targetSlice, targetSlice[len(targetSlice)-1])
		for i := len(targetSlice) - 2; i > 0; i-- {
			targetSlice[i] = targetSlice[i-1]
		}
		targetSlice[0] = &indexNode{
			value:     entry.Value,
			fileID:    fileID,
			offset:    offset,
			expiredAt: entry.ExpiredAt,
		}
	case storage.TypeListExpired:
		deleteIndex := 0
		for i := 0; i < len(targetSlice); i++ {
			if util.BytesArrayCompare(targetSlice[i].value, entry.Value) {
				deleteIndex = i
				break
			}
		}

		for ; deleteIndex < len(targetSlice)-1; deleteIndex++ {
			targetSlice[deleteIndex] = targetSlice[deleteIndex+1]
		}
		targetSlice = append(targetSlice[:len(targetSlice)-1])
	}
	li.index[string(entry.Key)] = targetSlice
	return nil
}

// refreshListRange 重整 list 对 index 中的每个 list 的全部元素进行遍历 检查是否过期 如果过期就删除
//
// 该函数只会被从文件中还原列表之后调用
func (li *ListIndex) refreshList() {
	// 该函数的作用是 遍历 index 里面的元素 检查是否过期
	// 如果过期就删除 这个函数要处理的是有过期 但是没有过期 entry 的元素 也就是该元素的过期时间是在数据库关闭期间发生的 这个可以直接删 因为过期前后都不会有新的写入
	// 如果有设置过期 但是还没到时间 就发消息到 channel 里面
	var targetSlice []*indexNode
	var i int
	var unix int64
	for key := range li.index {
		targetSlice = li.index[key]
		i = 0
		for i < len(targetSlice) {
			if targetSlice[i].expiredAt == -1 {
				i += 1
				continue
			} else {
				unix = time.Now().UnixMilli()
				if targetSlice[i].expiredAt <= unix {
					// 过期 直接删除
					for j := i; j < len(targetSlice)-1; j++ {
						targetSlice[j] = targetSlice[j+1]
					}
					targetSlice = append(targetSlice[:len(targetSlice)-1])
				} else {
					// 没过期
					li.delayExpiredMessage([]byte(key), targetSlice[i])
					i += 1
				}
			}
		}
		li.index[key] = targetSlice
	}
}

// LInsert 插入操作 插入位置由 index 指定
//
// 比如说列表里有1 2 3 4这几个元素 如果 index 指定为2插入一个10 那么插入后列表是1 2 10 3 4
func (li *ListIndex) LInsert(key []byte, index int, value []byte, expiredAt int64) error {
	li.mutex.RLock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		li.mutex.RUnlock()
		return logger.KeyIsNotExisted
	}
	if index > len(targetSlice) {
		li.mutex.RUnlock()
		return logger.IndexIsIllegal
	}

	li.mutex.RUnlock()
	li.mutex.Lock()
	defer li.mutex.Unlock()

	newEntry := &storage.Entry{
		Key:       key,
		Value:     util.EncodeKeyAndField(string(value), strconv.Itoa(index)),
		EntryType: storage.TypeLInsert,
		ExpiredAt: expiredAt,
	}
	offset, e := li.writeEntry(newEntry)
	if e != nil {
		return e
	}
	targetSlice = append(targetSlice, targetSlice[len(targetSlice)-1])
	for i := len(targetSlice) - 2; i >= index; i-- {
		targetSlice[i+1] = targetSlice[i]
	}
	targetSlice[index] = &indexNode{
		value:     value,
		fileID:    li.activeFile.GetFileID(),
		offset:    offset,
		expiredAt: expiredAt,
	}

	li.index[string(key)] = targetSlice

	if expiredAt != -1 {
		// 有实际的过期时间
		go li.delayExpiredMessage(key, targetSlice[index])
	}

	return nil
}

// LPop 弹出操作 特别说明是从 list 的首部弹出元素
//
// 比如说列表里有1 2 3 4这几个元素 弹出后列表是2 3 4
//
// 此外 该函数也承担删除整个列表的操作 只要列表中只有一个元素或者没有元素 该函数都会从 index 中移除列表
func (li *ListIndex) LPop(key []byte) ([]byte, error) {
	li.mutex.RLock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		li.mutex.RUnlock()
		return nil, logger.KeyIsNotExisted
	}

	li.mutex.RUnlock()
	li.mutex.Lock()
	defer li.mutex.Unlock()

	newEntry := &storage.Entry{
		Key:       key,
		Value:     nil,
		EntryType: storage.TypeLPop,
		ExpiredAt: 0,
	}
	_, e := li.writeEntry(newEntry)
	if e != nil {
		return nil, e
	}
	if len(targetSlice) == 0 {
		delete(li.index, string(key))
		return nil, nil
	} else {
		result := targetSlice[0].value
		targetSlice[0] = nil
		if len(targetSlice) == 1 {
			delete(li.index, string(key))
			return result, nil
		}
		for i := 1; i < len(targetSlice); i++ {
			targetSlice[i-1] = targetSlice[i]
		}

		targetSlice = append(targetSlice[:len(targetSlice)-1])
		li.index[string(key)] = targetSlice
		return result, nil
	}
}

// LPush 压入操作 特别说明是从 list 的首部压入元素
//
// 比如说列表里有1 2 3 4这几个元素 压入10列表是10 1 2 3 4
//
// # LPush 的多个元素一起 push 的操作是在该函数的上一层反复调用该函数实现的
//
// 该函数也承担创建一个列表的操作 如果列表不存在 就创建一个列表
func (li *ListIndex) LPush(key []byte, expiredAt int64, value []byte) error {
	li.mutex.Lock()
	defer li.mutex.Unlock()

	var e error
	var offset int64
	offset, e = li.writeEntry(&storage.Entry{
		Key:       key,
		Value:     value,
		EntryType: storage.TypeLPush,
		ExpiredAt: expiredAt,
	})
	if e != nil {
		return e
	}

	targetSlice, ok := li.index[string(key)]
	if !ok {
		targetSlice = []*indexNode{{
			value:     value,
			fileID:    li.activeFile.GetFileID(),
			offset:    offset,
			expiredAt: expiredAt,
		}}
	} else {
		// 把后面的元素挪一挪
		targetSlice = append(targetSlice, targetSlice[len(targetSlice)-1])
		for i := len(targetSlice) - 2; i > 0; i-- {
			targetSlice[i+1] = targetSlice[i]
		}
		// 然后再 push
		targetSlice[0] = &indexNode{
			value:     value,
			fileID:    li.activeFile.GetFileID(),
			offset:    offset,
			expiredAt: expiredAt,
		}
	}
	li.index[string(key)] = targetSlice
	if expiredAt != -1 {
		// 有实际的过期时间
		go li.delayExpiredMessage(key, targetSlice[0])
	}
	return nil
}

// LSet 修改操作
//
// 比如说列表里有1 2 3 4这几个元素 设置 index 为2的元素为10 修改列表是1 2 10 4
//
// 不支持修改过期时间
func (li *ListIndex) LSet(key []byte, index int, value []byte) error {
	li.mutex.RLock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		li.mutex.RUnlock()
		return logger.KeyIsNotExisted
	}
	if index >= len(targetSlice) {
		li.mutex.RUnlock()
		return logger.IndexIsIllegal
	}

	li.mutex.RUnlock()
	li.mutex.Lock()
	defer li.mutex.Unlock()

	setIndexNode := targetSlice[index]

	offset, e := li.writeEntry(&storage.Entry{
		Key:       key,
		Value:     util.EncodeKeyAndField(string(value), strconv.Itoa(index)),
		EntryType: storage.TypeRecord,
		ExpiredAt: setIndexNode.expiredAt,
	})
	if e != nil {
		return e
	}
	setIndexNode.value = value
	setIndexNode.fileID = li.activeFile.GetFileID()
	setIndexNode.offset = offset

	return nil
}

// LRem 删除操作 删除的对象由 count 和 value 一起指定
//
// 如果 count > 0 就是从列表表头开始搜索 删除 count 个符合条件的元素 如果 count < 0 就是从列表表尾开始搜索 删除 count 个符合条件的元素 如果 count = 0 就是删除列表中所有符合条件的元素
//
// 被删除的元素的 value 必须和传入的 value 一致
//
// 当 count != 0 时 如果被删除的元素个数不满足 count 就会返回 RemoveCountIsNotEnough 错误 返回该错误时 并不会影响索引中存储的值
//
// 注意 返回的 error 可能是多重错误 既有 RemoveCountIsNotEnough 也有文件写入错误
func (li *ListIndex) LRem(key []byte, count int, value []byte) error {
	li.mutex.RLock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		li.mutex.RUnlock()
		return logger.KeyIsNotExisted
	}
	li.mutex.RUnlock()
	li.mutex.Lock()
	defer li.mutex.Unlock()

	removeIndexNode := make([]int, count)
	if count >= 0 {
		// 从头开始搜索
		i := 0
		for i < len(targetSlice) {
			if util.BytesArrayCompare(targetSlice[i].value, value) {
				removeIndexNode = append(removeIndexNode, i)
				targetSlice = append(targetSlice[:i], targetSlice[i+1:]...) // 删除

				// 检查 count
				if len(removeIndexNode) == count && count > 0 {
					break
				}
			} else {
				i += 1
			}
		}
	} else {
		// 从尾开始搜索
		i := len(targetSlice) - 1
		for i >= 0 {
			if util.BytesArrayCompare(targetSlice[i].value, value) {
				removeIndexNode = append(removeIndexNode, i)
				targetSlice = append(targetSlice[:i], targetSlice[i+1:]...) // 删除

				// 检查 count
				if len(removeIndexNode) == -count {
					break
				}
			} else {
				i -= 1
			}
		}
	}
	var e error
	if count != 0 && (len(removeIndexNode) != -count || len(removeIndexNode) != count) {
		// 没删够
		return logger.RemoveCountIsNotEnough
	}

	li.index[string(key)] = targetSlice

	// 每次存的都是相对于那次删除发生时的 index
	// 假设列表是 1 2 2 4 我要删的是 count = 2 value = 2
	// 那么写入文件的就是两次删除时的 index 即两个1
	// removeIndexNode 里面的顺序即为删除发生的顺序
	for i := 0; i < len(removeIndexNode); i++ {
		_, e = li.writeEntry(&storage.Entry{
			Key:       key,
			Value:     []byte(strconv.Itoa(removeIndexNode[i])),
			EntryType: storage.TypeDelete,
			ExpiredAt: 0,
		})
		if e != nil {
			errors.Join(e)
		}
	}
	return e
}

// LIndex 按 index 进行查询操作
//
// 比如说列表里有1 2 3 4这几个元素 查询 index 为2的元素 返回的就是3
func (li *ListIndex) LIndex(key []byte, index int) (value []byte, err error) {
	li.mutex.RLock()
	defer li.mutex.RUnlock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		return nil, logger.KeyIsNotExisted
	}
	if index >= len(targetSlice) {
		return nil, logger.IndexIsIllegal
	}
	return targetSlice[index].value, nil
}

// LLen 查询列表长度
func (li *ListIndex) LLen(key []byte) (int, error) {
	li.mutex.RLock()
	defer li.mutex.RUnlock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		return 0, logger.KeyIsNotExisted
	}
	return len(targetSlice), nil
}

// LRange 按范围查询列表内的元素 查询范围是 [start, end)
//
// 比如说列表里有1 2 3 4这几个元素 查询 start 为1 end 为3的元素区间 返回的就是2 3
func (li *ListIndex) LRange(key []byte, start, end int) ([][]byte, error) {
	li.mutex.RLock()
	defer li.mutex.RUnlock()

	targetSlice, ok := li.index[string(key)]
	if !ok {
		return nil, logger.KeyIsNotExisted
	}
	if end > len(targetSlice) {
		return nil, logger.IndexIsIllegal
	}
	result := make([][]byte, end-start)
	for i := 0; i < end-start; i++ {
		result[i] = targetSlice[start+i].value
	}
	return result, nil
}
