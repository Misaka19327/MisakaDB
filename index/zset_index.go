package index

import (
	"MisakaDB/customDataStructure/skipList"
	"MisakaDB/logger"
	"MisakaDB/storage"
	"MisakaDB/util"
	"errors"
	"strconv"
	"sync"
	"time"
)

type zsetNode struct {
	indexNode
	score int
}

type zsetScore struct {
	score int
}

func (z zsetScore) Compare(other skipList.Comparable) int {
	return z.score - other.(zsetScore).score
}

func (z zsetScore) String() string {
	return strconv.Itoa(z.score)
}

type zset struct {
	dict      map[string]*zsetNode
	skipList  *skipList.SkipList[*zsetNode]
	expireNum int // 过期计数 计算有过期删除的这个需求的 member 的数量 即 expiredAt 字段不为-1的 member 数量 如果它是0 获取有序集合信息的时候就不需要检查是否过期
}

type ZSetIndex struct {
	index map[string]*zset

	mutex        sync.RWMutex
	activeFile   *storage.RecordFile
	archivedFile map[uint32]*storage.RecordFile

	fileIOMode     storage.FileIOType
	baseFolderPath string
	fileMaxSize    int64
	syncDuration   time.Duration
}

// BuildZSetIndex 给定当前活跃文件和归档文件 重新构建ZSet类型的索引 该方法只会在数据库启动时被调用 如果不存在旧的文件 则新建一个活跃文件
func BuildZSetIndex(activeFile *storage.RecordFile, archivedFile map[uint32]*storage.RecordFile, fileIOMode storage.FileIOType, baseFolderPath string, fileMaxSize int64, syncDuration time.Duration) (*ZSetIndex, error) {

	result := &ZSetIndex{
		index:          make(map[string]*zset),
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

	if activeFile == nil {
		result.activeFile, e = storage.NewRecordFile(fileIOMode, storage.List, 1, result.baseFolderPath, result.fileMaxSize)
		if e != nil {
			return nil, e
		}
		result.archivedFile = make(map[uint32]*storage.RecordFile)
		result.archivedFile[1] = result.activeFile
		result.activeFile.StartSyncRoutine(result.syncDuration)
		return result, nil
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

	return result, nil
}

// CloseIndex 关闭 ZSet 索引 同时停止定时Sync 关闭文件
func (zi *ZSetIndex) CloseIndex() error {
	zi.mutex.Lock()
	defer zi.mutex.Unlock()
	for _, v := range zi.archivedFile {
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

// writeEntry 向文件中写入 entry 使其持久化
func (zi *ZSetIndex) writeEntry(entry *storage.Entry) (int64, error) {
	offset := zi.activeFile.GetOffset()
	e := zi.activeFile.WriteEntryIntoFile(entry)
	if errors.Is(e, logger.FileBytesIsMaxedOut) {
		zi.activeFile.StopSyncRoutine()
		zi.activeFile, e = storage.NewRecordFile(zi.fileIOMode, storage.List, zi.activeFile.GetFileID()+1, zi.baseFolderPath, zi.fileMaxSize)
		if e != nil {
			return 0, e
		}
		zi.archivedFile[zi.activeFile.GetFileID()] = zi.activeFile
		zi.activeFile.StartSyncRoutine(zi.syncDuration)
		offset = zi.activeFile.GetOffset()
		e = zi.activeFile.WriteEntryIntoFile(entry)
		if e != nil {
			return 0, e
		}
	} else if e != nil {
		return 0, e
	}
	return offset, nil
}

// handleEntry 从文件中还原列表时 按 entry 对 index 进行操作
func (zi *ZSetIndex) handleEntry(entry *storage.Entry, fileID uint32, offset int64) error {

	if entry.ExpiredAt < time.Now().UnixMilli() && entry.ExpiredAt != -1 {
		return nil
	}

	switch entry.EntryType {
	case storage.TypeDelete:
		targetZset := zi.index[string(entry.Key)]
		_ = targetZset.skipList.DeleteNode(zsetScore{score: targetZset.dict[string(entry.Value)].score})
		delete(targetZset.dict, string(entry.Value))

		if targetZset.skipList.Length() == 0 {
			delete(zi.index, string(entry.Key))
		}
		return nil
	case storage.TypeRecord:
		memberString, scoreString, e := util.DecodeKeyAndField(entry.Value)
		if e != nil {
			return e
		}
		score, _ := strconv.Atoi(scoreString)

		targetZset, ok := zi.index[string(entry.Key)]
		if !ok {
			targetZset = &zset{
				dict:     make(map[string]*zsetNode),
				skipList: skipList.NewSkipList[*zsetNode](),
			}
			zi.index[string(entry.Key)] = targetZset
		}
		targetNode := &zsetNode{
			indexNode: indexNode{
				value:     []byte(memberString),
				fileID:    fileID,
				offset:    offset,
				expiredAt: entry.ExpiredAt,
			},
			score: score,
		}
		targetZset.dict[memberString] = targetNode
		targetZset.skipList.AddNode(zsetScore{score: score}, targetNode)
		return nil
	default:
		return nil
	}
}

// ZAdd 按给定参数添加元素 如果元素存在则更新 如果有序集合不存在则创建
func (zi *ZSetIndex) ZAdd(key []byte, score int, member []byte, expiredAt int64) error {
	zi.mutex.Lock()
	defer zi.mutex.Unlock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		targetZset = &zset{
			dict:     make(map[string]*zsetNode),
			skipList: skipList.NewSkipList[*zsetNode](),
		}
		zi.index[string(key)] = targetZset
	}

	offset, e := zi.writeEntry(&storage.Entry{
		Key:       key,
		Value:     util.EncodeKeyAndField(string(member), strconv.Itoa(score)),
		EntryType: storage.TypeRecord,
		ExpiredAt: expiredAt,
	})

	if e != nil {
		return e
	}

	targetNode := &zsetNode{
		indexNode: indexNode{
			value:     member,
			fileID:    zi.activeFile.GetFileID(),
			offset:    offset,
			expiredAt: expiredAt,
		},
		score: score,
	}
	targetZset.dict[string(member)] = targetNode
	targetZset.skipList.AddNode(zsetScore{score: score}, targetNode)
	if expiredAt != -1 {
		targetZset.expireNum += 1
	}

	return nil
}

// ZRem 按给定的 key 和 member 删除元素 如果删除后有序列表不再拥有元素则自动删除有序列表
func (zi *ZSetIndex) ZRem(key []byte, member []byte) error {
	zi.mutex.RLock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		return logger.KeyIsNotExisted
	}

	targetNode, ok := targetZset.dict[string(member)]
	if !ok {
		return logger.MemberIsNotExisted
	}

	zi.mutex.RUnlock()
	zi.mutex.Lock()
	defer zi.mutex.Unlock()

	e := targetZset.skipList.DeleteNode(zsetScore{score: targetNode.score})
	if e != nil {
		return e
	}
	delete(targetZset.dict, string(member))
	if targetNode.expiredAt != -1 {
		targetZset.expireNum -= 1
	}

	if targetZset.skipList.Length() == 0 {
		delete(zi.index, string(key))
	}

	_, e = zi.writeEntry(&storage.Entry{
		Key:       key,
		Value:     member,
		EntryType: storage.TypeDelete,
		ExpiredAt: 0,
	})
	if e != nil {
		return e
	}

	return nil
}

// ZScore 按给定的 key 和 member 获取对应元素的 score
func (zi *ZSetIndex) ZScore(key []byte, member []byte) (int, error) {
	zi.mutex.RLock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		zi.mutex.RUnlock()
		return 0, logger.KeyIsNotExisted
	}

	targetNode, ok := targetZset.dict[string(member)]
	if !ok {
		zi.mutex.RUnlock()
		return 0, logger.MemberIsNotExisted
	}
	if targetNode.expiredAt < time.Now().UnixMilli() && targetNode.expiredAt != -1 {
		// 过期
		zi.mutex.RUnlock()
		zi.mutex.Lock()
		_ = targetZset.skipList.DeleteNode(zsetScore{score: targetNode.score})
		delete(targetZset.dict, string(member))
		targetZset.expireNum -= 1
		zi.mutex.Unlock()
		return 0, logger.MemberIsExpired
	}
	zi.mutex.RUnlock()
	return targetNode.score, nil
}

// ZCard 获取 zset 的有效成员数
func (zi *ZSetIndex) ZCard(key []byte) (int, error) {
	zi.mutex.RLock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		zi.mutex.RUnlock()
		return 0, logger.KeyIsNotExisted
	}
	if targetZset.expireNum == 0 {
		zi.mutex.RUnlock()
		return targetZset.skipList.Length(), nil
	} else {
		zi.mutex.RUnlock()
		zi.mutex.Lock()
		targetZset.refreshZset()
		zi.mutex.Unlock()
		return targetZset.skipList.Length(), nil
	}
}

// ZCount 获取 score 在 [min, max] 区间内的所有 member 个数
func (zi *ZSetIndex) ZCount(key []byte, min, max int) (int, error) {
	zi.mutex.RLock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		zi.mutex.RUnlock()
		return 0, logger.KeyIsNotExisted
	}
	if targetZset.expireNum != 0 {
		zi.mutex.RUnlock()
		zi.mutex.Lock()
		targetZset.refreshZset()
		zi.mutex.Unlock()
		zi.mutex.RLock()
	}
	nodes, e := targetZset.skipList.QueryNodeInterval(zsetScore{score: min}, zsetScore{score: max})
	if e != nil {
		zi.mutex.RUnlock()
		return 0, e
	}
	zi.mutex.RUnlock()
	return len(nodes), nil
}

// ZRange 获取 score 在 [min, max] 区间内的所有 member
func (zi *ZSetIndex) ZRange(key []byte, min, max int) ([][]byte, error) {
	zi.mutex.RLock()

	targetZset, ok := zi.index[string(key)]
	if !ok {
		zi.mutex.RUnlock()
		return nil, logger.KeyIsNotExisted
	}
	if targetZset.expireNum != 0 {
		zi.mutex.RUnlock()
		zi.mutex.Lock()
		targetZset.refreshZset()
		zi.mutex.Unlock()
		zi.mutex.RLock()
	}
	nodes, e := targetZset.skipList.QueryNodeInterval(zsetScore{score: min}, zsetScore{score: max})
	if e != nil {
		zi.mutex.RUnlock()
		return nil, e
	}
	result := make([][]byte, len(nodes))
	for i := range nodes {
		result[i] = nodes[i].value
	}
	zi.mutex.RUnlock()
	return result, nil
}

// refreshZset 对有序集合进行循环 删除过期元素
func (z *zset) refreshZset() {
	// 这块不加锁 因为调用者已经把锁加好了
	var node *zsetNode
	for key := range z.dict {
		node = z.dict[key]
		if node.expiredAt != -1 && node.expiredAt < time.Now().UnixMilli() {
			// 过期
			delete(z.dict, key)
			_ = z.skipList.DeleteNode(zsetScore{score: node.score})
			z.expireNum -= 1
		}
	}
}
