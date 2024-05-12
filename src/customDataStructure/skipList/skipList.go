package skipList

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	indexUpgradeProbability = 0.5 // 索引晋升概率
	indexMaxHeight          = 32  // 索引最高高度
	// 这种情况下跳表存储节点最好不要超过2^32个
)

type (
	// SkipList 跳表的简单实现
	SkipList struct {
		head   *skipListNode // 头节点
		length uint32        // 跳表当前长度
		height uint32        // 跳表当前高度 注意这个高度是1的时候 对应的索引是在index[0]
		mtx    sync.RWMutex  // 读写锁 上并发保护用的
	}

	// index 具体的索引 指向节点在某一层的下一个节点
	index struct {
		nextNode *skipListNode
	}

	// skipListNode 跳表节点
	skipListNode struct {
		key        string  // 键
		value      any     // 值 值可以是任何类型
		indexLevel []index // 存储该节点在任意层的索引
	}
)

// NewSkipList 跳表的构造函数 默认索引晋升概率0.5 默认索引最高高度为32
func NewSkipList() *SkipList {
	return &SkipList{
		length: 0,
		head: &skipListNode{
			indexLevel: make([]index, indexMaxHeight),
		},
		height: 1,
	}
}

// randomLevel 为每个新加入的节点随机一个索引高度
func (sl *SkipList) randomLevel() int {
	l := 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Float64() < indexUpgradeProbability && l < indexMaxHeight {
		l += 1
	}
	return l
}

// ToString 将跳表所存储的所有节点的键和索引高度信息转换为字符串的形式并返回 仅作为检查使用 pass
func (sl *SkipList) ToString() string {
	pointer := sl.head
	result := make([]string, sl.length+1)

	for i := 1; i <= int(sl.length); i++ {
		pointer = pointer.indexLevel[0].nextNode
		result[i] = "Key " + strconv.Itoa(i) + ": " + pointer.key + ", Index Height: " + strconv.Itoa(len(pointer.indexLevel))
	}
	return strings.Join(result, "\n")
}

// internalQueryNode 仅用于内部的查询指定节点的方法 如果存在则返回对应节点 如果不存在则返回空和一个error pass
func (sl *SkipList) internalQueryNode(key string) (result *skipListNode, err error) {

	// 加读锁
	sl.mtx.RLock()
	defer sl.mtx.RUnlock()

	// 先找到最高的索引
	pointer := sl.head
	height := sl.height

	// 从最高的索引开始 一步一步向下开始寻找值
	for {
		if pointer.indexLevel[height-1].nextNode == nil { // 如果当前层的索引循环完成仍然找不到值 就尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1 // 索引已经到最低 无法再下降 说明找不到元素
				continue
			}
		}
		if strings.Compare(key, pointer.indexLevel[height-1].nextNode.key) < 0 { // key较小 尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1
				continue
			}
		} else if strings.Compare(key, pointer.indexLevel[height-1].nextNode.key) > 0 { // key较大 按当前高度索引继续向前寻找
			pointer = pointer.indexLevel[height-1].nextNode
		} else {
			result = pointer.indexLevel[height-1].nextNode
			err = nil
			return
		}
	}
	err = errors.New("Query Key: " + key + " is not Existed! \n")
	result = nil
	return
}

// QueryNode 跳表的单节点查询方法 返回给定键所对应的值 pass
func (sl *SkipList) QueryNode(key string) (value any, err error) {
	result, e := sl.internalQueryNode(key)
	if e != nil {
		return nil, e
	} else {
		return result.value, nil
	}
}

// QueryNodeInterval 按给定的key1到key2的范围进行区间查询 返回这个区间（含两侧）内的所有value pass
func (sl *SkipList) QueryNodeInterval(key1 string, key2 string) (values []any, err error) {
	if strings.Compare(key1, key2) > 0 { // key1大于key2时交换 强制key1必须小于key2
		key1, key2 = key2, key1
	} else if strings.Compare(key1, key2) == 0 { // key1等于key2时等于请求一个元素
		value, e := sl.QueryNode(key1)
		if e != nil {
			return nil, e
		} else {
			return []any{value}, nil
		}
	}

	node1, e := sl.internalQueryNode(key1) // 先找起点
	if e != nil {
		return nil, e
	}
	values = append(values, node1.value)

	sl.mtx.RLock()

	node2 := node1.indexLevel[0].nextNode
	for node2 != nil { // 在找终点的过程中顺便添加值
		if node2.key != key2 {
			values = append(values, node2.value)
			node2 = node2.indexLevel[0].nextNode
		} else {
			break
		}
	}
	if node2 == nil {
		e = errors.New("Query Key2: " + key2 + " is not Existed! \n")
		return nil, e
	}
	values = append(values, node2.value)
	sl.mtx.RUnlock()
	return values, nil
}

// SetNode 修改指定节点的值
func (sl *SkipList) SetNode(key string, value any) (err error) {
	setNode, e := sl.internalQueryNode(key)
	if e != nil {
		return e
	} else {
		setNode.value = value
		return nil
	}
}

// DeleteNode 删除指定节点 pass
func (sl *SkipList) DeleteNode(key string) (err error) {

	sl.mtx.RLock()

	update := make([]*index, sl.height)         // 存储要修改的各层索引
	for i := uint32(0); i <= sl.height-1; i++ { // 开始寻找各层的指向要删除的节点的索引 从最底层找起
		pointer := &sl.head.indexLevel[i]
		for pointer.nextNode != nil && pointer.nextNode.key < key {
			pointer = &pointer.nextNode.indexLevel[i]
		}
		if i == 0 { // 只有在最下面的索引才能检查该元素是否存在
			if pointer.nextNode == nil || pointer.nextNode.key != key {
				err = errors.New("Delete Key: " + key + " is not Existed! \n")
				sl.mtx.RUnlock()
				return
			}
		}
		if pointer.nextNode != nil && pointer.nextNode.key == key { // 只有确定了是要删除的节点才能进行更新 上面循环完成的两种情况都要考虑 不然就会空指针
			update[i] = pointer
		}
	}
	deleteNode := update[0].nextNode // 具体要删除的节点

	sl.mtx.RUnlock() // 寻找节点完成 开始删除
	sl.mtx.Lock()

	for i, v := range update {
		if v == nil {
			continue
		} else {
			v.nextNode = deleteNode.indexLevel[i].nextNode // 修改指向
		}
	}
	err = nil
	sl.length -= 1

	if update[sl.height-1] != nil { // 如果删除的节点的索引高度和当前跳表的索引高度一致 就有高度下降的可能 需要重置高度
		for i := range sl.head.indexLevel {
			if sl.head.indexLevel[i].nextNode == nil {
				sl.height = uint32(i)
			}
		}
	}
	sl.mtx.Unlock()

	return
}

// AddNode 向跳表中添加节点 节点键值不允许相同 pass
func (sl *SkipList) AddNode(key string, value any) (err error) {

	// 准备阶段
	indexHeight := sl.randomLevel()
	newNode := &skipListNode{
		indexLevel: make([]index, indexHeight),
		key:        key,
		value:      value,
	}
	update := make([]*index, indexHeight)
	pointer := sl.head

	// 确定update 确定每层索引插入位置
	sl.mtx.RLock()
	for i := range update {
		pointer = sl.head
		for pointer.indexLevel[i].nextNode != nil {
			if strings.Compare(key, pointer.indexLevel[i].nextNode.key) < 0 { // 参数s1 s2 如果结果为-1 说明s1 < s2
				update[i] = &pointer.indexLevel[i]
				break
			} else if strings.Compare(key, pointer.indexLevel[i].nextNode.key) == 0 { // 如果结果为0 说明s1 == s2
				return errors.New("Add Key: " + key + " is Existed! \n")
			}
			pointer = pointer.indexLevel[i].nextNode
		}
		if update[i] == nil { // 说明插入的元素是当前高度索引中最大的
			update[i] = &pointer.indexLevel[i]
		}
	}
	sl.mtx.RUnlock()

	// 开始插入
	sl.mtx.Lock()
	for i := range update {
		pointer = update[i].nextNode
		update[i].nextNode = newNode
		newNode.indexLevel[i].nextNode = pointer
	}
	if uint32(indexHeight) > sl.height { // 更新索引最高高度
		sl.height = uint32(indexHeight)
	}
	sl.length += 1
	sl.mtx.Unlock()

	return nil
}
