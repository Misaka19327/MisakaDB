package skipList

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

/*
这应该是最终的定型的跳表了 不打算再有大改了

虽然它要比 art 树在存储字符串方面慢了得有几十倍（相同的22万个英语词汇作为键值对读写 跳表耗时17分半 art 树加上删除操作耗时0.23秒）
内存方面也比 art 树占用高了一倍（还是上面那个任务 跳表用了 44977.57kB art 树用了 22448.74kB）

但是我逐渐意识到一个问题

首先 跳表结构上和实现上都相当之简易

其次 如果储存的是字符串的话 再怎么 compare 都是躲不过的 而这个 Key.Compare 就能占到总运行失常的 43%

这里面既有为泛型作出的让步（指转型） 也有必须要付出的代价（bytes.Compare 耗时34%）

20000 个键值对读写 pprof 报告如下

(pprof) top 20
Showing nodes accounting for 1.38s, 100% of 1.38s total
Showing top 20 nodes out of 67
      flat  flat%   sum%        cum   cum%
     0.46s 33.33% 33.33%      1.06s 76.81%  MisakaDB/customDataStructure/skipList.(*SkipList[go.shape.struct { MisakaDB/customDataStructure/skipList.s string }]).AddNode
     0.44s 31.88% 65.22%      0.44s 31.88%  cmpbody
     0.22s 15.94% 81.16%      0.22s 15.94%  runtime.cgocall
     0.13s  9.42% 90.58%      0.60s 43.48%  MisakaDB/customDataStructure/skipList.Key.Compare
     0.02s  1.45% 92.03%      0.47s 34.06%  bytes.Compare (inline)
     0.02s  1.45% 93.48%      0.02s  1.45%  runtime.scanblock
     0.02s  1.45% 94.93%      0.02s  1.45%  runtime.stdcall1
     0.01s  0.72% 95.65%      0.01s  0.72%  fmt.(*buffer).write (inline)
     0.01s  0.72% 96.38%      0.01s  0.72%  internal/bytealg.Compare
     0.01s  0.72% 97.10%      0.01s  0.72%  runtime.(*mheap).initSpan
     0.01s  0.72% 97.83%      0.01s  0.72%  runtime.concatstring2
     0.01s  0.72% 98.55%      0.01s  0.72%  runtime.growslice
     0.01s  0.72% 99.28%      0.01s  0.72%  runtime.nextFreeFast (inline)
     0.01s  0.72%   100%      0.01s  0.72%  runtime.stdcall2
         0     0%   100%      1.33s 96.38%  MisakaDB/customDataStructure/skipList.TestSkipList_SetNode
         0     0%   100%      0.01s  0.72%  fmt.(*fmt).fmtInteger
         0     0%   100%      0.01s  0.72%  fmt.(*fmt).pad
         0     0%   100%      0.01s  0.72%  fmt.(*pp).doPrintf
         0     0%   100%      0.01s  0.72%  fmt.(*pp).fmtInteger
         0     0%   100%      0.01s  0.72%  fmt.(*pp).printArg

所以虽然我意识到我写的代码在哪块一定出了问题 但是我自己也找不出来哪里有问题 最后结论上我是能接受跳表如此差的表现

并且上述结论引出了一个新的结论 即跳表使用上 Comparable 的 Compare 函数尽量采用数值比较 尽量别直接用字符数组或者字符串比较 性能表现差
*/

const (
	indexUpgradeProbability = 0.5 // 索引晋升概率
	indexMaxHeight          = 32  // 索引最高高度
	// 这种情况下跳表存储节点最好不要超过2^32个
)

// Value 值的泛型 存入跳表的值键值对的值必须符合该泛型所定义的类型
type Value interface {
	any

	// String 将 Value 转换为 String 的方法 如果不需要打印跳表 直接写一个空函数即可
	String() string
}

// Comparable 键的接口 存入跳表的键值对必须实现该接口
//
// 示例如下
//
//	type Key1 struct {
//		key string
//		compareValue int
//	}
//
//	func (k Key1) String () string {
//		return k.key
//	}
//
//	func (k Key1) Compare (other Comparable) int {
//		k2, ok := other.(Key1)
//		if !ok {
//			return 1
//		}
//		return k.compareValue - k2.compareValue
//	}
type Comparable interface {

	// Compare
	//
	// result 为-1时 调用者 较小
	//
	// result 为1时 调用者 较大
	//
	// result 等于0时 调用者和参数相同
	//
	// 上述的 1 -1 并不是严格的返回值一定是-1 只要大于0小于0即可
	Compare(other Comparable) int

	// String 生成报错信息和打印跳表的时候用的 如果不需要 直接写一个空函数即可
	String() string
}

// Key 预先定义好的键的类型 符合 Comparable 泛型 其底层为 []byte 类型
type Key []byte

// Compare
//
// result 为-1时 k1 较小
//
// result 为1时 k1 较大
//
// result 等于0时 两者相同
func (k1 Key) Compare(k2Comparable Comparable) (result int) {
	k2, ok := k2Comparable.(Key)
	if !ok {
		return 1
	}
	return bytes.Compare(k1, k2)
}

func (k1 Key) String() string {
	return string(k1)
}

type (
	// SkipList 跳表的简单实现
	SkipList[T Value] struct {
		head   *skipListNode[T] // 头节点
		length uint32           // 跳表当前长度
		height uint32           // 跳表当前高度 注意这个高度是1的时候 对应的索引是在index[0]
		//mtx    sync.RWMutex  // 读写锁 上并发保护用的
		randSource *rand.Rand
	}

	// index 具体的索引 指向节点在某一层的下一个节点
	index[T Value] struct {
		nextNode *skipListNode[T]
	}

	// skipListNode 跳表节点
	skipListNode[T Value] struct {
		key        Comparable // 键
		value      T          // 值 值可以是任何类型
		indexLevel []index[T] // 存储该节点在任意层的索引
	}
)

// NewSkipList 跳表的构造函数 默认索引晋升概率0.5 默认索引最高高度为32
func NewSkipList[T Value]() *SkipList[T] {
	return &SkipList[T]{
		length: 0,
		head: &skipListNode[T]{
			indexLevel: make([]index[T], indexMaxHeight),
		},
		height:     1,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel 为每个新加入的节点随机一个索引高度
func (sl *SkipList[T]) randomLevel() int {
	l := 1
	for sl.randSource.Float64() < indexUpgradeProbability && l < indexMaxHeight {
		l += 1
	}
	return l
}

// ToString 将跳表所存储的所有节点的键和索引高度信息转换为字符串的形式并返回 仅作为检查使用 pass
func (sl *SkipList[T]) ToString() string {
	pointer := sl.head
	result := make([]string, sl.length+1)

	for i := 1; i <= int(sl.length); i++ {
		pointer = pointer.indexLevel[0].nextNode
		result[i] = "Key " + strconv.Itoa(i) + ": " + pointer.key.String() + ", Index Height: " + strconv.Itoa(len(pointer.indexLevel))
	}
	return strings.Join(result, "\n")
}

// internalQueryNode 仅用于内部的查询指定节点的方法 如果存在则返回对应节点 如果不存在则返回空和一个error pass
func (sl *SkipList[T]) internalQueryNode(key Comparable) (result *skipListNode[T], err error) {

	//// 加读锁
	//sl.mtx.RLock()
	//defer sl.mtx.RUnlock()

	// 先找到最高的索引
	pointer := sl.head
	height := sl.height

	// 从最高的索引开始 一步一步向下开始寻找值
	var compareResult int
	for {
		if pointer.indexLevel[height-1].nextNode == nil { // 如果当前层的索引循环完成仍然找不到值 就尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1 // 索引已经到最低 无法再下降 说明找不到元素
				continue
			}
		}
		compareResult = key.Compare(pointer.indexLevel[height-1].nextNode.key)
		if compareResult < 0 { // key较小 尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1
				continue
			}
		} else if compareResult > 0 {
			//} else if strings.Compare(key, pointer.indexLevel[height-1].nextNode.key) > 0 { // key较大 按当前高度索引继续向前寻找
			pointer = pointer.indexLevel[height-1].nextNode
		} else {
			result = pointer.indexLevel[height-1].nextNode
			err = nil
			return
		}
	}
	err = errors.New("Query Key: " + key.String() + " is not Existed! \n")
	result = nil
	return
}

// queryGreaterOrEqualNode 找到第一个大于等于给定的 key 的节点
func (sl *SkipList[T]) queryGreaterOrEqualNode(key Comparable) (result *skipListNode[T], e error) {
	pointer := sl.head
	height := sl.height

	// 从最高的索引开始 一步一步向下开始寻找值
	var compareResult int
	for {
		if pointer.indexLevel[height-1].nextNode == nil { // 如果当前层的索引循环完成仍然找不到值 就尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1 // 索引已经到最低 无法再下降 说明找不到元素
				continue
			}
		}
		compareResult = key.Compare(pointer.indexLevel[height-1].nextNode.key)
		if compareResult < 0 { // key较小 尝试使索引下降一个高度
			if height == 1 {
				break
			} else {
				height -= 1
				continue
			}
		} else if compareResult > 0 { // key 较大 尝试下降索引 如果没得下降就直接返回
			if height == 1 {
				return pointer.indexLevel[height-1].nextNode, nil
			} else {
				height -= 1
				continue
			}
		} else {
			result = pointer.indexLevel[height-1].nextNode
			e = nil
			return
		}
	}
	e = errors.New("There is No Node is eligible: \n" + key.String())
	result = nil
	return
}

// QueryNode 跳表的单节点查询方法 返回给定键所对应的值 pass
func (sl *SkipList[T]) QueryNode(key Comparable) (value T, err error) {
	result, e := sl.internalQueryNode(key)
	if e != nil {
		err = e
		return
	} else {
		return result.value, nil
	}
}

// QueryNodeInterval 在[key1, key2]这个范围内进行区间查询 返回这个区间内的所有value pass
func (sl *SkipList[T]) QueryNodeInterval(key1 Comparable, key2 Comparable) (values []T, err error) {
	//if strings.Compare(key1, key2) > 0 { key1大于key2时交换 强制key1必须小于key2
	if key1.Compare(key2) > 0 {
		key1, key2 = key2, key1
	} else if key1.Compare(key2) == 0 { // key1等于key2时等于请求一个元素
		value, e := sl.QueryNode(key1)
		if e != nil {
			return nil, e
		} else {
			return []T{value}, nil
		}
	}

	node1, e := sl.queryGreaterOrEqualNode(key1) // 先找起点
	if e != nil {
		return nil, e
	}
	values = append(values, node1.value)

	//sl.mtx.RLock()

	node2 := node1.indexLevel[0].nextNode
	for node2 != nil { // 在找终点的过程中顺便添加值
		if node2.key.Compare(key2) <= 0 {
			values = append(values, node2.value)
			node2 = node2.indexLevel[0].nextNode
		} else {
			break
		}
	}
	//sl.mtx.RUnlock()
	return values, nil
}

// SetNode 修改指定节点的值
func (sl *SkipList[T]) SetNode(key Comparable, value T) (err error) {
	setNode, e := sl.internalQueryNode(key)
	if e != nil {
		return e
	} else {
		setNode.value = value
		return nil
	}
}

// DeleteNode 删除指定节点 pass
func (sl *SkipList[T]) DeleteNode(key Comparable) (err error) {

	//sl.mtx.RLock()

	update := make([]*index[T], sl.height)      // 存储要修改的各层索引
	for i := uint32(0); i <= sl.height-1; i++ { // 开始寻找各层的指向要删除的节点的索引 从最底层找起
		pointer := &sl.head.indexLevel[i]
		for pointer.nextNode != nil && pointer.nextNode.key.Compare(key) < 0 {
			pointer = &pointer.nextNode.indexLevel[i]
		}
		if i == 0 { // 只有在最下面的索引才能检查该元素是否存在
			if pointer.nextNode == nil || pointer.nextNode.key.Compare(key) != 0 {
				err = errors.New("Delete Key: " + key.String() + " is not Existed! \n")
				//sl.mtx.RUnlock()
				return
			}
		}
		if pointer.nextNode != nil && pointer.nextNode.key.Compare(key) == 0 { // 只有确定了是要删除的节点才能进行更新 上面循环完成的两种情况都要考虑 不然就会空指针
			update[i] = pointer
		}
	}
	deleteNode := update[0].nextNode // 具体要删除的节点

	//sl.mtx.RUnlock() // 寻找节点完成 开始删除
	//sl.mtx.Lock()

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
	//sl.mtx.Unlock()

	return
}

// AddNode 向跳表中添加节点 节点键值相同则为更新节点值 pass
func (sl *SkipList[T]) AddNode(key Comparable, value T) {

	// 准备阶段
	indexHeight := sl.randomLevel()
	newNode := &skipListNode[T]{
		indexLevel: make([]index[T], indexHeight),
		key:        key,
		value:      value,
	}
	update := make([]*index[T], indexHeight)
	pointer := sl.head

	// 确定update 确定每层索引插入位置
	//sl.mtx.RLock()
	var compareResult int
	for i := range update {
		pointer = sl.head
		for pointer.indexLevel[i].nextNode != nil {
			//if strings.Compare(key, pointer.indexLevel[i].nextNode.key) < 0 { // 参数s1 s2 如果结果为-1 说明s1 < s2
			compareResult = key.Compare(pointer.indexLevel[i].nextNode.key)
			if compareResult < 0 {
				update[i] = &pointer.indexLevel[i]
				break
			} else if compareResult == 0 { // 如果结果为0 说明s1 == s2
				pointer.indexLevel[i].nextNode.value = value
				return
			}
			pointer = pointer.indexLevel[i].nextNode
		}
		if update[i] == nil { // 说明插入的元素是当前高度索引中最大的
			update[i] = &pointer.indexLevel[i]
		}
	}
	//sl.mtx.RUnlock()

	// 开始插入
	//sl.mtx.Lock()
	for i := range update {
		pointer = update[i].nextNode
		update[i].nextNode = newNode
		newNode.indexLevel[i].nextNode = pointer
	}
	if uint32(indexHeight) > sl.height { // 更新索引最高高度
		sl.height = uint32(indexHeight)
	}
	sl.length += 1
	//sl.mtx.Unlock()

	return
}

func (sl *SkipList[T]) Length() int {
	return int(sl.length)
}
