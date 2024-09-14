package adaptiveRadixTree

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

type testValueType string

func (t testValueType) String() string {
	return string(t)
}

func TestBitCalc(t *testing.T) {
	//a := uint16(7) // 0111
	//a &= ^uint16(5) // 5 -> 0101
	//fmt.Println(a)

	fmt.Println(540431955284459520 & (1 << 56))
	fmt.Println(13 & 65534)
	fmt.Println(^uint16(1))
}

//func TestNode4Add(t *testing.T) {
//	node := newNode16[testValueType]()
//	node.addChild16(33, true, newNode4[testValueType]())
//	node.addChild16(66, true, newNode4[testValueType]())
//	node.addChild16(99, true, newNode4[testValueType]())
//	node.addChild16(102, true, newNode4[testValueType]())
//	node.addChild16(100, true, newNode4[testValueType]())
//
//	fmt.Println(fmt.Sprintf("%b", node.node16().isExist))
//}
//
//func TestRemoveNode4(t *testing.T) {
//	artN := newNode16[*test]()
//	n := artN.node16()
//	artN.addChild(55, true, newNode4[*test]())
//	artN.addChild(56, true, newNode4[*test]())
//	artN.addChild(57, true, newNode4[*test]())
//	artN.addChild(58, true, newNode4[*test]())
//	artN.addChild(59, true, newNode4[*test]())
//	artN.addChild(60, true, newNode4[*test]())
//
//	t.Log(n.isExist)
//
//	artN.removeChild(59, true)
//	//t.Log(artN.node16().isExist)
//
//	artN.removeChild(59, true)
//	t.Log(artN.node4().keys)
//	artN.removeChild(58, true)
//	t.Log(artN.node4().keys)
//	artN.removeChild(57, true)
//	t.Log(artN.node4().keys)
//}
//
//func TestShrink(t *testing.T) {
//	artN := newNode48[*test]()
//	//n := artN.node16()
//	artN.addChild(55, true, newNode4[*test]())
//	artN.addChild(56, true, newNode4[*test]())
//	artN.addChild(57, true, newNode4[*test]())
//	artN.addChild(58, true, newNode4[*test]())
//	t.Log(artN.node48().keys[55:59])
//	t.Log(artN.node48().isExist.string())
//
//	n4 := artN.shrink()
//
//	t.Log(n4.node16().keys)
//}

type test struct {
	id int
}

func (t test) String() string {
	return strconv.Itoa(t.id)
}

func TestTreeSearch(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 1000000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
		//t.Log(i)
	}
	//fmt.Println(tr.(*tree[*test]).String())
	t.Log("Start Search")
	t.Log(fmt.Sprintf("size: %d", tr.Size()))
	t.Log(fmt.Sprintf("map size: %d", len(testMap)))
	//t.Log(tr.(*tree[*test]).String())
	count := 0
	var result *test
	var isSearched bool
	//var isDeleted bool
	for k, v := range testMap {
		//if v.id % 2 != 0 {
		//	result, isDeleted = tr.Delete([]byte(k))
		//	if isDeleted {
		//		result, isSearched = tr.Search([]byte(k))
		//		if isSearched {
		//			count += 1
		//			t.Log(result.id)
		//		} else {
		//			continue
		//		}
		//	} else {
		//		t.Log(v.id)
		//		count += 1
		//	}
		//} else {
		result, isSearched = tr.Search([]byte(k))
		if isSearched {
			if result.id == v.id {
				continue
			}
		}
		t.Log(v.id)
		count += 1
		//}
	}
	t.Log(count)
	t.Log(tr.Size())
}

func TestTree2(t *testing.T) {
	tr := New[*test]()
	tr.Insert([]byte("appleappleapp"), &test{id: 1})
	tr.Insert([]byte("appleappleapen"), &test{id: 2})
	//tr.Delete([]byte("apple"))
	result, _ := tr.Search([]byte("appleappleapen"))
	t.Log(result.id)
	result, _ = tr.Search([]byte("appleappleapp"))
	t.Log(result.id)
	t.Log(tr.(*tree[*test]).String())
	tr.Delete([]byte("appleappleapen"))
	t.Log(tr.(*tree[*test]).String())
}

func TestTreeOrigin(t *testing.T) {
	tr := New[*test]()
	tr.Insert([]byte("appleappleapp"), &test{id: 1})
	tr.Insert([]byte("appleappleapen"), &test{id: 2})
	//tr.Delete([]byte("apple"))
	result, _ := tr.Search([]byte("appleappleapen"))
	t.Log(result.id)
	result, _ = tr.Search([]byte("appleappleapp"))
	t.Log(result.id)
	t.Log(tr.(*tree[*test]).String())
	tr.Delete([]byte("appleappleapen"))
	t.Log(tr.(*tree[*test]).String())
}

func TestTreeDelete(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 100000)
	for i := 0; i < 1000000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
		//t.Log(i)
	}
	//fmt.Println(tr.(*tree).String())
	t.Log("Start Deleted")
	t.Log(fmt.Sprintf("size: %d", tr.Size()))
	t.Log(fmt.Sprintf("map size: %d", len(testMap)))
	//t.Log(tr.(*tree[*test]).String())
	count := 0
	var result *test
	var isSearched bool
	var isDeleted bool
	var a string
	for k, v := range testMap {
		result, isDeleted = tr.Delete([]byte(k))
		if isDeleted {
			result, isSearched = tr.Search([]byte(k))
			if isSearched {
				count += 1
				t.Log(result.id)
			} else {
				continue
			}
		} else {
			t.Log(v.id)
			result, isDeleted = tr.Delete([]byte(a))
			a = k
			count += 1
		}
	}
	t.Log(count)
	t.Log(tr.Size())
	t.Log(tr.(*tree[*test]).String())
}

func TestBitmap(t *testing.T) {
	b := newBitmap(4)
	b.set1(2)
	b.set1(1)
	b.set1(3)
	b.set1(4)
	fmt.Println(b.getNum())
	//b.set1(6)
	//b.set1(9)
	//b.set1(10)
	//b.set1(12)
	//b.set1(3)
	//b.set1(96)
	//b.set1(244)
	//b.set1(155)
	//b.set1(157)
	//b.set1(150)
	//b.set1(148)
	fmt.Println(b.get(2))
	fmt.Println(b.string())
	b.set0(1)
	fmt.Println(b.get(1))
	fmt.Println(b.string())
}

func TestTreeSearchAndDelete(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 1000000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
	}
	//fmt.Println(tr.(*tree[*test]).String())
	t.Log("Start Search")
	t.Log(fmt.Sprintf("size: %d", tr.Size()))
	t.Log(fmt.Sprintf("map size: %d", len(testMap)))
	//t.Log(tr.(*tree[*test]).String())
	count := 0
	var result *test
	var isSearched bool
	var isDeleted bool
	for k, v := range testMap {
		if v.id%2 != 0 {
			result, isDeleted = tr.Delete([]byte(k))
			if isDeleted {
				result, isSearched = tr.Search([]byte(k))
				if isSearched {
					count += 1
					t.Log(result.id)
				} else {
					continue
				}
			} else {
				t.Log(v.id)
				count += 1
			}
		} else {
			result, isSearched = tr.Search([]byte(k))
			if isSearched {
				if result.id == v.id {
					continue
				}
			}
			t.Log(v.id)
			count += 1
		}
	}
	t.Log(count)
	t.Log(tr.Size())
}

func TestTreeSearchAndDeleteRandom(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 1000000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
	}
	t.Log(tr.Size())

	var operate int
	var result *test
	var isSearched bool
	var isDeleted bool
	var errCount = 0
	for i := 0; i < 10000000; i++ {
		operate = rand.New(rand.NewSource(time.Now().UnixNano())).Intn(3)
		switch operate {
		case 0:
			k := uuid.NewString()
			testMap[k] = &test{
				id: i,
			}
			tr.Insert([]byte(k), &test{id: i})
		case 1:
			for k := range testMap {
				result, isSearched = tr.Search([]byte(k))
				if isSearched {
					if result.id == testMap[k].id {
						break
					}
				}
				errCount += 1
				break
			}
		case 2:
			for k := range testMap {
				result, isDeleted = tr.Delete([]byte(k))
				if isDeleted {
					if result.id == testMap[k].id {
						delete(testMap, k)
						break
					}
				}
				errCount += 1
				break
			}
		}
	}
	t.Log(tr.Size())
	t.Log(errCount)
}

func TestForEach(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 1000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
	}
	t.Log(tr.Size())

	tr.ForEach(func(node Node[*test]) (isContinue bool) {
		fmt.Println(string(node.Key()))
		return true
	})
}

func TestTree_ForEachWithPrefix(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 1000000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
	}
	t.Log(tr.Size())
	tr.Insert([]byte("ffaaa"), &test{1})
	tr.ForEachWithPrefix([]byte("fabcd"), func(node Node[*test]) (isContinue bool) {
		fmt.Println(string(node.Key()))
		return true
	})
}

func TestIterator(t *testing.T) {
	tr := New[*test]()
	testMap := make(map[string]*test, 10000)
	for i := 0; i < 100000; i++ {
		k := uuid.NewString()
		testMap[k] = &test{
			id: i,
		}
		tr.Insert([]byte(k), &test{id: i})
	}
	t.Log(tr.Size())

	count := 0
	i := tr.Iterator()
	//var result Node[*test]
	var e error
	for i.HasNext() {
		_, e = i.Next()
		if e == nil {
			//t.Log(result.Key())
			count += 1
		}
	}
	t.Log(count)
}

func (k Key) String() string {
	return string(k)
}

func TestName(t *testing.T) {
	tr := New[Key]()
	keys := []Key{
		Key("test/a1"),
		Key("test/a2"),
		Key("test/a3"),
		Key("test/a4"),
		// This should become zeroChild
		Key("test/a"),
	}

	for _, w := range keys {
		tr.Insert(w, w)
	}

	t.Log(tr.(*tree[Key]).String())

	for _, w := range keys {
		// Fail!!!
		tr.Delete(w)
		t.Log(tr.(*tree[Key]).String())
	}
}

func loadTestFile(path string) [][]byte {
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	var words [][]byte
	reader := bufio.NewReader(file)
	for {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				words = append(words, line[:len(line)-1])
			}
		}
	}
	return words
}

type test2 struct {
	b string
}

func (t *test2) String() string {
	return t.b
}

func TestDict(t *testing.T) {
	words := loadTestFile("testWord/words.txt")
	tr := New[*test2]()
	//words = words[:233000]
	for _, w := range words {
		tr.Insert(w, &test2{string(w)})
	}
	t.Log(tr.Size())
	count := 0
	for _, w := range words {
		result, isSearched := tr.Search(w)
		if isSearched {
			if result.b == string(w) {
				continue
			}
		}
		count += 1
		t.Error(result)
		t.Error(string(w))
	}
	t.Log("search error count: " + strconv.Itoa(count))
	//str := tr.(*tree[*test2]).String()
	//t.Log(str[:62300])
	//t.Log(DumpNode[*test2](tr.(*tree[*test2]).root.node16().children[1]))
	//temp := tr.(*tree[*test2]).root.node16().children[1].node256()
	//fmt.Sprintf("%T", temp)

	count = 0
	var result *test2
	var isSearched bool
	var isDeleted bool
	for _, w := range words {
		result, isDeleted = tr.Delete(w)
		if isDeleted && result.b == string(w) {
			result, isSearched = tr.Search(w)
			if isSearched {
				count += 1
				t.Log(result.b)
			} else {
				continue
			}
		} else {
			count += 1
		}
	}
	t.Log("delete error count: " + strconv.Itoa(count))
	t.Log("tree size: " + strconv.Itoa(tr.Size()))
	//str = tr.(*tree[*test2]).String()
	//t.Log(str[:6230])
}

func TestDictIterator(t *testing.T) {
	words := loadTestFile("testWord/hsk_words.txt")
	tr := New[*test2]()
	for _, w := range words {
		tr.Insert(w, &test2{string(w)})
	}
	t.Log(tr.Size())
	count := 0
	it := tr.Iterator()
	for it.HasNext() {
		_, e := it.Next()
		if e == nil {
			//t.Log(result.Value().b)
			count += 1
		}
	}
	t.Log(count)
}

func TestDictForEach(t *testing.T) {
	words := loadTestFile("testWord/hsk_words.txt")
	tr := New[*test2]()
	for _, w := range words {
		tr.Insert(w, &test2{string(w)})
	}
	t.Log(tr.Size())
	count := 0
	tr.ForEach(func(node Node[*test2]) (isContinue bool) {
		count += 1
		return true
	})
	t.Log(count)
}

func BenchmarkWordsTreeSearch(b *testing.B) {
	words := loadTestFile("testWord/hsk_words.txt")
	tree := New[*test]()
	for _, w := range words {
		tree.Insert(w, nil)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			tree.Search(w)
		}
	}
}
