package skipList

import (
	"bufio"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
)

type testValue struct {
	s string
}

func (t testValue) String() string {
	return t.s
}

func TestSkipList_AddNode(t *testing.T) {
	time := 2000

	sl := NewSkipList[testValue]()
	for i := 0; i < time; i++ {
		testV := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			t.Log(err)
		}
	}
	t.Log("add node is completed")
	s := sl.ToString()
	t.Log(s)
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

func TestSkipList_QueryNode(t *testing.T) {
	f, _ := os.OpenFile("mem.profile", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()

	words := loadTestFile("testWord/words.txt")

	sl := NewSkipList[testValue]()
	for i := 0; i < len(words); i++ {
		err := sl.AddNode(Key(words[i]), testValue{string(words[i])})
		if err != nil {
			t.Log(err)
		}
	}

	t.Log("Write Completed")

	errCount := 0
	for i := 0; i < len(words); i++ {
		queryValue, e := sl.QueryNode(Key(words[i]))
		if e != nil {
			errCount += 1
			t.Log(e.Error())
			continue
		}
		if queryValue.s != string(words[i]) {
			t.Errorf("Error :%d %s", i, queryValue.s)
			errCount += 1
		} else {
			t.Log("Success: " + string(words[i]))
		}
	}
	t.Log(errCount)

	pprof.Lookup("heap").WriteTo(f, 0)
}

func TestSkipList_DeleteNode(t *testing.T) {
	time := 2000

	sl := NewSkipList[testValue]()
	for i := 0; i < time; i++ {
		testV := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			t.Log(err)
		}
	}

	for i := 0; i < time+5; i++ {
		err := sl.DeleteNode(Key(strconv.Itoa(i)))
		_, err2 := sl.QueryNode(Key(strconv.Itoa(i)))
		if err2 != nil {
			t.Log(err2)
		}
		if err != nil {
			t.Log(err)
		} else {
			t.Log("Success: " + strconv.Itoa(i))
		}
	}
}

func temp(p []testValue) []string {
	result := make([]string, len(p))
	for i, v := range p {
		result[i] = v.String()
	}
	return result
}

func TestSkipList_QueryNodeInterval(t *testing.T) {
	time := 2000

	sl := NewSkipList[testValue]()
	for i := 0; i < time; i++ {
		testV := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			t.Log(err)
		}
	}

	for i := 0; i < time; i++ {
		value, err := sl.QueryNodeInterval(Key(strconv.Itoa(i)), Key(strconv.Itoa(i+5)))
		if err != nil {
			t.Log(err)
		} else {
			t.Log("Success: " + strings.Join(temp(value), ", "))
		}
	}
}

func TestSkipList_SetNode(t *testing.T) {
	//f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	//defer f.Close()
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	f, _ := os.OpenFile("mem.profile", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()

	time := 20000

	sl := NewSkipList[testValue]()
	for i := 0; i < time; i++ {
		testV := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			t.Log(err)
		}
	}

	//t.Log(sl.ToString())

	for i := 0; i < time; i++ {
		testV := "TestValue" + strconv.Itoa(i+5)
		err := sl.SetNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			t.Log(err)
		} else {
			result, e := sl.QueryNode(Key(strconv.Itoa(i)))
			if e == nil {
				t.Log("Success: " + strconv.Itoa(i) + ", Value: " + result.s)
			} else {
				t.Log("Fail: " + e.Error())
			}
		}
	}

	pprof.Lookup("heap").WriteTo(f, 0)
}

func BenchmarkSkipList_SetNode(b *testing.B) {
	//time := 2000

	sl := NewSkipList[testValue]()
	for i := 0; i < b.N; i++ {
		testV := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			b.Log(err)
		}
	}

	b.ResetTimer()

	//t.Log(sl.ToString())

	for i := 0; i < b.N; i++ {
		testV := "TestValue" + strconv.Itoa(i+5)
		err := sl.SetNode(Key(strconv.Itoa(i)), testValue{testV})
		if err != nil {
			b.Log(err)
		} else {
			_, e := sl.QueryNode(Key(strconv.Itoa(i)))
			if e == nil {
				//b.Log("Success: " + strconv.Itoa(i) + ", Value: " + result.s)
			} else {
				//b.Log("Fail: " + e.Error())
			}
		}
	}
}
