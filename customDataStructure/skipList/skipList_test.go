package skipList

import (
	"strconv"
	"strings"
	"testing"
)

func TestSkipList_AddNode(t *testing.T) {
	time := 2000

	sl := NewSkipList()
	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		}
	}
	t.Log("add node is completed")
	s := sl.ToString()
	t.Log(s)
}

func TestSkipList_QueryNode(t *testing.T) {
	time := 40000

	sl := NewSkipList()
	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		}
	}

	t.Log("Write Completed")

	//for i := 0; i < time+5; i++ {
	//	queryValue, e := sl.QueryNode(strconv.Itoa(i))
	//	if e != nil {
	//		t.Log(e.Error())
	//		continue
	//	}
	//	if *queryValue.(*string) != "TestValue"+strconv.Itoa(i) {
	//		t.Errorf("Error :%d %s", i, *queryValue.(*string))
	//	} else {
	//		t.Log("Success: " + strconv.Itoa(i))
	//	}
	//}
}

func TestSkipList_DeleteNode(t *testing.T) {
	time := 2000

	sl := NewSkipList()
	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		}
	}

	for i := 0; i < time+5; i++ {
		err := sl.DeleteNode(strconv.Itoa(i))
		_, err2 := sl.QueryNode(strconv.Itoa(i))
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

func temp(p []any) []string {
	result := make([]string, len(p))
	for i, v := range p {
		result[i] = *(v.(*string))
	}
	return result
}

func TestSkipList_QueryNodeInterval(t *testing.T) {
	time := 2000

	sl := NewSkipList()
	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		}
	}

	for i := 0; i < time; i++ {
		value, err := sl.QueryNodeInterval(strconv.Itoa(i), strconv.Itoa(i+5))
		if err != nil {
			t.Log(err)
		} else {
			t.Log("Success: " + strings.Join(temp(value), ", "))
		}
	}
}

func TestSkipList_SetNode(t *testing.T) {
	time := 2000

	sl := NewSkipList()
	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i)
		err := sl.AddNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		}
	}

	t.Log(sl.ToString())

	for i := 0; i < time; i++ {
		testValue := "TestValue" + strconv.Itoa(i+5)
		err := sl.SetNode(strconv.Itoa(i), &testValue)
		if err != nil {
			t.Log(err)
		} else {
			result, e := sl.QueryNode(strconv.Itoa(i))
			if e == nil {
				t.Log("Success: " + strconv.Itoa(i) + ", Value: " + *result.(*string))
			} else {
				t.Log("Fail: " + e.Error())
			}
		}
	}
}
