package index

import (
	"MisakaDB/storage"
	"fmt"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	e := time.Now().Add(2 * time.Second).UnixMilli()
	time.Sleep(time.Until(time.UnixMilli(e)))

	a := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	a = append(a, 1)
	fmt.Println(a)
}

func TestListIndex(t *testing.T) {
	listIndex, e := BuildListIndex(nil, nil, storage.TraditionalIOFile, "D:\\", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}

	e = listIndex.LPush([]byte("111"), time.Now().Add(998*time.Millisecond).UnixMilli(), []byte("1111"))
	if e != nil {
		t.Error(e)
		return
	}
	time.Sleep(1 * time.Second)
	value, e := listIndex.LPop([]byte("111"))
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(value)
	t.Log(value == nil)
}
