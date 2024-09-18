package index

import (
	"MisakaDB/storage"
	"testing"
	"time"
)

func TestZSetIndex(t *testing.T) {
	zsetIndex, e := BuildZSetIndex(nil, nil, storage.TraditionalIOFile, "D:\\", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}

	e = zsetIndex.ZAdd([]byte("111"), 1, []byte("1111"), time.Now().Add(1*time.Millisecond).UnixMilli())
	if e != nil {
		t.Error(e)
		return
	}
	//time.Sleep(1 * time.Second)
	value, e := zsetIndex.ZScore([]byte("111"), []byte("1111"))
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(value)
}
