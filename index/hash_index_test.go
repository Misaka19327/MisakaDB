package index

import (
	"MisakaDB/logger"
	"MisakaDB/storage"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"
)

func TestBuildHashIndex(t *testing.T) {
	//activeFiles, archiveFiles, e := storage.RecordFilesInit("D:\\MisakaDBTest", 65536)
	//if e != nil {
	//	t.Error(e)
	//	return
	//}

	rand.Int()
	hashIndex, e := BuildHashIndex(nil, nil, storage.TraditionalIOFile, "D:\\", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}
	e = hashIndex.HSet("testKey1", "testField1", "testValue1", 32503637532)
	if e != nil {
		t.Error(e)
		return
	}
	value, e := hashIndex.HGet("testKey1", "testField1")
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(value)
}

func TestBuildHashIndex2(t *testing.T) {
	activeFiles, archiveFiles, e := storage.RecordFilesInit("D:\\MisakaDBTest", 65536, storage.MMapIOFile)
	if e != nil {
		t.Error(e)
		return
	}
	hashIndex, e := BuildHashIndex(activeFiles[storage.Hash], archiveFiles[storage.Hash], storage.TraditionalIOFile, "D:\\MisakaDBTest", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}
	_ = hashIndex.HSet("testKey1", "testField1", "testValue1", -1)
	value, e := hashIndex.HGet("testKey1", "testField1")
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(value)
}

func TestBuildHashIndex3(t *testing.T) {
	activeFiles, archiveFiles, e := storage.RecordFilesInit("D:\\MisakaDBTest", 65536, storage.MMapIOFile)
	if e != nil {
		t.Error(e)
		return
	}
	hashIndex, e := BuildHashIndex(activeFiles[storage.Hash], archiveFiles[storage.Hash], storage.TraditionalIOFile, "D:\\MisakaDBTest", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}
	e = hashIndex.HDel("testKey1", "testField1", true)
	if e != nil {
		t.Error(e)
		return
	}
	_, e = hashIndex.HGet("testKey1", "testField1")
	if e != nil {
		t.Error(e)
		return
	}
}

func TestBuildHashIndex4(t *testing.T) {
	activeFiles, archiveFiles, e := storage.RecordFilesInit("D:\\MisakaDBTest", 65536, storage.MMapIOFile)
	if e != nil {
		t.Error(e)
		return
	}
	hashIndex, e := BuildHashIndex(activeFiles[storage.Hash], archiveFiles[storage.Hash], storage.TraditionalIOFile, "D:\\MisakaDBTest", 65536, time.Second)
	if e != nil {
		t.Error(e)
		return
	}
	e = hashIndex.HSet("testKey1", "testField1", "testValue1", 32503637532)
	if e != nil {
		t.Error(e)
		return
	}
	value, e := hashIndex.HExist("testKey1", "testField1")
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(value)
	v, e := hashIndex.HLen("testKey1")
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(v)
	va, e := hashIndex.HStrLen("testKey1", "testField1")
	if e != nil {
		t.Error(e)
		return
	}
	t.Log(va)
}

func TestBuildHashIndex5(t *testing.T) {
	l, e := logger.NewLogger("/home/MisakaDBLog")
	if e != nil {
		t.Fatal(e)
	}
	l.ListenLoggerChannel()

	startTime := time.Now()
	activeFiles, archiveFiles, e := storage.RecordFilesInit("/home/MisakaDB", 10983040, storage.MMapIOFile)
	if e != nil {
		t.Error(e)
		return
	}
	hashIndex, e := BuildHashIndex(activeFiles[storage.Hash], archiveFiles[storage.Hash], storage.MMapIOFile, "/home/MisakaDB", 10983040, time.Second)
	if e != nil {
		t.Error(e)
		return
	}

	endTime := time.Now()
	t.Log(endTime.Sub(startTime).Seconds())

	testData := make(map[string]map[string]string)
	for i := 0; i < 1000; i++ {
		testData["testKey"+strconv.Itoa(i)] = make(map[string]string)
		for j := 0; j < 10000; j++ {
			testData["testKey"+strconv.Itoa(i)]["testField"+strconv.Itoa(j)] = "testValue" + strconv.Itoa(j)
		}
	}

	t.Log("Test Data is Ready!")

	startTime = time.Now()
	for key, fieldMap := range testData {
		for field, value := range fieldMap {
			e = hashIndex.HSet(key, field, value, time.Now().Add(10*time.Minute).UnixMilli())
			if e != nil {
				t.Error(e)
				return
			}
		}
	}

	endTime = time.Now()
	t.Log(endTime.Sub(startTime).Seconds())
	startTime = time.Now()

	var getValue string
	count := 0
	for key, fieldMap := range testData {
		for field, value := range fieldMap {
			getValue, e = hashIndex.HGet(key, field)
			if e != nil {
				t.Error(e)
				return
			}
			if getValue != value {
				t.Log(value + "---" + getValue)
				count += 1
			}
		}
	}
	endTime = time.Now()
	t.Log(endTime.Sub(startTime).Seconds())

	t.Log(count)

	_ = hashIndex.CloseIndex()
}
