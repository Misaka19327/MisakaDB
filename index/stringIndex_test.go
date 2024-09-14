package index

import (
	"MisakaDB/logger"
	"MisakaDB/storage"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestBuildStringIndex(t *testing.T) {
	l, _ := logger.NewLogger("D:\\MisakaDBLog")
	l.ListenLoggerChannel()

	startTime := time.Now()
	activeFiles, archiveFiles, e := storage.RecordFilesInit("D:\\MisakaDBTest", 50000000)
	if e != nil {
		t.Error(e)
		return
	}
	hashIndex, e := BuildStringIndex(activeFiles[storage.String], archiveFiles[storage.String], storage.TraditionalIOFile, "D:\\MisakaDBTest", 50000000, time.Second)
	if e != nil {
		t.Error(e)
		return
	}

	endTime := time.Now()
	t.Log(endTime.Sub(startTime).Seconds())

	testData := make(map[string]string)
	for i := 0; i < 100000; i++ {
		testData["testKey"+strconv.Itoa(i)] = "testValue" + strconv.Itoa(i)
	}

	file, e := os.OpenFile("D:\\1.txt", os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		t.Log(e)
		return
	}

	t.Log("Test Data is Ready!")
	for key, value := range testData {
		startTime = time.Now()
		e = hashIndex.Set(key, value, 32503637532)
		endTime = time.Now()
		_, e = file.Write([]byte(strconv.Itoa(int(endTime.Sub(startTime).Microseconds()))))
		_, e = file.Write([]byte(" "))
		if e != nil {
			t.Error(e)
			return
		}
	}

	startTime = time.Now()

	var getValue string
	count := 0
	for key, value := range testData {
		getValue, e = hashIndex.Get(key)
		if e != nil {
			t.Error(e)
			return
		}
		if getValue != value {
			t.Log(value + "---" + getValue)
			count += 1
		}
	}
	endTime = time.Now()
	t.Log(endTime.Sub(startTime).Seconds())

	t.Log(count)
}
