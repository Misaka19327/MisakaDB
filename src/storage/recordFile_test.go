package storage

import (
	"fmt"
	"testing"
)

func TestNewRecordFile(t *testing.T) {
	testFile, e := NewRecordFile(TraditionalIOFile, String, 1, "D:\\MisakaDBTest", 65536)
	if e != nil {
		fmt.Println(e)
		return
	}
	testEntry := &Entry{
		Key:       []byte("testKey"),
		Value:     []byte("testValue"),
		EntryType: TypeRecord,
		ExpiredAt: 0,
	}
	e = testFile.WriteEntryIntoFile(testEntry)
	if e != nil {
		fmt.Println(e)
		return
	}
	e = testFile.Sync()
	if e != nil {
		fmt.Println(e)
		return
	}
	testReadEntry, length, e := testFile.ReadIntoEntry(0)
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println(string(testReadEntry.Key))
	fmt.Println(string(testReadEntry.Value))
	fmt.Println(length)

	testEntry = &Entry{
		Key:       []byte("testKey----2"),
		Value:     []byte("testValue----2"),
		EntryType: TypeRecord,
		ExpiredAt: 0,
	}
	e = testFile.WriteEntryIntoFile(testEntry)
	if e != nil {
		fmt.Println(e)
		return
	}
	e = testFile.Sync()
	if e != nil {
		fmt.Println(e)
		return
	}
	testReadEntry, length, e = testFile.ReadIntoEntry(25)
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println(string(testReadEntry.Key))
	fmt.Println(string(testReadEntry.Value))
	fmt.Println(length)

	testEntry = &Entry{
		Key:       []byte("Key3"),
		Value:     []byte("Value3"),
		EntryType: TypeRecord,
		ExpiredAt: 0,
	}
	e = testFile.WriteEntryIntoFile(testEntry)
	if e != nil {
		fmt.Println(e)
		return
	}
	e = testFile.Sync()
	if e != nil {
		fmt.Println(e)
		return
	}
	testReadEntry, length, e = testFile.ReadIntoEntry(59)
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println(string(testReadEntry.Key))
	fmt.Println(string(testReadEntry.Value))
	fmt.Println(length)

	e = testFile.Close()
	if e != nil {
		fmt.Println(e)
		return
	}
}

func TestParseFileName(t *testing.T) {
	result1, result2, e := parseFileName("record.string.000000001.misaka")
	if e != nil {
		t.Log(e)
	} else {
		t.Log(result1)
		t.Log(result2)
	}
}

func TestLoadRecordFileFromDisk(t *testing.T) {
	rf, e := LoadRecordFileFromDisk("D:\\MisakaDBTest\\record.string.000000001.misaka", 65536)
	if e != nil {
		t.Log(e)
		return
	}
	t.Log(rf.newestOffset)

	testEntry := &Entry{
		Key:       []byte("testKey5"),
		Value:     []byte("testValue5"),
		EntryType: TypeRecord,
		ExpiredAt: 0,
	}
	e = rf.WriteEntryIntoFile(testEntry)
	if e != nil {
		fmt.Println(e)
		return
	}
	e = rf.Sync()
	if e != nil {
		fmt.Println(e)
		return
	}

	testReadEntry, length, e := rf.ReadIntoEntry(84)
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println(string(testReadEntry.Key))
	fmt.Println(string(testReadEntry.Value))
	fmt.Println(length)

	testReadEntry, length, e = rf.ReadIntoEntry(59)
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println(string(testReadEntry.Key))
	fmt.Println(string(testReadEntry.Value))
	fmt.Println(length)

	fmt.Println(rf.newestOffset)
}
