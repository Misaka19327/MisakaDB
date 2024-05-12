package main

import (
	"MisakaDB/src/storage"
	"fmt"
)

func main() {
	testFile, e := storage.NewRecordFile(storage.TraditionalIOFile, storage.String, 1, "D:\\MisakaDBTest", 65536)
	if e != nil {
		fmt.Println(e)
		return
	}
	testEntry := &storage.Entry{
		Key:       []byte("testKey"),
		Value:     []byte("testValue"),
		EntryType: storage.TypeRecord,
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

	testEntry = &storage.Entry{
		Key:       []byte("testKey----2"),
		Value:     []byte("testValue----2"),
		EntryType: storage.TypeRecord,
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

	testEntry = &storage.Entry{
		Key:       []byte("Key3"),
		Value:     []byte("Value3"),
		EntryType: storage.TypeRecord,
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
