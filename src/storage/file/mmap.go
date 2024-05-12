package storage

import (
	"os"
)

type MMapFile struct {
	file      *os.File
	mmapArray []byte
	fileSize  uint32
}

//func NewMMapFile(filePath string) (*MMapFile, error) {
//	file, e := os.OpenFile(filePath, os.O_CREATE | os.O_RDWR, 0644)
//	if e != nil {
//		// todo logger
//		return nil, e
//	}
//	fileInfo, e := file.Stat()
//	if e != nil {
//		// todo logger
//		return nil, e
//	}
//	mmapData, e := syscall.Mmap()
//}
//
//func (*MMapFile) Write (input []byte) error {}
//
//func (*MMapFile) Read () (result []byte, e error) {}
//
//func (*MMapFile) Sync () error {}
//
//func (*MMapFile) Delete () error {}
//
//func (*MMapFile) Close () error {}
