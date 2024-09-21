//go:build linux

package file

import (
	"MisakaDB/logger"
	"io"
	"os"
	"syscall"
)

/*
Linux 的 mmap 思路总体上延续之前的 Windows 的思路

即 通过维护 fileContentSize 来确保文件不会被写多了
*/

type MMapFile struct {
	file            *os.File
	mmapArray       []byte // 从底层上 将一个数组转换而来的切片 所以严禁! 严禁! 严禁!对该切片调用 append 函数
	fileContentSize int64
	fileMaxSize     int64
	actualAddr      uintptr
}

func NewMMapFile(filePath string, MaxFileSize int64) (*MMapFile, error) {
	file, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, e
	}
	fileInfo, e := file.Stat()
	if e != nil {
		return nil, e
	}
	fileActualSize := fileInfo.Size()

	e = file.Truncate(MaxFileSize)
	if e != nil {
		return nil, e
	}

	mmapData, e := syscall.Mmap(int(file.Fd()), 0, int(MaxFileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	result := &MMapFile{
		file:            file,
		mmapArray:       mmapData,
		fileContentSize: fileActualSize,
		fileMaxSize:     MaxFileSize,
	}

	return result, nil
}

func (mf *MMapFile) Write(input []byte, offset int) error {
	if len(input)+offset > int(mf.fileMaxSize) {
		return io.EOF
	}
	if int64(offset) > mf.fileContentSize {
		return logger.OffsetIsIllegal
	}
	i := 0
	for i < len(input) {
		mf.mmapArray[i+offset] = input[i]
		i += 1
	}
	if int64(offset) < mf.fileContentSize {
		mf.fileContentSize += int64(offset+len(input)) - mf.fileContentSize
	} else {
		mf.fileContentSize += int64(len(input))
	}
	return nil
}

func (mf *MMapFile) Read(buf []byte, offset int) error {
	i := 0
	for i+offset < int(mf.fileMaxSize) && i < len(buf) {
		buf[i] = mf.mmapArray[i+offset]
		i += 1
	}
	if i < len(buf) {
		return io.EOF
	}
	return nil
}

func (mf *MMapFile) Sync() error {
	_, _, e := syscall.Syscall(syscall.SYS_MSYNC, uintptr(mf.mmapArray[0]), uintptr(mf.fileMaxSize), syscall.MS_SYNC)
	return e
}

func (mf *MMapFile) Delete() error {
	e := mf.Close()
	if e != nil {
		return e
	}
	e = os.Remove(mf.file.Name())
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), mf.file.Name())
		return e
	}
	return nil
}

func (mf *MMapFile) Close() error {
	e := syscall.Munmap(mf.mmapArray)
	if e != nil {
		return e
	}
	e = mf.file.Truncate(mf.fileContentSize)
	if e != nil {
		return e
	}
	e = mf.file.Close()
	if e != nil {
		return e
	}
	return nil
}

func (mf *MMapFile) Length() (int64, error) {
	return mf.fileContentSize, nil
}
