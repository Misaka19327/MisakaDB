package file

import (
	"MisakaDB/logger"
	"MisakaDB/util"
	"os"
	"strconv"
)

type FileIO struct {
	file *os.File
}

// NewFileIO 以传统IO的方式开始读写文件
func NewFileIO(filePath string) (*FileIO, error) {
	file, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		logger.GenerateErrorLog(false, true, e.Error(), filePath)
		return nil, e
	}
	result := &FileIO{
		file: file,
	}
	return result, nil
}

// Write 在文件的指定位置进行写入
func (f *FileIO) Write(input []byte, offset int) error {
	_, e := f.file.WriteAt(input, int64(offset))
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), util.TurnByteArrayToString(input), strconv.Itoa(offset), f.file.Name())
	}
	return e
}

// Read 在文件的指定位置进行读取
func (f *FileIO) Read(buf []byte, offset int) error {
	_, e := f.file.ReadAt(buf, int64(offset))
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), strconv.Itoa(offset), f.file.Name())
	}
	return e
}

// Sync 强制内存和文件同步一次 以保证一致性
func (f *FileIO) Sync() error {
	e := f.file.Sync()
	if e != nil {
		logger.GenerateErrorLog(false, true, e.Error(), f.file.Name())
	}
	return e
}

// Delete 删除文件
func (f *FileIO) Delete() error {
	e := f.Close()
	if e != nil {
		return e
	}
	e = os.Remove(f.file.Name())
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), f.file.Name())
		return e
	}
	return nil
	// attention *os.File.Remove()只会报PathError 其他的问题（如权限不足）导致删除文件不成功的话它报不了错的
	// attention *os.File.Name()返回的是当初创建文件时给定的字符串 不论这个字符串是文件绝对路径还是只是文件名
}

// Close 关闭文件
func (f *FileIO) Close() error {
	e := f.file.Close()
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), f.file.Name())
	}
	return e
}

// Length 返回该文件的长度
func (f *FileIO) Length() (int64, error) {
	fileStat, e := f.file.Stat()
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), f.file.Name())
		return 0, e
	}
	return fileStat.Size(), nil
}

// 检查接口实现
var _ FileWriter = (*FileIO)(nil)
