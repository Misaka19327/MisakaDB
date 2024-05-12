package storage

import "os"

type FileIO struct {
	file *os.File
}

func NewFileIO(filePath string) (*FileIO, error) {
	file, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		// todo logger
		return nil, e
	}
	result := &FileIO{
		file: file,
	}
	return result, nil
}

func (f *FileIO) Write(input []byte, offset int) error {
	_, e := f.file.WriteAt(input, int64(offset))
	return e
}

func (f *FileIO) Read(buf []byte, offset int) error {
	_, e := f.file.ReadAt(buf, int64(offset))
	return e
}

func (f *FileIO) Sync() error {
	return f.file.Sync()
}

func (f *FileIO) Delete() error {
	e := f.file.Close()
	if e == nil {
		// todo logger
		return e
	}
	return os.Remove(f.file.Name())
	// attention *os.File.Remove()只会报PathError 其他的问题（如权限不足）导致删除文件不成功的话它报不了错的
	// attention *os.File.Name()返回的是当初创建文件时给定的字符串 不论这个字符串是文件绝对路径还是只是文件名
}

func (f *FileIO) Close() error {
	return f.file.Close()
}

func (f *FileIO) Length() (int64, error) {
	fileStat, e := f.file.Stat()
	if e != nil {
		return 0, e
	}
	return fileStat.Size(), nil
}

// 检查接口实现
var _ FileWriter = (*FileIO)(nil)
