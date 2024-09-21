// go:build linux

package file

type MMapFile struct {
	file      *os.File
	mmapArray []byte
	fileSize  uint32
}

func NewMMapFile(filePath string) (*MMapFile, error) {
	file, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, e
	}
	fileInfo, e := file.Stat()
	if e != nil {
		return nil, e
	}
	mmapData, e := syscall.Mmap()
}

func (M *MMapFile) Write(input []byte, offset int) error {
	//TODO implement me
	panic("implement me")
}

func (M *MMapFile) Read(buf []byte, offset int) error {
	//TODO implement me
	panic("implement me")
}

func (M *MMapFile) Sync() error {
	//TODO implement me
	panic("implement me")
}

func (M *MMapFile) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (M *MMapFile) Close() error {
	//TODO implement me
	panic("implement me")
}

func (M *MMapFile) Length() (int64, error) {
	//TODO implement me
	panic("implement me")
}
