package storage

import (
	"os"
	"path/filepath"
)

type Storage struct {
	ActiveFiles   map[FileForData]*RecordFile            // 当前可以写入的活跃文件
	ArchivedFiles map[FileForData]map[uint32]*RecordFile // 已经归档的文件 那个uint32是给fileID用的

}

// LoadRecordFiles 加载所有文件到Storage中
// todo 转移到数据库操作中 不在存储这一层进行调用 取消Storage类型
func LoadRecordFiles(path string, fileMaxSize int64) (*Storage, error) {
	var filesPath []string
	var walkFunc = func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			filesPath = append(filesPath, path)
		}
		return nil
	}
	e := filepath.Walk(path, walkFunc)
	if e != nil {
		return nil, e
	}
	var recordFile *RecordFile
	result := &Storage{}
	for _, i := range filesPath {
		recordFile, e = LoadRecordFileFromDisk(i, fileMaxSize)
		if e != nil {
			return nil, e
		}
		if result.ActiveFiles[recordFile.dataType] == nil {
			result.ActiveFiles[recordFile.dataType] = recordFile
			continue
		}
		if result.ActiveFiles[recordFile.dataType].fileID < recordFile.fileID {
			result.ArchivedFiles[recordFile.dataType][result.ActiveFiles[recordFile.dataType].fileID] = result.ActiveFiles[recordFile.dataType]
		}
		result.ActiveFiles[recordFile.dataType] = recordFile
	}
	return nil, nil
}
