package storage

import (
	"os"
	"path/filepath"
)

// RecordFilesInit 按路径读取所有文件 并且转换为RecordFile 按数据类型进行分类 默认情况下编号最大的文件是活跃文件
// attention 活跃文件也存在于归档文件中 等到活跃文件写满之后 再开一个活跃文件存入归档文件即可 之前的活跃文件自动成为归档文件
func RecordFilesInit(path string, fileMaxSize int64) (activeFiles map[FileForData]*RecordFile, archiveFiles map[FileForData]map[uint32]*RecordFile, e error) {
	var filesPath []string
	var walkFunc = func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			filesPath = append(filesPath, path)
		}
		return nil
	}
	e = filepath.Walk(path, walkFunc)
	if e != nil {
		return nil, nil, e
	}

	archiveFiles = make(map[FileForData]map[uint32]*RecordFile)
	activeFiles = make(map[FileForData]*RecordFile)

	var recordFile *RecordFile
	for _, i := range filesPath {
		// 读取文件
		recordFile, e = LoadRecordFileFromDisk(i, fileMaxSize)
		if e != nil {
			return nil, nil, e
		}
		// 先写入归档文件 记得先检查双重hash是否有nil
		if _, ok := archiveFiles[recordFile.dataType]; ok {
			archiveFiles[recordFile.dataType][recordFile.fileID] = recordFile
		} else {
			archiveFiles[recordFile.dataType] = make(map[uint32]*RecordFile)
			archiveFiles[recordFile.dataType][recordFile.fileID] = recordFile
		}
		// 再看fileID大小写入活跃文件
		if activeFiles[recordFile.dataType] == nil {
			activeFiles[recordFile.dataType] = recordFile
		} else {
			if activeFiles[recordFile.dataType].fileID < recordFile.fileID {
				activeFiles[recordFile.dataType] = recordFile
			}
		}
	}
	e = nil
	return
}
