package storage

import (
	"MisakaDB/file"
	"MisakaDB/logger"
	"MisakaDB/util"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FileForData 指定该数据文件存储的数据类型
type FileForData int8

const (
	String FileForData = iota
	Hash
	List
	Set
	ZSet
)

var (
	// 文件名的后缀
	fileNameSuffix = map[FileForData]string{
		String: "record.string.",
		Hash:   "record.hash.",
		List:   "record.list.",
		Set:    "record.set.",
		ZSet:   "record.zset.",
	}

	// 根据文件名解析该文件的存储数据的类型
	filenameToDataTypeMap = map[string]FileForData{
		"string": String,
		"hash":   Hash,
		"list":   List,
		"set":    Set,
		"zset":   ZSet,
	}
)

// RecordFile 将数据文件抽象为该结构体
type RecordFile struct {
	file         file.FileWriter // 对文件进行操作的结构体
	fileID       uint32          // 文件ID
	newestOffset int64           // 该文件写入位置 或者说最新偏移位也可以
	dataType     FileForData     // 该文件存储的键值对的类型
	fileMaxSize  int64           // 该文件最大的大小

	IsSyncing bool // 是否在定时sync
}

// FileIOType 指定文件的读写模式
type FileIOType int8

const (
	MMapIOFile        FileIOType = iota // MMap方式
	TraditionalIOFile                   // 传统IO方式
)

// NewRecordFile 给定路径 读写模式 存储数据类型 文件ID和文件最大大小 新建一个RecordFile
func NewRecordFile(ioType FileIOType, dataType FileForData, fid uint32, path string, fileMaxSize int64) (*RecordFile, error) {
	result := &RecordFile{
		fileID:       fid,
		fileMaxSize:  fileMaxSize,
		newestOffset: 0,
		dataType:     dataType,
	}
	fileFullPath, e := getFileName(fid, dataType, path)
	if e != nil {
		return nil, e
	}
	var fileWriter file.FileWriter
	switch ioType {
	case MMapIOFile:
		return nil, logger.MMapIsNotSupport
	case TraditionalIOFile:
		fileWriter, e = file.NewFileIO(fileFullPath)
	}
	if e != nil {
		return nil, e
	}
	result.file = fileWriter
	return result, nil
}

// LoadRecordFileFromDisk 加载一个文件
func LoadRecordFileFromDisk(filePath string, fileMaxSize int64) (*RecordFile, error) {
	if _, e := os.Stat(filePath); e != nil {
		return nil, logger.FileIsNotExist
	}
	f, e := file.NewFileIO(filePath)
	if e != nil {
		return nil, e
	}
	fileLen, e := f.Length()
	if e != nil {
		return nil, e
	}
	result := &RecordFile{
		file:         f,
		fileMaxSize:  fileMaxSize,
		newestOffset: fileLen,
	}
	result.fileID, result.dataType, e = parseFileName(path.Base(filePath))
	if e != nil {
		return nil, e
	}
	return result, nil
}

// WriteEntryIntoFile 尝试将Entry写入文件 如果文件剩余大小已经不足以再写入Entry 则返回FileBytesIsMaxedOut错误
func (rf *RecordFile) WriteEntryIntoFile(entry *Entry) error {
	writeContent, length := entry.Encode()
	if int64(length)+rf.newestOffset > rf.fileMaxSize {
		logger.GenerateErrorLog(false, false, logger.FileBytesIsMaxedOut.Error(), strconv.Itoa(int(rf.fileID)), strconv.Itoa(int(rf.dataType)))
		return logger.FileBytesIsMaxedOut
	}
	e := rf.file.Write(writeContent, int(rf.newestOffset))
	rf.newestOffset += int64(length) // 调整偏移位
	return e
}

// ReadIntoEntry 在RecordFile中 从给定的offset开始 尝试读取一个完整的Entry并且返回 第二个返回值为当此读取Entry的长度 用以快速定位下次Entry的offset
func (rf *RecordFile) ReadIntoEntry(offset int64) (*Entry, int64, error) {
	entryHeaderBytes := make([]byte, MaxEntryHeaderLength)
	e := rf.file.Read(entryHeaderBytes, int(offset))
	if e != nil {
		return nil, 0, e
	}
	entryHeader, index := decodeEntryHeader(entryHeaderBytes)
	result := &Entry{
		EntryType: entryHeader.entryType,
		ExpiredAt: entryHeader.expiredAt,
		Key:       make([]byte, entryHeader.keyLength),
		Value:     make([]byte, entryHeader.valueLength),
	}
	e = rf.file.Read(result.Key, int(offset+index))
	if e != nil {
		return nil, 0, e
	}
	e = rf.file.Read(result.Value, int(offset+index)+int(entryHeader.keyLength))
	if e != nil {
		return nil, 0, e
	}
	if crc := getEntryCRC(result, entryHeaderBytes[crc32.Size:index]); crc != entryHeader.crc {
		logger.GenerateErrorLog(false, false, logger.CRCCheckSumNotPassed.Error(), util.TurnByteArrayToString(entryHeaderBytes))
		return nil, 0, logger.CRCCheckSumNotPassed
	}
	entrySize := index + int64(entryHeader.keyLength+entryHeader.valueLength)
	if entrySize < MaxEntryHeaderLength { // 注意Entry的最小长度为25
		return result, MaxEntryHeaderLength, nil
	}
	return result, entrySize, nil
}

// Sync 强制刷新缓冲区到文件中
func (rf *RecordFile) sync() error {
	return rf.file.Sync()
}

// Delete 删除该文件
func (rf *RecordFile) Delete() error {
	return rf.file.Delete()
}

// Close 关闭该文件
func (rf *RecordFile) Close() error {
	return rf.file.Close()
}

// Length 获取文件长度
func (rf *RecordFile) Length() (int64, error) {
	return rf.file.Length()
}

// GetFileID 获取文件ID
func (rf *RecordFile) GetFileID() uint32 {
	return rf.fileID
}

// GetOffset 获取当前文件的最新offset
func (rf *RecordFile) GetOffset() int64 {
	return rf.newestOffset
}

// StartSyncRoutine 开始定时Sync 时间间隔以duration为准
func (rf *RecordFile) StartSyncRoutine(duration time.Duration) {
	rf.IsSyncing = true
	logger.GenerateInfoLog("File " + strconv.Itoa(int(rf.fileID)) + " is Syncing!")
	go func() {
		for {
			if rf.IsSyncing {
				time.Sleep(duration)
				e := rf.file.Sync()
				if e != nil { // 一旦报错就结束定时同步
					return
				}
			} else {
				logger.GenerateInfoLog("File " + strconv.Itoa(int(rf.fileID)) + " Stop Sync!")
				_ = rf.file.Sync()
				return
			}
		}
	}()
}

func (rf *RecordFile) StopSyncRoutine() {
	rf.IsSyncing = false
	return
}

// getFileName 给record文件起名 示例名字：record.string.000000001.misaka
func getFileName(fid uint32, dataType FileForData, path string) (string, error) {
	if _, ok := fileNameSuffix[dataType]; !ok {
		logger.GenerateErrorLog(false, true, logger.UnSupportDataType.Error(), strconv.Itoa(int(dataType)))
		return "", logger.UnSupportDataType
	}
	fileName := fileNameSuffix[dataType] + fmt.Sprintf("%09d", fid) + ".misaka"
	return filepath.Join(path, fileName), nil
}

// parseFileName 给定的FileName中 解析出需要的信息 示例文件名：record.string.000000001.misaka
func parseFileName(fileName string) (uint32, FileForData, error) {
	strs := strings.Split(fileName, ".")
	resultNum, e := strconv.Atoi(strs[2])
	if e != nil {
		logger.GenerateErrorLog(false, true, e.Error(), fileName)
		return 0, String, e
	}
	return uint32(resultNum), filenameToDataTypeMap[strs[1]], nil
}
