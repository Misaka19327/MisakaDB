//go:build windows

package file

import (
	"MisakaDB/logger"
	"io"
	"os"
	"syscall"
	"unsafe"
)

/*
必须意识到 syscall 是为 golang 提供了调用系统接口的接口 所以下面的这些接口实质上都是 c++ 的函数

Windows 的 mmap 所使用的函数如下

CreateFileMapping 它的本质是 memoryapi.h 中的 CreateFileMappingW 函数 作用是为指定文件创建或打开命名或未命名的文件映射对象 (这个文件映射对象就是 mmap

参数依次向下排列为
hFile 要创建文件映射对象的文件的句柄
lpFileMappingAttributes 指向 SECURITY_ATTRIBUTES 结构的指针 起到一个设定文件映射对象安全性的作用 一般不用管 默认为空即可
flProtect 指定文件映射对象的页保护 说人话就是读写权限
dwMaximumSizeHigh 文件映射对象最大大小的高序 DWORD
dwMaximumSizeLow 文件映射对象最大大小的低序 DWORD
lpName 文件映射对象的名称 一般也不用起名字 进程间通信除外

如果创建成功 那么返回值即为新创建的文件映射对象的句柄
如果对象之前就存在 那么返回之前存在的那个句柄
如果没创建成功 返回 null (鉴于 syscall.Handle 底层的类型是 uintptr 返回一个 null 其实是返回0吧

具体信息参照 https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw

MapViewOfFile 它的本质是 memoryapi.h 中的 MapViewOfFile 函数 作用是将文件映射的视图映射到调用进程的地址空间中

参数依次向下排列为
hFileMappingObject 文件映射对象的句柄
dwDesiredAccess 文件映射对象的访问类型 FILE_MAP_WRITE FILE_MAP_ALL_ACCESS 这俩是一样的 即为允许读写 FILE_MAP_READ 即为只读 允许读写的时候 上面的函数的 flProtect 必须是 PAGE_READWRITE
dwFileOffsetHigh 视图开始位置的文件偏移量的高阶 DWORD
dwFileOffsetLow 视图开始位置的文件偏移量的低序 DWORD
dwNumberOfBytesToMap 要映射到视图的文件映射的字节数 该字节数必须小于等于 CreateFileMapping 函数指定的最大大小

如果函数成功 返回映射视图的起始地址
如果函数失败 返回 null

具体信息参照 https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-mapviewoffile

CloseHandle 它的本质是 handleapi.h 里的 closeHandle 函数 作用是关闭一个打开的对象句柄 参数也只有一个要被关闭的句柄

具体信息参照 https://learn.microsoft.com/zh-cn/windows/win32/api/handleapi/nf-handleapi-closehandle

DWORD 类型一般是 unsigned long 一般是uint32
*/

/*
mmap 的主要思路是这样的

映射到内存里的大小由 MaxFileSize 指定 所以要读取的文件大小一定不能超过 MaxFileSize

在映射开始之前 必须保存这个文件的初始大小作为该文件的有效内容长度

之后无论是删除其中内容还是写入其中内容 都要维护这个有效内容长度

最后关闭文件时 先取消映射 再根据内容长度来重新保存文件 防止后面多出一堆的0导致映射开始时无法获取到文件真实内容长度

attention 注意 写测试的时候也需要关闭文件（关闭索引）
*/

type MMapFile struct {
	file            *os.File
	mmapArray       []byte // 从底层上 将一个数组转换而来的切片 所以严禁! 严禁! 严禁!对该切片调用 append 函数
	fileContentSize int64
	fileMaxSize     int64
	actualAddr      uintptr
}

func NewFileMMap(filePath string, MaxFileSize int64) (*MMapFile, error) {
	maxFileSizeUint32 := uint32(MaxFileSize)
	file, e := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, e
	}

	s, e := file.Stat()
	if e != nil {
		return nil, e
	}

	result := &MMapFile{
		file:            file,
		fileContentSize: s.Size(),
		fileMaxSize:     MaxFileSize,
	}

	h, e := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READWRITE, 0, maxFileSizeUint32, nil)
	if e != nil || h == 0 {
		return nil, e
	}

	addr, e := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(maxFileSizeUint32))
	if addr == 0 {
		return nil, e
	}

	result.actualAddr = addr

	e = syscall.CloseHandle(h)
	if e != nil {
		return nil, e
	}

	// 这个写法很值得参考 从数组转 slice 不再需要重新申请内存空间
	var customSlice = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(maxFileSizeUint32), int(maxFileSizeUint32)}

	result.mmapArray = *(*[]byte)(unsafe.Pointer(&customSlice))

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
	return syscall.FlushViewOfFile(mf.actualAddr, uintptr(mf.fileContentSize))
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
	e := syscall.UnmapViewOfFile(mf.actualAddr)
	if e != nil {
		return e
	}
	e = mf.file.Truncate(mf.fileContentSize) // 只保留前 mf.fileContentSize 数量的字节
	if e != nil {
		return e
	}
	return mf.file.Close()
}

func (mf *MMapFile) Length() (int64, error) {
	return mf.fileContentSize, nil
}
