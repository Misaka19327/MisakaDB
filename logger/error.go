package logger

import "errors"

// 准备常驻的错误们
var (
	UnSupportDataType       = errors.New("Un Supported Data Type! ")
	FileBytesIsMaxedOut     = errors.New("File Size Exceeds Requirement and It can't Write Any More Bytes! ")
	CRCCheckSumNotPassed    = errors.New("CRC32 Check is Not Passed! ")
	FileIsNotExist          = errors.New("File is not Exist! ")
	DecodeKeyAndFieldFailed = errors.New("Decode Key and Field Failed! ")

	FieldIsExisted    = errors.New("Field is Existed! ")
	KeyIsNotExisted   = errors.New("Key is Not Existed! ")
	FieldIsNotExisted = errors.New("Field is Not Existed! ")

	ValueIsExpired = errors.New("This Value was Expired! ")
)

// 不准备常驻的错误们
var (
	MMapIsNotSupport = errors.New("MMap IO will Support Soon")
)
