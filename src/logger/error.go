package logger

import "errors"

// 准备常驻的错误们
var (
	UnSupportDataType       = errors.New("Un Supported Data Type! \n")
	FileBytesIsMaxedOut     = errors.New("File Size Exceeds Requirement and It can't Write Any More Bytes! \n")
	CRCCheckSumNotPassed    = errors.New("CRC32 Check is Not Passed! \n")
	FileIsNotExist          = errors.New("File is not Exist! \n")
	DecodeKeyAndFieldFailed = errors.New("Decode Key and Field Failed! \n")

	FieldIsExisted    = errors.New("Field is Existed! \n")
	KeyIsNotExisted   = errors.New("Key is Not Existed! \n")
	FieldIsNotExisted = errors.New("Field is Not Existed! \n")
)

// 不准备常驻的错误们
var (
	MMapIsNotSupport = errors.New("MMap IO will Support Soon")
)
