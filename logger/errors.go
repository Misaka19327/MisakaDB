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
	FieldIsNotExisted = errors.New("Field is Not Existed! ")

	KeyIsNotExisted = errors.New("Key is Not Existed! ")
	KeyIsExisted    = errors.New("Key is Existed! ")

	ValueIsExpired = errors.New("This Value was Expired! ")

	ParameterIsNotAllowed = errors.New("Parameter is Not Allowed! ")

	TimeUnitIsNotSupported = errors.New("Time Unit is Not Supported! ")

	// ListIndex 使用的错误

	IndexIsIllegal         = errors.New("Index is Illegal to Access List! ")
	RemoveCountIsNotEnough = errors.New("List Do Not Have Enough Element to Remove! ")

	// ZSet 使用的错误

	MemberIsNotExisted = errors.New("Member is Not Existed! ")
	MemberIsExpired    = errors.New("This Member was Expired! ")
)

// 不准备常驻的错误们
var (
	MMapIsNotSupport = errors.New("MMap IO will Support Soon")
)
