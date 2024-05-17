package util

import "testing"

func TestDecodeKeyAndField(t *testing.T) {
	bytes := EncodeKeyAndField("testKey", "")
	t.Log(bytes)
	key, field, e := DecodeKeyAndField(bytes)
	if e != nil {
		t.Log(e)
		return
	}
	t.Log(key)
	t.Log(field)
}

func TestGenerateLogFilePath(t *testing.T) {
	result := GenerateLogFilePath("D:\\MisakaDBLog")
	t.Log(result)
}
