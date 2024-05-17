package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenFileUnderFolder(t *testing.T) {
	var files []string
	//方法一
	var walkFunc = func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		//fmt.Printf("%s\n", path)
		return nil
	}
	err := filepath.Walk("D:\\MisakaDBTest", walkFunc)
	if err != nil {
		t.Log(err)
		return
	}
	fmt.Println(files)
}
