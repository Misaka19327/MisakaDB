package storage

import "testing"

func TestNewFileIO(t *testing.T) {
	file, e := NewFileIO("C:\\Users\\Misaka19327\\OneDrive\\Projects\\GoLandProjects\\MisakaDB\\test.txt")
	if e != nil {
		t.Log(e)
		return
	}

	defer func() {
		e = file.Close()
		if e != nil {
			t.Log(e)
		}
	}()

	t.Log(file.file.Name())
	e = file.Write([]byte{1, 2, 3, 4, 5}, 0)
	if e != nil {
		t.Log(e)
	}
	buffer := make([]byte, 5)
	e = file.Read(buffer, 0)
	if e != nil {
		t.Log(e)
	} else {
		t.Log(buffer)
	}
}
