package file

import "testing"

func TestMMap(t *testing.T) {
	m, e := NewFileMMap("C:\\Users\\Misaka19327\\OneDrive\\Projects\\GoLandProjects\\MisakaDB\\test.txt", 65536)
	if e != nil {
		t.Fatal(e)
	}

	defer func() {
		e = m.Close()
		if e != nil {
			t.Fatal(e)
		}
	}()

	e = m.Write([]byte("111222"), 0)
	if e != nil {
		t.Fatal(e)
	}
	e = m.Write([]byte("333444"), 6)
	if e != nil {
		t.Fatal(e)
	}

	data := make([]byte, 5)
	e = m.Read(data, 0)
	if e != nil {
		t.Log(data)
		t.Fatal(e)
	}
	t.Log(data)
}
