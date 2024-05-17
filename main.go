package main

import (
	"fmt"
	"runtime"
)

func main() {
	db, e := Init()
	if e != nil {
		fmt.Println(e.Error())
		return
	}
	e = db.StartServe()
	if e != nil {
		fmt.Println(e.Error())
	}

	buffer := make([]byte, 10000000)
	bytesNum := runtime.Stack(buffer, false)
	fmt.Println(string(buffer[:bytesNum]))
}
