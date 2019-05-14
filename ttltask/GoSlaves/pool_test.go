package slaves

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen_SlavePool(t *testing.T) {
	sp := MakePool(5, func(obj interface{}) interface{} {
		fmt.Println(obj)
		return nil
	}, nil)

	sp.Open()
	defer sp.Close()
	println("opened")
}

func TestSendWork_SlavePool(t *testing.T) {
	sp := MakePool(5, func(obj interface{}) interface{} {
		fmt.Println(obj)
		return nil
	}, nil)

	sp.Open()
	defer sp.Close()

	files, err := ioutil.ReadDir(os.TempDir())
	if err == nil {
		fmt.Println("Files in temp directory:")
		for i := range files {
			sp.SendWork(files[i].Name())
		}
	}
}
