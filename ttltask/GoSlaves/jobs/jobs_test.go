package jobs

import "testing"

func TestOpen_Jobs(t *testing.T) {
	jobs := new(Jobs)

	jobs.Open()

	go func() {
		jobs.Put("hola")
		jobs.Put("ye")
		jobs.Put("yo")
		jobs.Put("ya")
		jobs.Close()
	}()

	var i = 0
	for {
		data, err := jobs.Get()
		if err != nil {
			break
		}
		println(data.(string))
		i++
	}

	if i != 4 {
		panic("error")
	}
}
