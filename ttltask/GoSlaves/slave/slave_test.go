package slave

import "testing"

func TestOpen_Slave(t *testing.T) {
	s := &Slave{
		Work: func(obj interface{}) interface{} {
			println(obj.(string))
			return nil
		},
	}
	s.Open()
	defer s.Close()
	s.SendWork("this")
	s.SendWork("is")
	s.SendWork("to")
	s.SendWork("check")
	s.SendWork("one")
	s.SendWork("slave")
	s.SendWork("hehe")
}
