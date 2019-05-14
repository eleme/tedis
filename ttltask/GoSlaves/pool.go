package slaves

import (
	"errors"
	"ekvproxy/ttltask/GoSlaves/slave"
	"math"
)

var (
	// ErrPoolOpened occurs when pool have been opened
	// twice
	ErrPoolOpened = errors.New("pool have been opened")
)

// SlavePool object
type SlavePool struct {
	Slaves []*slave.Slave
}

// MakePool creates a pool and initialise Slaves
// num is the number of Slaves
// work is the function to execute in the Slaves
// after the function to execute after execution of work
func MakePool(num uint, work func(interface{}) interface{},
	after func(interface{})) SlavePool {

	sp := SlavePool{
		Slaves: make([]*slave.Slave, num),
	}
	for i := range sp.Slaves {
		sp.Slaves[i] = &slave.Slave{
			Work:  work,
			After: after,
		}
	}
	return sp
}

// Open open all Slaves
func (sp *SlavePool) Open() error {
	//if sp.Slaves != nil {
	//	return ErrPoolOpened
	//}
	for _, s := range sp.Slaves {
		if s != nil {
			s.Open()
		}
	}
	return nil
}

// Len Gets the length of the slave array
func (sp *SlavePool) Len() int {
	return len(sp.Slaves)
}

// SendWork Send work to the pool.
// This function get the slave with less number
// of works and send him the job
func (sp *SlavePool) SendWork(job interface{}) {
	v := math.MaxInt32
	sel := 0
	for i, s := range sp.Slaves {
		if len := s.ToDo(); len < v {
			v, sel = len, i
		}
	}
	sp.Slaves[sel].SendWork(job)
}

// Close closes the pool waiting
// the end of all jobs
func (sp *SlavePool) Close() {
	for _, s := range sp.Slaves {
		s.Close()
	}
}

func (sp *SlavePool) WorkQueueLen() int {
	var count int
	for _, s := range sp.Slaves {
		count += s.ToDo()
	}

	return count;
}
