package slave

import (
	"errors"
	"ekvproxy/ttltask/GoSlaves/jobs"
	"sync"
)

var (
	defaultAfter = func(obj interface{}) {}
	// ErrSlaveOpened occurs whem the same
	// slave is open twice
	ErrSlaveOpened = errors.New("slave is already opened")
)

// Slave object that will
// do the works
type Slave struct {
	jobs *jobs.Jobs
	// Name of slave
	Name string
	// Work of slave
	Work func(interface{}) interface{}
	// Function that will be execute when
	// Work finishes. The return value of
	// Work() will be parse to After()
	After func(interface{})
	wg    sync.WaitGroup
}

// NewSlave Create a slave easily parsing
// the name of slave,
// work to do when SendWork it's called (cannot be nil)
// and work to do after Work() it's called
func NewSlave(name string,
	work func(interface{}) interface{},
	after func(interface{})) *Slave {

	if work == nil {
		return nil
	}
	return &Slave{
		Work:  work,
		After: after,
	}
}

// Open Creates job buffered channel and
// starts a goroutine which will receive
// all works asynchronously
func (s *Slave) Open() error {
	if s.jobs != nil {
		return ErrSlaveOpened
	}
	s.jobs = new(jobs.Jobs)
	s.jobs.Open()

	if s.After == nil {
		s.After = defaultAfter
	}

	go func() {
		for {
			job, err := s.jobs.Get()
			if err != nil {
				return
			}
			s.After(s.Work(job))
			s.wg.Done()
		}
	}()

	return nil
}

// SendWork sends work to slave and increment WaitGroup
func (s *Slave) SendWork(job interface{}) {
	s.wg.Add(1)
	s.jobs.Put(job)
}

// ToDo Returns the number of jobs to do
func (s *Slave) ToDo() int {
	return s.jobs.Len()
}

// Close waits all jobs to finish and close
// buffered channel of jobs
func (s *Slave) Close() {
	s.wg.Wait()
	s.jobs.Close()
}
