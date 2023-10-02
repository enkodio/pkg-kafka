package kafka_client

type Semaphore struct {
	c chan struct{}
}

func NewSemaphore(count int) Semaphore {
	return Semaphore{
		c: make(chan struct{}, count),
	}
}

func (s *Semaphore) Acquire() {
	s.c <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.c
}
