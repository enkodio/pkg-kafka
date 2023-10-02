package entity

import (
	"sync"
)

type SyncGroup struct {
	wg     *sync.WaitGroup
	done   chan struct{}
	closed bool
}

func NewSyncGroup() *SyncGroup {
	return &SyncGroup{
		wg:   new(sync.WaitGroup),
		done: make(chan struct{}),
	}
}

func (s *SyncGroup) Add(delta int) {
	s.wg.Add(delta)
}

func (s *SyncGroup) Done() {
	s.wg.Done()
}

func (s *SyncGroup) Wait() {
	s.wg.Wait()
}

func (s *SyncGroup) Close() {
	//if not closed
	if !s.closed {
		close(s.done)
		s.closed = true
	}
	s.wg.Wait()
	return
}

func (s *SyncGroup) IsDone() <-chan struct{} {
	return s.done
}

func (s *SyncGroup) Start() {
	s.closed = false
	s.done = make(chan struct{})
}
