package pool

import "sync"

type Output[T any, R any] struct {
	Task   T
	Result R
	Err    error
}

type Pool[T any, R any] struct {
	taskCh   chan T
	handler  func(T) (R, error)
	workers  uint32
	resultCh chan Output[T, R]
	errorCh  chan error
	doneCh   chan bool
	quitCh   chan bool
	wg       sync.WaitGroup
}

func New[T any, R any](handler func(T) (R, error), workers uint32) *Pool[T, R] {
	pool := &Pool[T, R]{
		taskCh:   make(chan T, workers),
		handler:  handler,
		workers:  workers,
		resultCh: make(chan Output[T, R], workers),
		errorCh:  make(chan error, workers),
		doneCh:   make(chan bool),
		quitCh:   make(chan bool),
	}
	return pool
}

func (p *Pool[T, R]) Dispatch(task T) {
	p.taskCh <- task
}

func (p *Pool[T, R]) Errors() <-chan error {
	return p.errorCh
}

func (p *Pool[T, R]) Done() <-chan bool {
	return p.doneCh
}

func (p *Pool[T, R]) Results() <-chan Output[T, R] {
	return p.resultCh
}

func (p *Pool[T, R]) Start() {
	for i := 0; uint32(i) < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool[T, R]) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.taskCh:
			if !ok {
				return
			}
			result, err := p.handler(task)
			p.resultCh <- Output[T, R]{Task: task, Result: result, Err: err}
		case <-p.quitCh:
			return
		}
	}
}

func (p *Pool[T, R]) Close() {
	close(p.taskCh)
	close(p.quitCh)
	p.wg.Wait()
	close(p.errorCh)
	close(p.doneCh)
}
