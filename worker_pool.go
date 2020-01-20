package goutils

import (
	"errors"
	log "github.com/thinkphoebe/golog"
)

type TaskCallback func(param interface{}) (result interface{}, err error)

type taskResult struct {
	result interface{}
	err    error
}

type taskInfo struct {
	callback TaskCallback
	param    interface{}
	retry    int
	chResult chan *taskResult
}

type Worker struct {
	chTask chan *taskInfo
	chCmd  chan int
}

type WorkerPoolConfig struct {
	WorkerCount int
	MaxTasks    int
	//ScaleInterval  int // in seconds
}

type WorkerPool struct {
	config  *WorkerPoolConfig
	workers []*Worker
	chTask  chan *taskInfo
	chIdle  chan *taskInfo
	stoped  bool
}

func (self *WorkerPool) Start(config *WorkerPoolConfig) {
	self.config = config
	self.stoped = false

	self.chTask = make(chan *taskInfo, self.config.MaxTasks)
	self.chIdle = make(chan *taskInfo, self.config.MaxTasks)
	for i := 0; i < self.config.MaxTasks; i++ {
		ti := &taskInfo{chResult: make(chan *taskResult, 1)}
		self.chIdle <- ti
	}

	for i := 0; i < self.config.WorkerCount; i++ {
		w := &Worker{
			chTask: self.chTask,
			chCmd:  make(chan int, 1),
		}
		self.workers = append(self.workers, w)
	}
}

// wait all task in queue complete if elegant is true, else return error for uncompleted task
func (self *WorkerPool) Stop(elegant bool) {
	self.stoped = true

	if elegant {
		for i := 0; i < self.config.MaxTasks; i++ {
			ti := <-self.chIdle
			close(ti.chResult)
		}
		for _, w := range self.workers {
			close(w.chCmd)
		}
	} else {
		for _, w := range self.workers {
			close(w.chCmd)
		}
		for {
			select {
			case ti := <-self.chIdle:
				close(ti.chResult)
			case ti := <-self.chTask:
				close(ti.chResult)
			default:
				break
			}
		}
	}

	close(self.chTask)
	close(self.chIdle)
}

func (self *WorkerPool) workerRoutine(worker *Worker) {
	for {
		select {
		case task := <-self.chTask:
			var result interface{}
			var err error
			for i := 0; i < task.retry+1; i++ {
				result, err = task.callback(task.param)
				if err == nil {
					break
				}
			}
			task.chResult <- &taskResult{result: result, err: err}
		case _, ok := <-worker.chCmd:
			if !ok {
				break
			}
		}
	}
}

//func (self *WorkerPool) checkScale(clean bool) {
//	if len(self.chTask) > self.config.MaxTasks*3/4 {
//	}
//}
//
//func (self *WorkerPool) scaleRoutine() {
//	ticker := time.NewTicker(time.Second * time.Duration(self.config.ScaleInterval))
//	tickChan := ticker.C
//	for {
//		select {
//		case <-tickChan:
//			self.checkScale(true)
//		case _, ok := <-self.chTask:
//			if !ok {
//				break
//			}
//		}
//	}
//}

func (self *WorkerPool) RunTask(callback TaskCallback, param interface{}, retry int) (result interface{}, err error) {
	if self.stoped {
		return nil, errors.New("worker pool has stopped")
	}

	//self.checkScale(false)

	ti := <-self.chIdle
	ti.callback = callback
	ti.param = param
	ti.retry = retry

	self.chTask <- ti

	r, ok := <-ti.chResult
	if !ok {
		return nil, errors.New("worker pool has stopped")
	}
	return r.result, r.err
}
