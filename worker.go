package mqlessworker

import (
	"errors"
	"log"

	"github.com/routiz/mqlessworker/job"
)

type Worker interface {
	RegisterHdlr(string, job.Handler, job.ErrHandler) error
	Run() error
	Notify() error
}

type worker struct {
	C             Config
	jobStore      job.JobStore
	jobchan       chan job.Job
	jobHandlerMap map[string]job.Handler
	errHandlerMap map[string]job.ErrHandler
}

func NewWorker(cnf Config, js job.JobStore) Worker {
	return &worker{
		C:             cnf,
		jobStore:      js,
		jobchan:       make(chan job.Job, 1024),
		jobHandlerMap: make(map[string]job.Handler),
		errHandlerMap: make(map[string]job.ErrHandler),
	}
}

func (w *worker) work() {
	for {
		j := <-w.jobchan
		if err := j.Hdlr(j.JsonData); err != nil && j.Ehdlr != nil {
			j.Ehdlr(err)
			j.Status = job.JobStatusError
		} else {
			j.Status = job.JobStatusFinished
		}
		w.jobStore.Put(j)
	}
}

func (w *worker) RegisterHdlr(jobTypeID string, h job.Handler, eh job.ErrHandler) error {
	if jobTypeID == "" {
		return errors.New("empty string job type ID is not allowed")
	}
	if h == nil {
		return errors.New("nil Handler is not allowed")
	}
	w.jobHandlerMap[jobTypeID] = h
	if eh != nil {
		w.errHandlerMap[jobTypeID] = eh
	}
	return nil
}

// Run 은 worker 고루틴들을 시작한다. Run 호출 이후부터 워커들이
// 작업을 대기하는 상태로 들어간다.
func (w *worker) Run() error {
	for i := 0; i < w.C.WorkerCount; i++ {
		go w.work()
	}
	// TODO stub
	return nil
}

func (w *worker) Notify() error {
	retrycnt := 0

	// Notify 가 호출되었을 때 worker 들이 수행할 작업이 있는지를
	// 체크하여 필요한 경우 수행한다.

	// TODO 현재 하나의 job을 가져올 때마다 DB 읽기를 수행하게
	// 되어 있다. 적정량의 job을 한 번에 가져올 수 있는 방법을
	// 고민하면 성능을 향상시킬 수 있을 것이다
	for {
		j, err := w.jobStore.Get()
		if err != nil {
			log.Println("failed to get job -", err.Error())
			if retrycnt++; retrycnt == 3 {
				return err
			}
			continue
		}
		if j.JobID == 0 || j.JobTypeID == "" {
			break
		}
		h, ok := w.jobHandlerMap[j.JobTypeID]
		if !ok {
			log.Println("handler of", j.JobTypeID,
				"is not registered")
			j.Status = job.JobStatusError
			w.jobStore.Put(j)
			continue
		}
		j.Hdlr = h
		eh, ok := w.errHandlerMap[j.JobTypeID]
		if !ok {
			log.Println("error handler of", j.JobTypeID,
				"is not registered")
			j.Status = job.JobStatusError
			w.jobStore.Put(j)
			continue
		}
		j.Ehdlr = eh
		w.jobchan <- j
	}
	return nil
}
