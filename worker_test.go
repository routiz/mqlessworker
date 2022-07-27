package mqlessworker_test

import (
	"errors"
	"log"
	"testing"
	"time"

	"github.com/routiz/mqlessworker"
	"github.com/routiz/mqlessworker/job"
)

type FakeJobStore struct {
	jobs []job.Job
}

func (fjs *FakeJobStore) Get() (job.Job, error) {
	for i, j := range fjs.jobs {
		if j.Status == job.JobStatusQueued {
			j.Status = job.JobStatusDoing
			fjs.jobs[i] = j
			return j, nil
		}
	}
	return job.Job{}, nil
}

func (fjs *FakeJobStore) Put(j job.Job) error {
	for i, iterj := range fjs.jobs {
		if iterj.JobID == j.JobID {
			fjs.jobs[i] = j
			return nil
		}
	}
	fjs.jobs = append(fjs.jobs, j)
	return nil
}

const (
	JobTypeSliceLen = "slice-len"
	JobTypePrintMsg = "print-msg"
)

func handleSliceLen(jsondata []byte) error { return nil }

func handleSliceLenErr(err error) {}

func handlePrintMsg(jsondata []byte) error { return nil }

func handlePrintMsgErr(err error) {}

func TestWorker(t *testing.T) {
	t.Run("test-run", func(t *testing.T) {
		cnf := mqlessworker.Config{
			AppID:       "worker_test",
			WorkerCount: 1,
		}
		fjs := FakeJobStore{
			jobs: []job.Job{{
				JobID:     1,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("[]"),
			}, {
				JobID:     2,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypePrintMsg,
				JsonData:  []byte("{\"msg\":\"message-1\"}"),
			}, {
				JobID:     3,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("{\"msg\":\"message-2\"}"),
			}},
		}
		w := mqlessworker.NewWorker(cnf, &fjs)
		w.RegisterHdlr(JobTypeSliceLen,
			func(b []byte) error { return nil },
			func(err error) {})
		w.RegisterHdlr(JobTypePrintMsg,
			func(b []byte) error { return nil },
			func(err error) {})
		w.Run()
		w.Notify()
		time.Sleep(100 * time.Millisecond)
		for _, j := range fjs.jobs {
			if j.Status != job.JobStatusFinished {
				t.Log("expected successed:", j.Status)
				t.Fail()
			}
		}
	})
	t.Run("test-register-handler-errors", func(t *testing.T) {
		cnf := mqlessworker.Config{
			AppID:       "worker_test",
			WorkerCount: 1,
		}
		fjs := FakeJobStore{
			jobs: []job.Job{{
				JobID:     1,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("[]"),
			}, {
				JobID:     2,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypePrintMsg,
				JsonData:  []byte("{\"msg\":\"message-1\"}"),
			}, {
				JobID:     3,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("{\"msg\":\"message-2\"}"),
			}},
		}
		w := mqlessworker.NewWorker(cnf, &fjs)
		w.RegisterHdlr(JobTypeSliceLen,
			func(b []byte) error { return nil },
			nil)
		if err := w.RegisterHdlr("",
			func(b []byte) error { return nil },
			func(err error) {}); err == nil {
			t.Error("empty string job type ID should not be allowed")
		}
		if err := w.RegisterHdlr(JobTypePrintMsg,
			nil,
			func(err error) {}); err == nil {
			t.Error("nil handler should not be allowed")
		}
		w.Run()
		w.Notify()
		time.Sleep(100 * time.Millisecond)
		for _, j := range fjs.jobs {
			if j.JobTypeID == JobTypePrintMsg &&
				j.Status != job.JobStatusError {

				t.Log("expected error:", j.Status)
				t.Fail()
			}
		}
	})
	t.Run("test-handler-error", func(t *testing.T) {
		cnf := mqlessworker.Config{
			AppID:       "worker_test",
			WorkerCount: 1,
		}
		fjs := FakeJobStore{
			jobs: []job.Job{{
				JobID:     1,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("[]"),
			}},
		}
		errhndlchan := make(chan int, 10)
		w := mqlessworker.NewWorker(cnf, &fjs)
		w.RegisterHdlr(JobTypeSliceLen,
			func(b []byte) error {
				return errors.New("intended-error")
			},
			func(err error) {
				errhndlchan <- 123
			})
		w.Run()
		w.Notify()
		reply := <-errhndlchan
		time.Sleep(100 * time.Millisecond)

		if j := fjs.jobs[0]; j.Status != job.JobStatusError {
			t.Log("expected error:", j.Status)
			t.Fail()
		}
		if reply != 123 {
			t.Log("expected 123:", reply)
			t.Fail()
		}
	})
}

type FakeJobStoreGetJobError struct {
	FakeJobStore
}

func (fjs *FakeJobStoreGetJobError) Get() (job.Job, error) {
	return job.Job{}, errors.New("jobstore-get-error")
}

func TestWorkerGetJobError(t *testing.T) {
	cnf := mqlessworker.Config{
		AppID:       "worker_test",
		WorkerCount: 1,
	}
	fjs := FakeJobStoreGetJobError{
		FakeJobStore: FakeJobStore{
			[]job.Job{{
				JobID:     1,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("[]"),
			}, {
				JobID:     2,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypePrintMsg,
				JsonData:  []byte("{\"msg\":\"message-1\"}"),
			}, {
				JobID:     3,
				Status:    job.JobStatusQueued,
				JobTypeID: JobTypeSliceLen,
				JsonData:  []byte("{\"msg\":\"message-2\"}"),
			}}},
	}
	w := mqlessworker.NewWorker(cnf, &fjs)
	w.Run()

	log.Println("notify here")
	w.Notify()

	// If we don't have infinite loop, this test is done.
}
