package job

import "time"

type Handler func([]byte) error

type ErrHandler func(err error)

type JobStatus string

const (
	JobStatusQueued   = "queued"
	JobStatusDoing    = "doing"
	JobStatusFinished = "finished"
	JobStatusError    = "error"
)

type Job struct {
	JobID     uint64
	TimeStamp time.Time
	Status    JobStatus

	JobTypeID string
	JsonData  []byte

	Hdlr  Handler
	Ehdlr ErrHandler
}

type JobStore interface {
	Get() (Job, error)
	Put(Job) error
}
