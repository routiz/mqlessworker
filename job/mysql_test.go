package job_test

import (
	"testing"

	"github.com/routiz/mqlessworker/job"
)

func TestMySqlJobStore(t *testing.T) {
	jst, err := job.NewMySqlJobStore(
		job.MySqlDbInfo{
			Host: "127.0.0.1", Port: 13306,
			User: "test", PW: "test", DB: "test",
		},
		"mysql_worker_test",
	)
	if err != nil {
		t.Error(err.Error())
		t.Fatal("check test DB settings. user docker-compose.yml.")
	}

	if err := jst.Put(job.Job{
		JobTypeID: "test-job",
		JsonData:  []byte("[]"),
	}); err != nil {
		t.Error(err.Error())
	}

	j, err := jst.Get()
	if err != nil {
		t.Error(err.Error())
	}

	if j.Status != job.JobStatusDoing {
		t.Error("expected:", job.JobStatusDoing,
			"real:", j.Status)
	}

	// TODO DB에 queued status로 저장된 값을 확인할 수 있는 방법을
	// 강구할 것
	j.Status = job.JobStatusFinished
	if err := jst.Put(j); err != nil {
		t.Error(err.Error())
	}

	// JsonData를 NULL로 넣는 것 성공 확인. sql.NullString
	// 사용하지 않고 나이브하게 작성할 경우 동작하지 않음.
	if err := jst.Put(job.Job{
		JobTypeID: "test-job",
		JsonData:  nil,
	}); err != nil {
		t.Error(err.Error())
	}
}
