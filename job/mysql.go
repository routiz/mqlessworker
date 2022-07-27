package job

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// host, port, username, password
type MySqlDbInfo struct {
	Host string
	Port int

	User, PW string

	DB string
}

type MySQLJobStore struct {
	db        *sqlx.DB
	tableName string
}

type sqlJob struct {
	JobID     uint64 `db:"id"`
	TimeStamp string `db:"timestamp"`
	Status    string `db:"status"`

	JobTypeID string         `db:"job_type"`
	JsonData  sql.NullString `db:"json_data"`
}

func sqlJobFromJob(j Job) sqlJob {
	ts := j.TimeStamp.UTC().Format(time.RFC3339Nano)
	var jsd sql.NullString
	if j.JsonData != nil {
		jsd = sql.NullString{
			String: string(j.JsonData),
			Valid:  true,
		}
	}
	return sqlJob{
		JobID:     j.JobID,
		TimeStamp: ts,
		Status:    string(j.Status),
		JobTypeID: j.JobTypeID,
		JsonData:  jsd,
	}
}

func jobFromSql(mjob sqlJob) Job {
	ts, err := time.Parse(time.RFC3339Nano, mjob.TimeStamp)
	if err != nil {
		log.Println("failed to parse timestamp")
	}
	var jsd []byte = nil
	if mjob.JsonData.Valid {
		jsd = []byte(mjob.JsonData.String)
	}
	return Job{
		JobID:     mjob.JobID,
		TimeStamp: ts,
		Status:    JobStatus(mjob.Status),
		JobTypeID: mjob.JobTypeID,
		JsonData:  jsd,
	}
}

const ensureTableStmtTmpl = `CREATE TABLE IF NOT EXISTS %s
(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
 timestamp VARCHAR(35) NOT NULL,
 status VARCHAR(16) NOT NULL,
 job_type VARCHAR(32) NOT NULL,
 json_data JSON);`

func NewMySqlJobStore(info MySqlDbInfo, appID string) (JobStore, error) {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s",
		info.User, info.PW, info.Host, info.Port, info.DB,
	)
	db, err := sqlx.Open("mysql", dsn)

	tbname := appID + "_jobs"
	ensureTableStmt := fmt.Sprintf(ensureTableStmtTmpl, tbname)
	_, err = db.Exec(ensureTableStmt)
	if err != nil {
		return nil, err
	}

	return &MySQLJobStore{
		db:        db,
		tableName: tbname,
	}, nil
}

const selStmtTmpl = `SELECT * FROM %s
WHERE status = ?
ORDER BY timestamp ASC
LIMIT 1;`

func (js *MySQLJobStore) Get() (Job, error) {
	selStmt := fmt.Sprintf(selStmtTmpl, js.tableName)
	tx, err := js.db.Beginx()
	defer func() {
		tx.Commit()
	}()
	if err != nil {
		return Job{}, err
	}
	r := tx.QueryRowx(selStmt, JobStatusQueued)
	sqlj := sqlJob{}
	err = r.StructScan(&sqlj)
	sqlj.Status = JobStatusDoing
	js.updateTx(sqlj, tx)
	if err == sql.ErrNoRows {
		return Job{}, nil
	}
	if err != nil {
		return Job{}, err
	}

	j := jobFromSql(sqlj)
	return j, nil
}

const insertStmtTmpl = `INSERT INTO %s
(timestamp, status, job_type, json_data)
VALUES (?, ?, ?, ?);`

func (js *MySQLJobStore) insert(j Job) error {
	sqlj := sqlJobFromJob(j)
	insertStmt := fmt.Sprintf(insertStmtTmpl, js.tableName)
	_, err := js.db.Exec(
		insertStmt,
		time.Now().UTC().Format(time.RFC3339Nano),
		string(JobStatusQueued),
		sqlj.JobTypeID,
		sqlj.JsonData)
	return err
}

const updateStmtTmpl = `UPDATE %s
SET status = ?
WHERE id = ?;`

func (js *MySQLJobStore) update(j Job) error {
	updateStmt := fmt.Sprintf(updateStmtTmpl, js.tableName)
	_, err := js.db.Exec(updateStmt, j.Status, j.JobID)
	return err
}

func (js *MySQLJobStore) updateTx(j sqlJob, tx *sqlx.Tx) error {
	updateStmt := fmt.Sprintf(updateStmtTmpl, js.tableName)
	_, err := tx.Exec(updateStmt, j.Status, j.JobID)
	return err
}

func (js *MySQLJobStore) Put(j Job) error {
	if j.JobID == 0 {
		return js.insert(j)

	}
	return js.update(j)
}
