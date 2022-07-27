package mqlessworker

// Config worker에 관한 설정들
type Config struct {
	// AppID Worker의 카운트
	AppID string

	// WorkerCount worker goroutine 개수
	WorkerCount int
}
