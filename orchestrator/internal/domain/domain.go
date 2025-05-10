package domain

type Notification struct {
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

type RunnerMsg struct {
	Id     string `json:"id"`
	Action string `json:"action"`
}
