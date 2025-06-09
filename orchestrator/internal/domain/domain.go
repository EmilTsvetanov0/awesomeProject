package domain

import "encoding/json"

type Notification struct {
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

type RunnerMsg struct {
	Id     string `json:"id"`
	Action string `json:"action"`
}

type RunnerHb struct {
	Id        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

type RunnerTermReq struct {
	Id        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error"`
}

type KafkaEvent struct {
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
}
