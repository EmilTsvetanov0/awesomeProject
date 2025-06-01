package domain

import "encoding/json"

type RunnerMsg struct {
	Id     string `json:"id"`
	Action string `json:"action"`
}

type StatusReq struct {
	Id string `json:"id"`
}

type KafkaEvent struct {
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
}

type LoggableKafkaEvent struct {
	AggregateType string `json:"aggregate_type"`
	AggregateID   string `json:"aggregate_id"`
	EventType     string `json:"event_type"`
	Payload       string `json:"payload,omitempty"`
}
