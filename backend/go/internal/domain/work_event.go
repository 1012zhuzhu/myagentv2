package domain

import "time"

type WorkflowEvent struct {
	ID        int64
	RunID     string
	EventSeq  int64
	EventType string
	NodeID    string
	Payload   string
	CreatedAt time.Time
}