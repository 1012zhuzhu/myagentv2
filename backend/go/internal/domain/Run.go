package domain

import "time"

type Run struct {
	RunID          string
	ConversationID string
	WorkflowID     string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
