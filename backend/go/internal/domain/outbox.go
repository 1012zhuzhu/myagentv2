package domain

import "time"

type Outbox struct {
	EventID     string     `json:"event_id"`
	RunID       string     `json:"run_id"`
	EventType   string     `json:"event_type"`
	Payload     string     `json:"payload"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at"`
}
