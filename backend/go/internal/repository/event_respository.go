
package repository

import (
	"database/sql"
	"fmt"
	"my-agent-v2/backend/go/internal/domain"
	"time"
)

type EventRepository struct {
	db *sql.DB
}

func NewEventRepository(db *sql.DB) *EventRepository {
	return &EventRepository{db: db}
}

// AppendEvent 为当前 Run 分配下一个事件序号，并持久化事件。
// 序号分配和事件插入必须在同一个事务中完成。
func (r *EventRepository) AppendEvent(
	event *domain.WorkflowEvent,
) error {
	if event == nil {
		return fmt.Errorf("workflow event is nil")
	}

	if event.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	if event.EventType == "" {
		return fmt.Errorf("event_type is required")
	}

	payload := event.Payload
	if payload == "" {
		payload = "{}"
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin append event transaction: %w", err)
	}
	defer tx.Rollback()

	// 在当前事务中读取该 Run 已有的最大事件序号。
	var maxSeq int64
	err = tx.QueryRow(`
		SELECT COALESCE(MAX(event_seq), 0)
		FROM workflow_events
		WHERE run_id = ?
	`, event.RunID).Scan(&maxSeq)
	if err != nil {
		return fmt.Errorf("query latest event sequence: %w", err)
	}

	nextSeq := maxSeq + 1
	now := time.Now().UTC()

	var nodeID interface{}
	if event.NodeID != "" {
		nodeID = event.NodeID
	}

	result, err := tx.Exec(`
		INSERT INTO workflow_events (
			run_id,
			event_seq,
			event_type,
			node_id,
			payload,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`,
		event.RunID,
		nextSeq,
		event.EventType,
		nodeID,
		payload,
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert workflow event: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get workflow event id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workflow event: %w", err)
	}

	// 只有提交成功后才回填事件信息。
	event.ID = id
	event.EventSeq = nextSeq
	event.Payload = payload
	event.CreatedAt = now

	return nil
}

// ListEvents 查询指定 Run 在 afterSeq 之后的事件。
// afterSeq 通常来自 SSE 的 Last-Event-ID。
func (r *EventRepository) ListEvents(
	runID string,
	afterSeq int64,
	limit int,
) ([]domain.WorkflowEvent, error) {
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}

	if afterSeq < 0 {
		return nil, fmt.Errorf("afterSeq cannot be negative")
	}

	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.Query(`
		SELECT
			id,
			run_id,
			event_seq,
			event_type,
			node_id,
			payload,
			created_at
		FROM workflow_events
		WHERE run_id = ? AND event_seq > ?
		ORDER BY event_seq ASC
		LIMIT ?
	`, runID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("query workflow events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.WorkflowEvent, 0)

	for rows.Next() {
		var event domain.WorkflowEvent
		var nodeID sql.NullString
		var createdAt string

		if err := rows.Scan(
			&event.ID,
			&event.RunID,
			&event.EventSeq,
			&event.EventType,
			&nodeID,
			&event.Payload,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow event: %w", err)
		}

		if nodeID.Valid {
			event.NodeID = nodeID.String
		}

		event.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse workflow event time: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow events: %w", err)
	}

	return events, nil
}
