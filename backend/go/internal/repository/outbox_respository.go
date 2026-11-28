package repository

import (
	"context"
	"database/sql"
	"my-agent-v2/backend/go/internal/domain"
	"time"
)

type OutboxRepository struct {
	db DBTX
}

func NewOutboxRepository(db DBTX) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) CreateOutbox(outbox domain.Outbox) error {
	var publishedAt interface{}

	// 如果已经发布，保存发布时间；否则保存 NULL
	if outbox.PublishedAt != nil {
		publishedAt = outbox.PublishedAt.Format(time.RFC3339)
	}

	_, err := r.db.Exec(`
		INSERT INTO outbox (
			event_id,
			run_id,
			event_type,
			payload,
			status,
			created_at,
			published_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		outbox.EventID,
		outbox.RunID,
		outbox.EventType,
		outbox.Payload,
		outbox.Status,
		outbox.CreatedAt.Format(time.RFC3339),
		publishedAt,
	)

	if err != nil {
		return err
	}

	return nil
}   

func (r *OutboxRepository) GetPendingOutboxes(ctx context.Context) ([]domain.Outbox,error){
	var Outbox []domain.Outbox

	rows, err := r.db.QueryContext(ctx,`
		SELECT event_id, run_id, event_type, payload, status, created_at, published_at
		FROM outbox
		WHERE status = ?
		ORDER BY created_at ASC
	`, "pending")

	if err != nil {
		return nil,err
	}

	defer rows.Close()

	for rows.Next() {
		var outbox domain.Outbox
		var createdAt string
		var publishedAt sql.NullString

		if err := rows.Scan(
			&outbox.EventID,
			&outbox.RunID,
			&outbox.EventType,
			&outbox.Payload,
			&outbox.Status,
			&createdAt,
			&publishedAt,
		); err != nil {
			return nil,err
		}

		outbox.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
		if err != nil {
			return nil, err
		}

		// published_at 允许为 NULL
		if publishedAt.Valid {
			parsedTime, err := time.Parse(time.RFC3339, publishedAt.String)
			if err != nil {
				return nil, err
			}
			outbox.PublishedAt = &parsedTime
		}

		// 将当前记录加入结果切片
		Outbox = append(Outbox, outbox)
	}


	if err := rows.Err(); err != nil {
		return nil, err
	}

	return Outbox, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
		publish := publishedAt.Format(time.RFC3339)

		result,err := r.db.ExecContext(ctx,
			`
			UPDATE outbox
			SET status =?, published_at = ?
			WHERE event_id = ? AND status = ?
			`,"published",publish,eventID,"pending",
		)
		if err != nil {
			return err
		}

		rowsAffected,err := result.RowsAffected()
		if err != nil {
			return err
		}

		if rowsAffected == 0 {
			return sql.ErrNoRows
		}
		return nil
}