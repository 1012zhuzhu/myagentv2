package repository

import (
	"my-agent-v2/backend/go/internal/domain"
	"time"
)

type RunRepository struct {
	db DBTX
}

func NewRunRepository(db DBTX) *RunRepository {
	return &RunRepository{
		db: db,
	}
}

func (r *RunRepository) CreateRun(run domain.Run) error {
	_, err := r.db.Exec(`
		INSERT INTO runs (
			run_id,
			conversation_id,
			workflow_id,
			status,
			created_at,
			updated_at
		) VALUES (?,?,?,?,?,?)
	`,
		run.RunID,
		run.ConversationID,
		run.WorkflowID,
		run.Status,
		run.CreatedAt.Format(time.RFC3339),
		run.UpdatedAt.Format(time.RFC3339),
	)

	return err
}

func (r *RunRepository) GetRunByID(runID string) (domain.Run, error) {
	var run domain.Run
	var createdAt string
	var updatedAt string

	err := r.db.QueryRow(`
		SELECT
			run_id,
			conversation_id,
			workflow_id,
			status,
			created_at,
			updated_at
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&run.RunID,
		&run.ConversationID,
		&run.WorkflowID,
		&run.Status,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		return domain.Run{}, err
	}

	run.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return domain.Run{}, err
	}

	run.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return domain.Run{}, err
	}

	return run, nil
}

// ClaimPendingRun 原子地领取一个待执行的 Run
func (r *RunRepository) ClaimPendingRun(runID string) (bool, error) {
	result, err := r.db.Exec(`
        UPDATE runs
        SET status = ?, updated_at = ?
        WHERE run_id = ? AND status = ?
    `,
		"running",
		time.Now().UTC().Format(time.RFC3339),
		runID,
		"pending",
	)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected == 1, nil
}

// UpdateStatus 更新 Run 状态
func (r *RunRepository) UpdateStatus(runID string, status string) error {
	_, err := r.db.Exec(`
        UPDATE runs
        SET status = ?, updated_at = ?
        WHERE run_id = ?
    `,
		status,
		time.Now().UTC().Format(time.RFC3339),
		runID,
	)
	return err
}
