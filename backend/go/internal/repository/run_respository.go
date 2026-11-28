package repository

import (
	"fmt"
	"my-agent-v2/backend/go/internal/domain"
	"time"
)

type RunRepository struct {
	db DBTX
}

func NewRunRepository(db DBTX) *RunRepository {
	return &RunRepository{db: db}
}

func (r *RunRepository) CreateRun(run domain.Run) error {
	_, err := r.db.Exec(`
		INSERT INTO runs (
			run_id, conversation_id, workflow_id,
			status, result, error, created_at, updated_at
		) VALUES (?,?,?,?,?,?,?,?)
	`,
		run.RunID,
		run.ConversationID,
		run.WorkflowID,
		run.Status,
		run.Result,
		run.Error,
		run.CreatedAt.Format(time.RFC3339),
		run.UpdatedAt.Format(time.RFC3339),
	)
	return err
}

func (r *RunRepository) GetRunByID(runID string) (domain.Run, error) {
	var run domain.Run
	var createdAt, updatedAt string

	err := r.db.QueryRow(`
		SELECT
			run_id, conversation_id, workflow_id,
			status, result, error, created_at, updated_at
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&run.RunID,
		&run.ConversationID,
		&run.WorkflowID,
		&run.Status,
		&run.Result,
		&run.Error,
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

// ClaimPendingRun 原子领取一个待执行的 Run
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
// func (r *RunRepository) UpdateStatus(runID string, status string) error {
// 	_, err := r.db.Exec(`
// 		UPDATE runs
// 		SET status = ?, updated_at = ?
// 		WHERE run_id = ?
// 	`,
// 		status,
// 		time.Now().UTC().Format(time.RFC3339),
// 		runID,
// 	)
// 	return err
// }

// SaveResult 保存成功执行结果
// func (r *RunRepository) SaveResult(runID string, result string) error {
// 	_, err := r.db.Exec(`
// 		UPDATE runs
// 		SET result = ?, updated_at = ?
// 		WHERE run_id = ?
// 	`,
// 		result,
// 		time.Now().UTC().Format(time.RFC3339),
// 		runID,
// 	)
// 	return err
// }

// CompleteRun 原子保存执行结果并更新 Run 状态
func (r *RunRepository) CompleteRun(
    runID string,
    result string,
) error {
    updateResult, err := r.db.Exec(`
        UPDATE runs
        SET result = ?, status = ?, updated_at = ?
        WHERE run_id = ? AND status = ?
    `,
        result,
        "succeeded",
        time.Now().UTC().Format(time.RFC3339),
        runID,
        "running",
    )
    if err != nil {
        return fmt.Errorf("complete run failed: %w", err)
    }

    rowsAffected, err := updateResult.RowsAffected()
    if err != nil {
        return fmt.Errorf("get complete run rows affected: %w", err)
    }

	if rowsAffected != 1 {
		return fmt.Errorf(
			"%w: complete run rejected, run_id=%s, affected=%d",
			ErrRunStateConflict,
			runID,
			rowsAffected,
		)
	}

    return nil
}


// MarkRunFailed 原子保存失败原因并更新状态
func (r *RunRepository) MarkRunFailed(
    runID string,
    errorMessage string,
) error {
    result, err := r.db.Exec(`
        UPDATE runs
        SET status = ?, error = ?, updated_at = ?
        WHERE run_id = ? AND status = ?
    `,
        "failed",
        errorMessage,
        time.Now().UTC().Format(time.RFC3339),
        runID,
        "running",
    )
    if err != nil {
        return fmt.Errorf("mark run failed: %w", err)
    }

    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("get failed run rows affected: %w", err)
    }

    if rowsAffected != 1 {
		return fmt.Errorf(
			"%w: mark run failed rejected, run_id=%s, affected=%d",
			ErrRunStateConflict,
			runID,
			rowsAffected,
		)
	}

    return nil
}


// RequestRunCancellation 请求取消一个 Run。
// pending -> cancelled
// running -> cancelling
// 其他状态保持不变，返回当前状态。
func (r *RunRepository) RequestRunCancellation(
	runID string,
) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// 尚未执行：直接取消。
	result, err := r.db.Exec(`
		UPDATE runs
		SET status = ?, updated_at = ?
		WHERE run_id = ? AND status = ?
	`,
		"cancelled",
		now,
		runID,
		"pending",
	)
	if err != nil {
		return "", fmt.Errorf("cancel pending run: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("get pending cancellation rows affected: %w", err)
	}

	if rowsAffected == 1 {
		return "cancelled", nil
	}

	// 已经开始执行：先记录取消意图。
	result, err = r.db.Exec(`
		UPDATE runs
		SET status = ?, updated_at = ?
		WHERE run_id = ? AND status = ?
	`,
		"cancelling",
		now,
		runID,
		"running",
	)
	if err != nil {
		return "", fmt.Errorf("cancel running run: %w", err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("get running cancellation rows affected: %w", err)
	}

	if rowsAffected == 1 {
		return "cancelling", nil
	}

	// 可能已经结束，也可能正在取消。
	// 查询当前状态，让上层决定如何响应。
	run, err := r.GetRunByID(runID)
	if err != nil {
		return "", fmt.Errorf("get run after cancellation request: %w", err)
	}

	return run.Status, nil
}


// MarkRunCancelled 将正在取消的 Run 标记为已取消。
func (r *RunRepository) MarkRunCancelled(runID string) error {
	result, err := r.db.Exec(`
		UPDATE runs
		SET status = ?, updated_at = ?
		WHERE run_id = ? AND status = ?
	`,
		"cancelled",
		time.Now().UTC().Format(time.RFC3339),
		runID,
		"cancelling",
	)
	if err != nil {
		return fmt.Errorf("mark run cancelled: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get cancelled run rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"%w: mark run cancelled rejected, run_id=%s, affected=%d",
			ErrRunStateConflict,
			runID,
			rowsAffected,
		)
	}

	return nil
}
