package repository

import (
	"database/sql"
	"fmt"
	"time"
)

// RecoverInterruptedRuns 恢复上一次进程异常退出时遗留的 Run。
func RecoverInterruptedRuns(db *sql.DB) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin recovery transaction: %w", err)
	}
	defer tx.Rollback()

	// 找出所有上次执行中断、仍处于 running 的 Run。
	rows, err := tx.Query(`
		SELECT run_id
		FROM runs
		WHERE status = ?
	`, "running")
	if err != nil {
		return 0, fmt.Errorf("query interrupted runs: %w", err)
	}

	var runIDs []string

	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan interrupted run: %w", err)
		}
		runIDs = append(runIDs, runID)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate interrupted runs: %w", err)
	}
	
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close interrupted runs rows: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	recovered := 0

	for _, runID := range runIDs {
		// 先把 Run 恢复为 pending。
		result, err := tx.Exec(`
			UPDATE runs
			SET status = ?, updated_at = ?
			WHERE run_id = ? AND status = ?
		`, "pending", now, runID, "running")
		if err != nil {
			return 0, fmt.Errorf("recover run %s: %w", runID, err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("check recovered run %s: %w", runID, err)
		}
		if affected == 0 {
			continue
		}

		// 重新开放已经发布过的 Outbox，让 Publisher 再次投递。
		_, err = tx.Exec(`
			UPDATE outbox
			SET status = ?, published_at = NULL
			WHERE run_id = ? AND status = ?
		`, "pending", runID, "published")
		if err != nil {
			return 0, fmt.Errorf("recover outbox for run %s: %w", runID, err)
		}

		recovered++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit recovery transaction: %w", err)
	}

	return recovered, nil
}
