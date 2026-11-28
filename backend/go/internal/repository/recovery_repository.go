
package repository

import (
	"database/sql"
	"fmt"
	"time"
)

// RecoverInterruptedRuns 恢复上一次进程异常退出时遗留的 Run。
//
// running -> pending：
//   - 恢复未完成的执行任务。
//   - 将对应已发布的 Outbox 重新开放，等待 Publisher 重发。
//
// cancelling -> cancelled：
//   - 用户已经请求取消。
//   - 不再重新执行这条 Run。
//
// 所有恢复操作在同一个 SQLite 事务中完成。
func RecoverInterruptedRuns(db *sql.DB) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin recovery transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	recovered := 0

	// 1. 先处理上次进程退出时遗留的 cancelling。
	// 这类 Run 已经收到用户取消请求，不应该重新执行。
	cancelResult, err := tx.Exec(`
		UPDATE runs
		SET status = ?, updated_at = ?
		WHERE status = ?
	`,
		"cancelled",
		now,
		"cancelling",
	)
	if err != nil {
		return 0, fmt.Errorf(
			"recover cancelling runs: %w",
			err,
		)
	}

	cancelledCount, err := cancelResult.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"count recovered cancelled runs: %w",
			err,
		)
	}

	recovered += int(cancelledCount)

	// 2. 找出上次执行中断、仍处于 running 的 Run。
	rows, err := tx.Query(`
		SELECT run_id
		FROM runs
		WHERE status = ?
	`, "running")
	if err != nil {
		return 0, fmt.Errorf(
			"query interrupted runs: %w",
			err,
		)
	}

	var runIDs []string

	for rows.Next() {
		var runID string

		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return 0, fmt.Errorf(
				"scan interrupted run: %w",
				err,
			)
		}

		runIDs = append(runIDs, runID)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf(
			"iterate interrupted runs: %w",
			err,
		)
	}

	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf(
			"close interrupted runs rows: %w",
			err,
		)
	}

	// 3. 恢复 running Run 和对应 Outbox。
	for _, runID := range runIDs {
		result, err := tx.Exec(`
			UPDATE runs
			SET status = ?, updated_at = ?
			WHERE run_id = ? AND status = ?
		`,
			"pending",
			now,
			runID,
			"running",
		)
		if err != nil {
			return 0, fmt.Errorf(
				"recover run %s: %w",
				runID,
				err,
			)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf(
				"check recovered run %s: %w",
				runID,
				err,
			)
		}

		if affected == 0 {
			continue
		}

		// 重新开放已经发布过的 Outbox，
		// 让 Publisher 后续重新投递。
		_, err = tx.Exec(`
			UPDATE outbox
			SET status = ?, published_at = NULL
			WHERE run_id = ? AND status = ?
		`,
			"pending",
			runID,
			"published",
		)
		if err != nil {
			return 0, fmt.Errorf(
				"recover outbox for run %s: %w",
				runID,
				err,
			)
		}

		recovered++
	}

	// 4. 所有状态恢复和 Outbox 更新一起提交。
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf(
			"commit recovery transaction: %w",
			err,
		)
	}

	return recovered, nil
}
