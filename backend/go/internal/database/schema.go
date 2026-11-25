package database

import (
	"database/sql"
)

func InitSchema(db *sql.DB) error {
	createRunSQL := `
		CREATE TABLE IF NOT EXISTS runs (
			run_id TEXT PRIMARY KEY,
			conversation_id TEXT NOT NULL,
			workflow_id TEXT NOT NULL,
			status TEXT NOT NULL,
			result TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
	`

	if _, err := db.Exec(createRunSQL); err != nil {
		return err
	}

	// 兼容旧数据库：检查 result 和 error 字段是否存在
	rows, err := db.Query(`PRAGMA table_info(runs)`)
	if err != nil {
		return err
	}

	hasResult := false
	hasError := false

	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue interface{}

		if err := rows.Scan(
			&cid,
			&name,
			&columnType,
			&notNull,
			&defaultValue,
			&primaryKey,
		); err != nil {
			rows.Close()
			return err
		}

		switch name {
		case "result":
			hasResult = true
		case "error":
			hasError = true
		}
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if !hasResult {
		if _, err := db.Exec(`
			ALTER TABLE runs
			ADD COLUMN result TEXT NOT NULL DEFAULT ''
		`); err != nil {
			return err
		}
	}

	if !hasError {
		if _, err := db.Exec(`
			ALTER TABLE runs
			ADD COLUMN error TEXT NOT NULL DEFAULT ''
		`); err != nil {
			return err
		}
	}

	createOutBoxSQL := `
		CREATE TABLE IF NOT EXISTS outbox (
			event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			published_at TEXT,
			FOREIGN KEY(run_id) REFERENCES runs(run_id)
		);
	`

	if _, err := db.Exec(createOutBoxSQL); err != nil {
		return err
	}

	createIdempotencyRecords:= `
		CREATE TABLE IF NOT EXISTS idempotency_records (
			idempotency_key TEXT PRIMARY KEY,
			request_hash	TEXT NOT NULL,
			run_id			TEXT NOT NULL,
			created_at		TEXT NOT NULL,
			FOREIGN KEY (run_id) REFERENCES runs(run_id)
		);
	`
	if _, err := db.Exec(createIdempotencyRecords); err != nil {
		return err
	}

	return nil
}
