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
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
	`

	
	if _, err := db.Exec(createRunSQL); err != nil {
		return err
	}

	createOutBoxSQL := `
		CREATE TABLE IF NOT EXISTS outbox (
			event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			status   TEXT NOT NULL,
			created_at TEXT NOT NULL,
			published_at TEXT,
			FOREIGN KEY(run_id) REFERENCES runs(run_id)
		);
	`

	
	if _, err := db.Exec(createOutBoxSQL); err != nil {
		return err
	}

	return nil
}