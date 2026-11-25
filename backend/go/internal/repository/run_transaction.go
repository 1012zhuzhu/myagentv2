package repository

import (
	"database/sql"
	"fmt"
	"my-agent-v2/backend/go/internal/domain"
	"time"
)

type IdempotencyRecord struct {
    Key     string
    RequestHash string
    RunID      string
    CreatedAt  time.Time
}

func CreateRunWithOutbox(
    db *sql.DB,
    run domain.Run,
    outbox domain.Outbox,
    indempotency IdempotencyRecord,
) error {
    // 1. 开启事务
    tx, err := db.Begin()
    if err != nil {
        return err
    }

    // 2. 确保发生错误时回滚
    defer tx.Rollback()

    // 3. 将同一个 tx 注入两个 Repository
    runRepo := NewRunRepository(tx)
    outboxRepo := NewOutboxRepository(tx)

    // 4. 创建 Run
    if err := runRepo.CreateRun(run); err != nil {
        return fmt.Errorf("create run: %w", err)
    }

    // 5. 创建 Outbox
    if err := outboxRepo.CreateOutbox(outbox); err != nil {
        return fmt.Errorf("create outbox: %w", err)
    }

    _, err = tx.Exec(`
        INSERT INTO idempotency_records (
            idempotency_key,
            request_hash,
            run_id,
            created_at
        ) VALUES (?, ?, ?, ?)
    `,  indempotency.Key,
        indempotency.RequestHash,
        indempotency.RunID,
        indempotency.CreatedAt.UTC().Format(time.RFC3339),
    ) 
    if err != nil {
        return  fmt.Errorf("create idempotency record: %w", err)
    }

    // 6. 两次操作都成功，提交事务
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("commit transaction: %w", err)
    }

    return nil
}

func GetIdempotencyRecord(
    db *sql.DB,
    key string,
) (IdempotencyRecord, error) {
    var record IdempotencyRecord
    var createAt string

    err := db.QueryRow(`
        SELECT idempotency_key, request_hash, run_id, created_at
        FROM idempotency_records
        WHERE idempotency_key = ?
    `,key).Scan(
        &record.Key,
        &record.RequestHash,
        &record.RunID,
        &createAt,
    )
    if err != nil {
		return IdempotencyRecord{}, err
	}

	record.CreatedAt, err = time.Parse(time.RFC3339, createAt)
	if err != nil {
		return IdempotencyRecord{}, err
	}

	return record, nil
}