package repository

import (
    "database/sql"
    "fmt"
    "my-agent-v2/backend/go/internal/domain"
)

func CreateRunWithOutbox(
    db *sql.DB,
    run domain.Run,
    outbox domain.Outbox,
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

    // 6. 两次操作都成功，提交事务
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("commit transaction: %w", err)
    }

    return nil
}