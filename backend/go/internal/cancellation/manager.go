
package cancellation

import (
	"errors"
	"sync"
)

var (
	ErrInvalidRunID       = errors.New("run_id cannot be empty")
	ErrInvalidCancelFunc  = errors.New("cancel function cannot be nil")
	ErrRunAlreadyRegistered = errors.New("run already registered")
)

type Manager struct {
	mu     sync.RWMutex
	active map[string]func()
}

func NewManager() *Manager {
	return &Manager{
		active: make(map[string]func()),
	}
}

// Register 登记一个正在执行的 Run。
func (m *Manager) Register(
	runID string,
	cancel func(),
) error {
	if runID == "" {
		return ErrInvalidRunID
	}

	if cancel == nil {
		return ErrInvalidCancelFunc
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.active[runID]; exists {
		return ErrRunAlreadyRegistered
	}

	m.active[runID] = cancel
	return nil
}

// Unregister 清理已经结束的 Run。
func (m *Manager) Unregister(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.active, runID)
}

// Cancel 取消指定 Run。
// 返回 true 表示找到了对应的取消函数并发送了取消信号。
func (m *Manager) Cancel(runID string) bool {
	m.mu.RLock()
	cancel, exists := m.active[runID]
	m.mu.RUnlock()

	if !exists {
		return false
	}

	cancel()
	return true
}
