package repository

import "errors"

// ErrRunStateConflict 表示 Run 当前状态不允许执行本次状态变更。
var ErrRunStateConflict = errors.New("run state conflict")