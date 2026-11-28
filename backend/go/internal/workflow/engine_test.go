
package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/executor"
)

// memoryEventWriter 用于测试，不连接真实数据库。
type memoryEventWriter struct {
	events []domain.WorkflowEvent
}

func (w *memoryEventWriter) AppendEvent(
	event *domain.WorkflowEvent,
) error {
	w.events = append(w.events, *event)
	return nil
}

func TestEngine_Execute_HelloWorld(t *testing.T) {
	// 1. 创建 Registry
	registry := executor.NewRegistry()

	// 2. 注册 StartExecutor 和 EndExecutor
	if err := registry.Register(
		"startNode",
		executor.NewStartExecutor(),
	); err != nil {
		t.Fatalf("register start executor failed: %v", err)
	}

	if err := registry.Register(
		"endNode",
		executor.NewEndExecutor(),
	); err != nil {
		t.Fatalf("register end executor failed: %v", err)
	}

	// 3. 创建内存事件写入器和 Engine
	eventWriter := &memoryEventWriter{}
	engine := NewEngine(registry, eventWriter)

	// 4. 创建 StartNode -> EndNode 的 Workflow
	wf := domain.Workflow{
		ID: "workflow1",
		Nodes: []domain.Node{
			{
				ID:   "start-1",
				Type: "startNode",
			},
			{
				ID:   "end-1",
				Type: "endNode",
			},
		},
		Edges: []domain.Edge{
			{
				ID:           "edge-1",
				Source:       "start-1",
				Target:       "end-1",
				SourceHandle: "success",
			},
		},
	}

	// 5. 执行 Engine，输入 Hello World
	result, err := engine.Execute(
		context.Background(),
		wf,
		"run-1",
		"Hello World",
	)
	if err != nil {
		t.Fatalf("workflow execution failed: %v", err)
	}

	// 6. 判断最终输出是否等于 Hello World
	got, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result)
	}

	if got != "Hello World" {
		t.Fatalf("expected Hello World, got %s", got)
	}

	// 7. 验证节点事件
	expectedTypes := []string{
		"node.started",
		"node.completed",
		"node.started",
		"node.completed",
	}

	if len(eventWriter.events) != len(expectedTypes) {
		t.Fatalf(
			"expected %d events, got %d",
			len(expectedTypes),
			len(eventWriter.events),
		)
	}

	for i, expectedType := range expectedTypes {
		event := eventWriter.events[i]

		if event.RunID != "run-1" {
			t.Errorf(
				"event %d: expected run_id run-1, got %s",
				i,
				event.RunID,
			)
		}

		if event.EventType != expectedType {
			t.Errorf(
				"event %d: expected %s, got %s",
				i,
				expectedType,
				event.EventType,
			)
		}

		if event.NodeID == "" {
			t.Errorf("event %d: node_id should not be empty", i)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
			t.Errorf("event %d: invalid JSON payload: %v", i, err)
		}
	}
}
