
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/executor"
)

// EventWriter 定义 Engine 所需的最小事件写入能力。
// repository.EventRepository 实现了这个接口。
type EventWriter interface {
	AppendEvent(event *domain.WorkflowEvent) error
}

type Engine struct {
	registry  *executor.Registry
	eventRepo EventWriter
	maxSteps  int
}

func NewEngine(
	registry *executor.Registry,
	eventRepo EventWriter,
) *Engine {
	return &Engine{
		registry:  registry,
		eventRepo: eventRepo,
		maxSteps:  50,
	}
}

func (e *Engine) appendNodeEvent(
	runID string,
	nodeID string,
	eventType string,
	payload interface{},
) error {
	if e.eventRepo == nil {
		return fmt.Errorf("event writer is not configured")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}

	event := &domain.WorkflowEvent{
		RunID:     runID,
		NodeID:    nodeID,
		EventType: eventType,
		Payload:   string(payloadBytes),
	}

	if err := e.eventRepo.AppendEvent(event); err != nil {
		return fmt.Errorf("append workflow event %s: %w", eventType, err)
	}

	return nil
}

func (e *Engine) Execute(
	ctx context.Context,
	wf domain.Workflow,
	runID string,
	input interface{},
) (interface{}, error) {

	log.Printf(
		"[run_id=%s] workflow execution started nodes=%d edges=%d",
		runID,
		len(wf.Nodes),
		len(wf.Edges),
	)

	// 1. 校验 Workflow
	if err := ValidateWorkflow(wf); err != nil {
		log.Printf("[run_id=%s] workflow validation failed: %v", runID, err)
		return nil, fmt.Errorf("invalid workflow: %w", err)
	}

	// 2. 初始化执行上下文
	execCtx := &domain.ExecutionContext{
		RunID:       runID,
		Input:       input,
		NodeInputs:  make(map[string]interface{}),
		NodeOutputs: make(map[string]interface{}),
		Variables:   make(map[string]interface{}),
	}

	// 3. 建立 Node ID 到 Node 指针的映射
	nodeMap := make(map[string]*domain.Node)

	for i := range wf.Nodes {
		node := &wf.Nodes[i]
		nodeMap[node.ID] = node
	}

	// 4. 查找 StartNode
	var startNode *domain.Node

	for i := range wf.Nodes {
		if wf.Nodes[i].Type == domain.NodeTypeStart {
			startNode = &wf.Nodes[i]
			break
		}
	}

	if startNode == nil {
		return nil, fmt.Errorf("start node not found")
	}

	currentNode := startNode
	steps := 0

	// 5. 开始执行 Workflow
	for {
		if err := ctx.Err(); err != nil {
			log.Printf(
				"[run_id=%s] workflow execution canceled: %v",
				runID,
				err,
			)
			return nil, fmt.Errorf("workflow execution canceled: %w", err)
		}

		if steps >= e.maxSteps {
			return nil, fmt.Errorf(
				"workflow execution exceeded max step: %d",
				e.maxSteps,
			)
		}

		steps++

		// 5.1 获取当前节点 Executor
		nodeExecutor, err := e.registry.Get(currentNode.Type)
		if err != nil {
			log.Printf(
				"[run_id=%s node_id=%s] get executor failed: %v",
				runID,
				currentNode.ID,
				err,
			)
			return nil, fmt.Errorf(
				"get executor for node %s: %w",
				currentNode.ID,
				err,
			)
		}

		// 5.2 持久化 node.started
		if err := e.appendNodeEvent(
			runID,
			currentNode.ID,
			"node.started",
			map[string]interface{}{
				"node_type": currentNode.Type,
				"step":      steps,
			},
		); err != nil {
			return nil, fmt.Errorf(
				"record node.started for node %s: %w",
				currentNode.ID,
				err,
			)
		}

		// 5.3 执行节点
		nodeStart := time.Now()

		log.Printf(
			"[run_id=%s node_id=%s] node execution started type=%s step=%d",
			runID,
			currentNode.ID,
			currentNode.Type,
			steps,
		)

		result, executeErr := nodeExecutor.Execute(
			ctx,
			*currentNode,
			execCtx,
		)

		duration := time.Since(nodeStart)

		if executeErr != nil {
			log.Printf(
				"[run_id=%s node_id=%s] node execution failed duration=%s error=%v",
				runID,
				currentNode.ID,
				duration,
				executeErr,
			)

			eventErr := e.appendNodeEvent(
				runID,
				currentNode.ID,
				"node.failed",
				map[string]interface{}{
					"duration_ms": duration.Milliseconds(),
					"error":       executeErr.Error(),
				},
			)

			if eventErr != nil {
				return nil, fmt.Errorf(
					"execute node %s failed: %v; record node.failed failed: %w",
					currentNode.ID,
					executeErr,
					eventErr,
				)
			}

			return nil, fmt.Errorf(
				"execute node %s: %w",
				currentNode.ID,
				executeErr,
			)
		}

		if result == nil {
			nodeErr := fmt.Errorf(
				"executor returned nil result for node %s",
				currentNode.ID,
			)

			eventErr := e.appendNodeEvent(
				runID,
				currentNode.ID,
				"node.failed",
				map[string]interface{}{
					"duration_ms": duration.Milliseconds(),
					"error":       nodeErr.Error(),
				},
			)

			if eventErr != nil {
				return nil, fmt.Errorf(
					"%v; record node.failed failed: %w",
					nodeErr,
					eventErr,
				)
			}

			return nil, nodeErr
		}

		// 5.4 节点执行成功，持久化 node.completed
		if err := e.appendNodeEvent(
			runID,
			currentNode.ID,
			"node.completed",
			map[string]interface{}{
				"duration_ms": duration.Milliseconds(),
			},
		); err != nil {
			return nil, fmt.Errorf(
				"record node.completed for node %s: %w",
				currentNode.ID,
				err,
			)
		}

		log.Printf(
			"[run_id=%s node_id=%s] node execution succeeded duration=%s",
			runID,
			currentNode.ID,
			duration,
		)

		// 5.5 保存当前节点输出
		execCtx.NodeOutputs[currentNode.ID] = result.Output

		// 5.6 EndNode：结束 Workflow
		if currentNode.Type == domain.NodeTypeEnd {
			log.Printf(
				"[run_id=%s] workflow execution completed steps=%d",
				runID,
				steps,
			)
			return result.Output, nil
		}

		// 5.7 查找下一条 Edge
		var nextEdge *domain.Edge

		for i := range wf.Edges {
			edge := &wf.Edges[i]

			if edge.Source == currentNode.ID &&
				edge.SourceHandle == result.NextHandle {
				nextEdge = edge
				break
			}
		}

		if nextEdge == nil {
			return nil, fmt.Errorf(
				"no matching edge found for node %s with handle %s",
				currentNode.ID,
				result.NextHandle,
			)
		}

		// 5.8 查找下一个节点
		nextNode, exists := nodeMap[nextEdge.Target]
		if !exists {
			return nil, fmt.Errorf(
				"target node %s not found",
				nextEdge.Target,
			)
		}

		log.Printf(
			"[run_id=%s] workflow transition from=%s to=%s handle=%s",
			runID,
			currentNode.ID,
			nextNode.ID,
			result.NextHandle,
		)

		// 5.9 将输出传递给下一个节点
		execCtx.NodeInputs[nextNode.ID] = result.Output

		currentNode = nextNode
	}
}
