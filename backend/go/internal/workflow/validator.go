
package workflow

import (
	"fmt"

	"my-agent-v2/backend/go/internal/domain"
)

func ValidateWorkflow(wf domain.Workflow) error {
	seen := make(map[string]struct{})
	seenEdge := make(map[string]struct{})

	startCount := 0

	if len(wf.Nodes) == 0 {
		return fmt.Errorf("workflow must contain at least one node")
	}

	if len(wf.Edges) == 0 {
		return fmt.Errorf("workflow must contain at least one edge")
	}

	// 1. 校验节点
	for _, node := range wf.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node ID cannot be empty")
		}

		if _, exists := seen[node.ID]; exists {
			return fmt.Errorf("duplicate node ID: %s", node.ID)
		}

		seen[node.ID] = struct{}{}

		switch node.Type {
		case domain.NodeTypeStart:
			startCount++
		case domain.NodeTypeAgent, domain.NodeTypeEnd:
			// 当前支持的节点类型
		default:
			return fmt.Errorf("unsupported node type: %s", node.Type)
		}
	}

	if startCount != 1 {
		return fmt.Errorf(
			"workflow must contain exactly one start node, got %d",
			startCount,
		)
	}

	// 2. 校验边
	for _, edge := range wf.Edges {
		if edge.ID == "" {
			return fmt.Errorf("edge ID cannot be empty")
		}

		if _, exists := seenEdge[edge.ID]; exists {
			return fmt.Errorf("duplicate edge ID: %s", edge.ID)
		}

		seenEdge[edge.ID] = struct{}{}

		if edge.Source == "" {
			return fmt.Errorf("edge source cannot be empty")
		}

		if _, exists := seen[edge.Source]; !exists {
			return fmt.Errorf(
				"edge references unknown source node: %s",
				edge.Source,
			)
		}

		if edge.Target == "" {
			return fmt.Errorf("edge target cannot be empty")
		}

		if _, exists := seen[edge.Target]; !exists {
			return fmt.Errorf(
				"edge references unknown target node: %s",
				edge.Target,
			)
		}
	}

	return nil
}
