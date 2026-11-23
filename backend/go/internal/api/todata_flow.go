package api

import (
	"fmt"

	"my-agent-v2/backend/go/internal/domain"
)

// 前端 React Flow 的数据结构
type FrontendFlow struct {
	Nodes []FrontendNode `json:"nodes"`
	Edges []FrontendEdge `json:"edges"`
}

type FrontendNode struct {
	ID   string           `json:"id"`
	Data FrontendNodeData `json:"data"`
}

type FrontendNodeData struct {
	Name   string                 `json:"name"`
	Inputs map[string]interface{} `json:"inputs"`
}

type FrontendEdge struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle"`
	TargetHandle string `json:"targetHandle"`
}

// 将前端 Flow 转换成 Go 内部的 Workflow
func toDomainWorkflow(flow FrontendFlow) (domain.Workflow, error) {
	result := domain.Workflow{}

	// 1. 转换节点
	for _, node := range flow.Nodes {
		var nodeType domain.NodeType
		var config map[string]interface{}

		switch node.Data.Name {
		case "startNode":
			nodeType = domain.NodeTypeStart

		case "llmNode":
			nodeType = domain.NodeTypeAgent

			if node.Data.Inputs == nil {
				return domain.Workflow{}, fmt.Errorf(
					"agent node %s has no inputs",
					node.ID,
				)
			}

			model, err := getStringInput(node.Data.Inputs, "model")
			if err != nil {
				return domain.Workflow{}, fmt.Errorf(
					"agent node %s: %w",
					node.ID,
					err,
				)
			}

			prompt, err := getStringInput(node.Data.Inputs, "prompt")
			if err != nil {
				return domain.Workflow{}, fmt.Errorf(
					"agent node %s: %w",
					node.ID,
					err,
				)
			}

			tools, exists := node.Data.Inputs["tools"]
			if !exists {
				tools = []interface{}{}
			}

			if tools == nil {
				tools = []interface{}{}
			}

			if _, ok := tools.([]interface{}); !ok {
				return domain.Workflow{}, fmt.Errorf(
					"agent node %s: tools must be an array",
					node.ID,
				)
			}

			config = map[string]interface{}{
				"model":         model,
				"system_prompt": prompt,
				"allowed_tools": tools,
			}

		case "endNode":
			nodeType = domain.NodeTypeEnd

		default:
			return domain.Workflow{}, fmt.Errorf(
				"unsupported frontend node type: %s",
				node.Data.Name,
			)
		}

		result.Nodes = append(result.Nodes, domain.Node{
			ID:     node.ID,
			Type:   nodeType,
			Config: config,
		})
	}

	// 2. 转换边
	for _, edge := range flow.Edges {
		result.Edges = append(result.Edges, domain.Edge{
			ID:           edge.ID,
			Source:       edge.Source,
			Target:       edge.Target,
			SourceHandle: edge.SourceHandle,
			TargetHandle: edge.TargetHandle,
		})
	}

	return result, nil
}

func getStringInput(inputs map[string]interface{}, key string) (string, error) {
	value, exists := inputs[key]
	if !exists {
		return "", fmt.Errorf("missing input field: %s", key)
	}

	result, ok := value.(string)
	if !ok || result == "" {
		return "", fmt.Errorf("input field %s must be a non-empty string", key)
	}

	return result, nil
}
