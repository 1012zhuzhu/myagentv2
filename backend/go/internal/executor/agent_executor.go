
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"my-agent-v2/backend/go/internal/domain"
)

type AgentExecutor struct {
	pythonBaseURL string
	client        *http.Client
}

type AgentExecuteRequest struct {
	RunID  string              `json:"run_id"`
	NodeID string              `json:"node_id"`
	Input  interface{}         `json:"input"`
	Config domain.AgentConfig  `json:"config"`
}

type PythonErrorResponse struct {
	Detail struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		RunID   string `json:"run_id"`
		NodeID  string `json:"node_id"`
	} `json:"detail"`
}

type AgentExecuteResponse struct {
	Output interface{} `json:"output"`
}

func NewAgentExecutor(pythonBaseURL string) *AgentExecutor {
	return &AgentExecutor{
		pythonBaseURL: pythonBaseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type AgentExecutionError struct {
	Code       string
	Message    string
	RunID      string
	NodeID     string
	HTTPStatus int
}

func (e *AgentExecutionError) Error() string {
	return fmt.Sprintf(
		"agent error [%s] run_id=%s node_id=%s: %s",
		e.Code,
		e.RunID,
		e.NodeID,
		e.Message,
	)
}

func (e *AgentExecutor) Execute(
	ctx context.Context,
	node domain.Node,
	execCtx *domain.ExecutionContext,
) (*domain.NodeExecutionResult, error) {

	if node.Type != domain.NodeTypeAgent {
		return nil, fmt.Errorf("invalid node type: %s", node.Type)
	}

	if execCtx == nil {
		return nil, fmt.Errorf("execution context cannot be nil")
	}

	config, err := parseAgentConfig(node.Config)
	if err != nil {
		return nil, fmt.Errorf("parse agent config: %w", err)
	}

	input, exists := execCtx.NodeInputs[node.ID]
	if !exists {
		return nil, fmt.Errorf("input not found for agent node %s", node.ID)
	}

	request := AgentExecuteRequest{
		RunID:  execCtx.RunID,
		NodeID: node.ID,
		Input:  input,
		Config: config,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal agent request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		e.pythonBaseURL+"/internal/v1/agent/execute",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create python agent request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()

	log.Printf(
		"[run_id=%s node_id=%s] calling python agent service",
		execCtx.RunID,
		node.ID,
	)

	resq, err := e.client.Do(req)
	if err != nil {
		duration := time.Since(start)

		log.Printf(
			"[run_id=%s node_id=%s] python agent request failed duration=%s error=%v",
			execCtx.RunID,
			node.ID,
			duration,
			err,
		)

		if errors.Is(err, context.Canceled) {
			return nil, &AgentExecutionError{
				Code:    "EXECUTION_CANCELLED",
				Message: "Agent 执行已取消",
				RunID:   execCtx.RunID,
				NodeID:  node.ID,
			}
		}

		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) ||
			(errors.As(err, &netErr) && netErr.Timeout()) {
			return nil, &AgentExecutionError{
				Code:    "PYTHON_SERVICE_TIMEOUT",
				Message: "调用 Python Agent 服务超时",
				RunID:   execCtx.RunID,
				NodeID:  node.ID,
			}
		}

		return nil, fmt.Errorf(
			"call python agent service run_id=%s node_id=%s: %w",
			execCtx.RunID,
			node.ID,
			err,
		)
	}

	defer resq.Body.Close()

	if resq.StatusCode < 200 || resq.StatusCode >= 300 {
		log.Printf(
			"[run_id=%s node_id=%s] python agent returned error status=%d duration=%s",
			execCtx.RunID,
			node.ID,
			resq.StatusCode,
			time.Since(start),
		)

		errorBody, readErr := io.ReadAll(
			io.LimitReader(resq.Body, 1024*1024),
		)

		if readErr != nil {
			return nil, fmt.Errorf(
				"python agent service returned status %d; read error body: %w",
				resq.StatusCode,
				readErr,
			)
		}

		var pythonError PythonErrorResponse

		if err := json.Unmarshal(errorBody, &pythonError); err == nil {
			if pythonError.Detail.Code != "" {
				return nil, &AgentExecutionError{
					Code:       pythonError.Detail.Code,
					Message:    pythonError.Detail.Message,
					RunID:      execCtx.RunID,
					NodeID:     node.ID,
					HTTPStatus: resq.StatusCode,
				}
			}
		}

		return nil, fmt.Errorf(
			"python agent service returned status %d: %s",
			resq.StatusCode,
			string(errorBody),
		)
	}

	var agentResponse AgentExecuteResponse

	if err := json.NewDecoder(resq.Body).Decode(&agentResponse); err != nil {
		log.Printf(
			"[run_id=%s node_id=%s] decode python agent response failed: %v",
			execCtx.RunID,
			node.ID,
			err,
		)
		return nil, fmt.Errorf("decode agent response: %w", err)
	}

	log.Printf(
		"[run_id=%s node_id=%s] python agent execution succeeded duration=%s",
		execCtx.RunID,
		node.ID,
		time.Since(start),
	)

	return &domain.NodeExecutionResult{
		Output:     agentResponse.Output,
		NextHandle: "output",
	}, nil
}

func parseAgentConfig(
	config map[string]interface{},
) (domain.AgentConfig, error) {

	var result domain.AgentConfig

	systemPrompt, ok := config["system_prompt"].(string)
	if !ok {
		return result, fmt.Errorf("invalid or missing system_prompt")
	}

	model, ok := config["model"].(string)
	if !ok {
		return result, fmt.Errorf("invalid or missing model")
	}

	result.SystemPrompt = systemPrompt
	result.Model = model

	rawTools, exists := config["allowed_tools"]
	if !exists {
		result.AllowedTools = []string{}
		return result, nil
	}

	tools, ok := rawTools.([]interface{})
	if !ok {
		return result, fmt.Errorf("invalid allowed_tools")
	}

	result.AllowedTools = make([]string, 0, len(tools))

	for _, tool := range tools {
		toolName, ok := tool.(string)
		if !ok {
			return result, fmt.Errorf("allowed_tools must contain strings")
		}

		result.AllowedTools = append(
			result.AllowedTools,
			toolName,
		)
	}

	return result, nil
}

var _ Executor = (*AgentExecutor)(nil)
