package domain

type WorkflowRunPayload struct {
    RunID    string      `json:"run_id"`
    Workflow Workflow    `json:"workflow"`
    Input    interface{} `json:"input"`
}