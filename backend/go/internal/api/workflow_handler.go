package api

import (
	"database/sql"
	"encoding/json"
	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/repository"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type RunWorkflowRequest struct {
	Flow           FrontendFlow `json:"flow"`
	Input          interface{}  `json:"input"`
	ConversationID string       `json:"conversationId"`
}

type WorkflowHandler struct {
	db *sql.DB
}

func NewWorkflowHandler(db *sql.DB) *WorkflowHandler {
	return &WorkflowHandler{
		db: db,
	}
}

func (h *WorkflowHandler) Run(c *gin.Context) {
	var req RunWorkflowRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "invalid request body",
		})
		return
	}

	if req.ConversationID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "conversationId is required",
		})
		return
	}

	domainFlow, err := toDomainWorkflow(req.Flow)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	runID := uuid.NewString()

	now := time.Now().UTC()

	run := domain.Run{
		RunID:          runID,
		ConversationID: req.ConversationID,
		WorkflowID:     domainFlow.ID,
		Status:         "pending",
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	payload := domain.WorkflowRunPayload{
		RunID:    runID,
		Workflow: domainFlow,
		Input:    req.Input,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "failed to serialize workflow payload",
		})
		return
	}

	outbox := domain.Outbox{
		EventID:   uuid.NewString(),
		RunID:     runID,
		EventType: "workflow.run",
		Payload:   string(payloadBytes),
		Status:    "pending",
		CreatedAt: now,
	}

	if err := repository.CreateRunWithOutbox(h.db, run, outbox); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"success":         true,
		"run_id":          runID,
		"conversation_id": req.ConversationID,
		"status":          "pending",
	})
}
