package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
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
    db      *sql.DB
    runRepo *repository.RunRepository
}

func NewWorkflowHandler(
    db *sql.DB,
    runRepo *repository.RunRepository,
) *WorkflowHandler {
    return &WorkflowHandler{
        db:      db,
        runRepo: runRepo,
    }
}

func (h *WorkflowHandler) GetRun(c *gin.Context) {
    runID := c.Param("run_id")

    run, err := h.runRepo.GetRunByID(runID)
    if errors.Is(err, sql.ErrNoRows) {
        c.JSON(http.StatusNotFound, gin.H{
            "success": false,
            "message": "run not found",
        })
        return
    }

    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{
            "success": false,
            "message": err.Error(),
        })
        return
    }

    var result interface{}

    if run.Result != "" {
        if err := json.Unmarshal([]byte(run.Result), &result); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{
                "success": false,
                "message": "failed to parse result",
            })
            return
        }
    }

	log.Printf("[DEBUG] GetRun: runID=%s, status=%s, error=%q",
    run.RunID,
    run.Status,
    run.Error,
)

    c.JSON(http.StatusOK, gin.H{
		"success": true,
		"run_id":  run.RunID,
		"status":  run.Status,
		"result":  result,
		"error":   run.Error,
	})
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