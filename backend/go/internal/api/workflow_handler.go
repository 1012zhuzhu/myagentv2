
package api

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/repository"

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
		log.Printf(
			"[run_id=%s] get run failed: %v",
			runID,
			err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	var result interface{}

	if run.Result != "" {
		if err := json.Unmarshal([]byte(run.Result), &result); err != nil {
			log.Printf(
				"[run_id=%s] parse run result failed: %v",
				runID,
				err,
			)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": "failed to parse result",
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"run_id":  run.RunID,
		"status":  run.Status,
		"result":  result,
		"error":   run.Error,
	})
}

// requestHash 计算一次逻辑请求的内容摘要。
func requestHash(req RunWorkflowRequest) (string, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// returnExistingRun 判断已有 Key 对应的请求是否一致。
func (h *WorkflowHandler) returnExistingRun(
	c *gin.Context,
	key string,
	hash string,
) bool {
	record, err := repository.GetIdempotencyRecord(h.db, key)

	if errors.Is(err, sql.ErrNoRows) {
		return false
	}

	if err != nil {
		log.Printf(
			"idempotency record lookup failed: %v",
			err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "failed to query idempotency record",
		})
		return true
	}

	if record.RequestHash != hash {
		log.Printf(
			"[idempotency_key=%s] request hash conflict",
			key,
		)
		c.JSON(http.StatusConflict, gin.H{
			"success": false,
			"message": "idempotency key already used with a different request",
		})
		return true
	}

	run, err := h.runRepo.GetRunByID(record.RunID)
	if err != nil {
		log.Printf(
			"[run_id=%s] idempotency record exists but run lookup failed: %v",
			record.RunID,
			err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "idempotency record exists but run lookup failed",
		})
		return true
	}

	log.Printf(
		"[run_id=%s conversation_id=%s] idempotent request reused existing run",
		run.RunID,
		run.ConversationID,
	)

	c.JSON(http.StatusAccepted, gin.H{
		"success":         true,
		"run_id":          run.RunID,
		"conversation_id": run.ConversationID,
		"status":          run.Status,
	})

	return true
}

func (h *WorkflowHandler) Run(c *gin.Context) {
	var req RunWorkflowRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("invalid workflow request body: %v", err)
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

	// 1. 读取并校验 Idempotency-Key
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Idempotency-Key header is required",
		})
		return
	}

	// 2. 计算请求内容摘要
	hash, err := requestHash(req)
	if err != nil {
		log.Printf(
			"[conversation_id=%s] calculate request hash failed: %v",
			req.ConversationID,
			err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "failed to calculate request hash",
		})
		return
	}

	// 3. 先检查 Key 是否已经使用
	if h.returnExistingRun(c, key, hash) {
		return
	}

	// 4. 转换工作流
	domainFlow, err := toDomainWorkflow(req.Flow)
	if err != nil {
		log.Printf(
			"[conversation_id=%s] convert workflow failed: %v",
			req.ConversationID,
			err,
		)
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
		log.Printf(
			"[run_id=%s] serialize workflow payload failed: %v",
			runID,
			err,
		)
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

	idempotency := repository.IdempotencyRecord{
		Key:         key,
		RequestHash: hash,
		RunID:       runID,
		CreatedAt:   now,
	}

	// 5. 在同一个事务中创建 Run、Outbox、幂等记录
	err = repository.CreateRunWithOutbox(
		h.db,
		run,
		outbox,
		idempotency,
	)

	if err != nil {
		log.Printf(
			"[run_id=%s conversation_id=%s] create run transaction failed: %v",
			runID,
			req.ConversationID,
			err,
		)

		// 可能是另一个并发请求抢先创建了相同 Key。
		// 再查一次，若存在则返回原 Run。
		if h.returnExistingRun(c, key, hash) {
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// 6. 首次创建成功
	log.Printf(
		"[run_id=%s conversation_id=%s] workflow run created status=pending",
		runID,
		req.ConversationID,
	)

	c.JSON(http.StatusAccepted, gin.H{
		"success":         true,
		"run_id":          runID,
		"conversation_id": req.ConversationID,
		"status":          "pending",
	})
}
