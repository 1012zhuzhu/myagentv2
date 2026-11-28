
package api

import (
	"my-agent-v2/backend/go/internal/bootstrap"
	"net/http"

	"github.com/gin-gonic/gin"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "http://localhost:5173")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, Idempotency-Key")

		// 浏览器可能先发送 OPTIONS 预检请求
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func NewRouter(app *bootstrap.App) *gin.Engine {
	router := gin.Default()

	router.Use(corsMiddleware())

	handler := NewWorkflowHandler(
		app.DB,
		app.RunRepo,
		app.CancelManager,
	)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	router.POST(
		"/api/workflow/run",
		rateLimitMiddleware(app.RateLimiter),
		handler.Run,
	)

	router.GET(
		"/api/workflow/run/:run_id",
		handler.GetRun,
	)

	// 请求取消指定 Run
	router.POST(
		"/api/workflow/run/:run_id/cancel",
		handler.CancelRun,
	)

	return router
}
