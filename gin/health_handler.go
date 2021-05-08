package gin

import (
	"github.com/core-go/health"
	"github.com/gin-gonic/gin"
	"net/http"
)

type HealthHandler struct {
	HealthCheckers []health.HealthChecker
}

func NewHealthHandler(checkers ...health.HealthChecker) *HealthHandler {
	return &HealthHandler{checkers}
}

func (c *HealthHandler) Check() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		result := health.Check(ctx.Request.Context(), c.HealthCheckers)
		if result.Status == health.StatusUp {
			ctx.JSON(http.StatusOK, result)
		} else {
			ctx.JSON(http.StatusInternalServerError, result)
		}
	}
}
