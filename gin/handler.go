package gin

import (
	"github.com/core-go/mq/health"
	"github.com/gin-gonic/gin"
	"net/http"
)

type Handler struct {
	Checkers []health.Checker
}

func NewHandler(checkers ...health.Checker) *Handler {
	return &Handler{checkers}
}

func (c *Handler) Check() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		result := health.Check(ctx.Request.Context(), c.Checkers)
		if result.Status == health.StatusUp {
			ctx.JSON(http.StatusOK, result)
		} else {
			ctx.JSON(http.StatusInternalServerError, result)
		}
	}
}
