package echo

import (
	"github.com/core-go/health"
	"github.com/labstack/echo"
	"net/http"
)

type HealthHandler struct {
	HealthCheckers []health.HealthChecker
}

func NewHealthHandler(checkers ...health.HealthChecker) *HealthHandler {
	return &HealthHandler{checkers}
}

func (c *HealthHandler) Check() echo.HandlerFunc {
	return func(ctx echo.Context) error {
		result := health.Check(ctx.Request().Context(), c.HealthCheckers)
		if result.Status == health.StatusUp {
			return ctx.JSON(http.StatusOK, result)
		} else {
			return ctx.JSON(http.StatusInternalServerError, result)
		}
	}
}
