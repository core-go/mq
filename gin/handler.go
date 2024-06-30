package handler

import (
	"context"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
)

type SenderHandler struct {
	Response string
	Send     []func(ctx context.Context, data []byte, attributes map[string]string) (string, error)
}

func NewSenderHandler(response string, send ...func(context.Context, []byte, map[string]string) (string, error)) *SenderHandler {
	return &SenderHandler{Response: response, Send: send}
}

func (h *SenderHandler) Receive(ctx *gin.Context) {
	r := ctx.Request
	b, er1 := io.ReadAll(r.Body)
	defer r.Body.Close()
	if er1 != nil {
		ctx.String(http.StatusBadRequest, er1.Error())
		return
	}

	l := len(h.Send)
	if l == 0 {
		ctx.JSON(http.StatusOK, h.Response)
		return
	}
	var result string
	var er2 error
	for i := 0; i < l; i++ {
		result, er2 = h.Send[i](r.Context(), b, nil)
		if er2 != nil {
			ctx.String(http.StatusInternalServerError, er2.Error())
			return
		}
	}
	if len(h.Response) == 0 {
		ctx.JSON(http.StatusOK, result)
	} else {
		ctx.JSON(http.StatusOK, h.Response)
	}
}
