package handler

import (
	"context"
	"encoding/json"
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

func (h *SenderHandler) Receive(w http.ResponseWriter, r *http.Request) {
	b, er1 := io.ReadAll(r.Body)
	defer r.Body.Close()
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusBadRequest)
		return
	}
	l := len(h.Send)
	if l == 0 {
		respond(w, http.StatusOK, h.Response)
		return
	}
	var result string
	var er2 error
	for i := 0; i < l; i++ {
		result, er2 = h.Send[i](r.Context(), b, nil)
		if er2 != nil {
			http.Error(w, er2.Error(), http.StatusInternalServerError)
			return
		}
	}
	if len(h.Response) == 0 {
		respond(w, http.StatusOK, result)
	} else {
		respond(w, http.StatusOK, h.Response)
	}
}
func respond(w http.ResponseWriter, status int, result interface{}) error {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(result)
	return err
}
