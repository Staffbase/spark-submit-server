package httputil

import (
	"fmt"
	"net/http"

	"github.com/go-chi/render"
	"go.uber.org/zap"
)

type HTTPError struct {
	statusCode int
	message    string
}

func (e *HTTPError) Status() int {
	if e.statusCode == 0 {
		return http.StatusInternalServerError
	}

	return e.statusCode
}

func (e *HTTPError) Error() string {
	message := fmt.Sprintf("status: %d", e.statusCode)
	if e.message != "" {
		message = fmt.Sprintf("%s - %s", message, e.message)
	}

	return message
}

func WithStatusError(status int, message string) *HTTPError {
	return &HTTPError{status, message}
}

func BadRequestError(message string) *HTTPError {
	return &HTTPError{http.StatusBadRequest, message}
}

func InternelServerError(message string) *HTTPError {
	return &HTTPError{http.StatusInternalServerError, message}
}

func Wrap(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		zap.L().Info("got request")
		err := fn(w, r)
		if err != nil {
			if httpError, ok := err.(*HTTPError); ok {
				render.Status(r, httpError.statusCode)
				render.JSON(w, r, struct {
					Error string `json:"error"`
				}{httpError.message})
				return
			} else {
				zap.L().Error("unexpected error returned in handler", zap.Error(err))
				render.Status(r, httpError.statusCode)
				return
			}
		}
	}
}
