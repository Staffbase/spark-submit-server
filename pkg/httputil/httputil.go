/*
Copyright 2023, Staffbase GmbH and contributors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	message := fmt.Sprintf("status: %d", e.Status())
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

func NotFoundError(message string) *HTTPError {
	return &HTTPError{http.StatusNotFound, message}
}

type errorResponse struct {
	Error string `json:"error"`
}

func Wrap(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			if httpError, ok := err.(*HTTPError); ok {
				render.Status(r, httpError.Status())
				render.JSON(w, r, errorResponse{httpError.message})
				return
			} else {
				zap.L().Error("unexpected error returned in handler", zap.Error(err))
				render.Status(r, http.StatusInternalServerError)
				render.JSON(w, r, errorResponse{"unexpected error"})
				return
			}
		}
	}
}
