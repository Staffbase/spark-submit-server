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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrap(t *testing.T) {
	decodeError := func(t *testing.T, w *httptest.ResponseRecorder) string {
		var result struct {
			Error string `json:"error"`
		}
		require.NoError(t, json.NewDecoder(w.Result().Body).Decode(&result))
		return result.Error
	}

	t.Run("given an httpError, responds in json", func(t *testing.T) {
		myHandler := func(w http.ResponseWriter, r *http.Request) error {
			return &HTTPError{
				statusCode: http.StatusForbidden,
				message:    "my error message",
			}
		}
		handler := Wrap(myHandler)
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handler(w, r)
		require.Equal(t, http.StatusForbidden, w.Result().StatusCode)
		msg := decodeError(t, w)
		require.Equal(t, "my error message", msg)
	})

	t.Run("given an unknown error, falls back to 500", func(t *testing.T) {
		myHandler := func(w http.ResponseWriter, r *http.Request) error {
			return errors.New("nope")
		}

		handler := Wrap(myHandler)
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handler(w, r)
		msg := decodeError(t, w)
		require.Equal(t, "unexpected error", msg)
	})
}

func TestHTTPError(t *testing.T) {
	t.Run("should template a Error message", func(t *testing.T) {
		err := HTTPError{
			statusCode: 0,
			message:    "my custom error",
		}

		require.Equal(t, "status: 500 - my custom error", err.Error())
	})

	for _, tt := range []struct {
		name       string
		err        *HTTPError
		wantStatus int
	}{
		{
			name:       "WithStatusError",
			err:        WithStatusError(http.StatusBadGateway, ""),
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "BadRequestError",
			err:        BadRequestError(""),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "InternelServerError",
			err:        InternelServerError(""),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "NotFoundError",
			err:        NotFoundError(""),
			wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(fmt.Sprintf("given %s, should respond %d", tt.name, tt.wantStatus), func(t *testing.T) {
			require.Equal(t, tt.err.statusCode, tt.wantStatus)
		})
	}
}
