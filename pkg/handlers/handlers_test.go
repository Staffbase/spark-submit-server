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

package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Staffbase/spark-submit/pkg/spark"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	*httptest.ResponseRecorder
}

func (r *recorder) assertHTTPStatus(t *testing.T, wantStatus int) {
	t.Helper()
	require.Equal(t, wantStatus, r.Result().StatusCode)
}

func (r *recorder) assertError(t *testing.T, wantMessage string) {
	t.Helper()

	var errorResult struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(r.Result().Body).Decode(&errorResult))
	require.Contains(t, errorResult.Error, wantMessage)
}

func newRequest(method, target string) (recorder, *http.Request) {
	if method == "" {
		method = http.MethodGet
	}
	if target == "" {
		target = "/"
	}
	r := httptest.NewRequest(method, target, nil)
	w := httptest.NewRecorder()
	return recorder{w}, r
}

func TestHandleHealth(t *testing.T) {
	t.Run("responds with 200", func(t *testing.T) {
		w, r := newRequest("", "")
		HandleHealth(w, r)
		w.assertHTTPStatus(t, http.StatusOK)
	})
}

// mock implementation of spark dependency
type sparkMock struct {
	submit func(preset string) error
	kill   func(namespace, name string)
	status func(namespace, name string) string
}

func (sm *sparkMock) Submit(preset string) error {
	if sm.submit == nil {
		// relaxed fallback
		return nil
	}
	return sm.submit(preset)
}
func (sm *sparkMock) Kill(namespace, name string) {
	if sm.kill == nil {
		return
	}

	sm.kill(namespace, name)
}
func (sm *sparkMock) Status(namespace, name string) string {
	if sm.status == nil {
		return ""
	}

	return sm.status(namespace, name)
}

func TestHandleSubmit(t *testing.T) {
	t.Run("given a valid preset, responds 200", func(t *testing.T) {
		handler := HandleSubmit(&sparkMock{})
		w, r := newRequest("", "/?preset=pi")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusOK)
	})

	t.Run("given no preset parameter responds with 400", func(t *testing.T) {
		handler := HandleSubmit(&sparkMock{})
		w, r := newRequest("", "")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusBadRequest)
		w.assertError(t, "missing parameter preset")
	})

	t.Run("given missing preset, responds with 404", func(t *testing.T) {
		handler := HandleSubmit(&sparkMock{
			submit: func(preset string) error {
				return spark.PresetNotFoundError
			},
		})
		w, r := newRequest("", "/?preset=pi")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusNotFound)
		w.assertError(t, "reset not found")
	})

	t.Run("given submission error, responds with 500", func(t *testing.T) {
		handler := HandleSubmit(&sparkMock{
			submit: func(preset string) error {
				return errors.New("nope")
			},
		})
		w, r := newRequest("", "/?preset=pi")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusInternalServerError)
		w.assertError(t, "error when submitting spark app")
	})
}

func TestHandleKill(t *testing.T) {
	t.Run("given a valid preset, responds 200", func(t *testing.T) {
		handler := HandleKill(&sparkMock{})
		w, r := newRequest("", "/?namespace=foo&name=bar")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusOK)
	})

	t.Run("given no namespace, responds 400", func(t *testing.T) {
		handler := HandleKill(&sparkMock{})
		w, r := newRequest("", "/?name=bar")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusBadRequest)
		w.assertError(t, "missing parameter namespace")
	})

	t.Run("given no namespace, responds 400", func(t *testing.T) {
		handler := HandleKill(&sparkMock{})
		w, r := newRequest("", "/?namespace=foo")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusBadRequest)
		w.assertError(t, "missing parameter name")
	})
}

func TestHandleStatus(t *testing.T) {
	t.Run("given a valid preset, responds 200 and the status message from spark", func(t *testing.T) {
		sparkStatusMessage := "spark-status=good"
		handler := HandleStatus(&sparkMock{
			status: func(namespace, name string) string {
				return sparkStatusMessage
			},
		})
		w, r := newRequest("", "/?namespace=foo&name=bar")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusOK)

		var result struct {
			Status string `json:"status"`
		}
		require.NoError(t, json.NewDecoder(w.Result().Body).Decode(&result))
		require.Equal(t, result.Status, sparkStatusMessage)
	})
	t.Run("given no namespace, responds 400", func(t *testing.T) {
		handler := HandleStatus(&sparkMock{})
		w, r := newRequest("", "/?name=bar")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusBadRequest)
		w.assertError(t, "missing parameter namespace")
	})

	t.Run("given no name, falls back to name=*", func(t *testing.T) {
		ran := false
		handler := HandleStatus(&sparkMock{
			status: func(namespace, name string) string {
				require.Equal(t, "*", name)
				ran = true
				return ""
			},
		})
		w, r := newRequest("", "/?namespace=foo")
		handler(w, r)
		w.assertHTTPStatus(t, http.StatusOK)
		require.True(t, ran)
	})
}
