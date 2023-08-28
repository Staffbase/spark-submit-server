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
	"errors"
	"net/http"

	"github.com/Staffbase/spark-submit/pkg/httputil"
	"github.com/Staffbase/spark-submit/pkg/spark"
	"github.com/go-chi/render"
	"go.uber.org/zap"
)

var HandleHealth http.HandlerFunc = httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
	render.JSON(w, r, struct {
		OK bool `json:"ok"`
	}{true})
	return nil
})

type Spark interface {
	Submit(preset string) error
	Kill(namespace, name string)
	Status(namespace, name string) string
}

var HandleSubmit = func(s Spark) http.HandlerFunc {
	return httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		preset := r.URL.Query().Get("preset")
		if preset == "" {
			return httputil.BadRequestError("missing parameter preset")
		}

		if err := s.Submit(preset); err != nil {
			if errors.Is(err, spark.PresetNotFoundError) {
				return httputil.NotFoundError("preset not found")
			}

			zap.L().Error("error when submitting spark app", zap.Error(err))
			return httputil.InternelServerError("error when submitting spark app")
		}

		return nil
	})
}

var HandleKill = func(s Spark) http.HandlerFunc {
	return httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			return httputil.BadRequestError("missing parameter namespace")
		}

		name := r.URL.Query().Get("name")
		if name == "" {
			return httputil.BadRequestError("missing parameter name")
		}

		s.Kill(namespace, name)
		return nil
	})
}

var HandleStatus = func(s Spark) http.HandlerFunc {
	return httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		namespace := r.URL.Query().Get("namespace")
		if namespace == "" {
			return httputil.BadRequestError("missing parameter namespace")
		}

		name := r.URL.Query().Get("name")
		if name == "" {
			name = "*"
		}

		render.JSON(w, r, struct {
			Status string `json:"status"`
		}{
			Status: s.Status(namespace, name),
		})
		return nil
	})
}
