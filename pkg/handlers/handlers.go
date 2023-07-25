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
