package main

import (
	"net/http"

	"github.com/Staffbase/spark-submit/pkg/httputil"
	"github.com/go-chi/render"
)

var handleHealth http.HandlerFunc = httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
	render.JSON(w, r, struct {
		OK bool `json:"ok"`
	}{true})
	return nil
})
