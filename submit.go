package main

import (
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"

	"github.com/Staffbase/spark-submit/pkg/httputil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
)

var handleSubmit = httputil.Wrap(func(w http.ResponseWriter, r *http.Request) error {
	preset := r.URL.Query().Get("preset")
	if preset == "" {
		return httputil.BadRequestError("missing parameter preset")
	}

	conf := presets[preset]
	zap.L().Info("submit with args", zap.Any("args", conf))
	if err := runSparkSubmit(preset, conf); err != nil {
		zap.L().Error("error when submitting spark app", zap.Error(err))
		return httputil.InternelServerError("error when submitting spark app")
	}

	return nil
})

func runSparkSubmit(name string, preset configurationPreset) error {
	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--master=%s", *master))
	args = append(args, "--deploy-mode=cluster")
	args = append(args, fmt.Sprintf("--name=%s", name))
	for key, value := range preset.SparkConf {
		args = append(args, fmt.Sprintf("--conf=%s=%s", key, value))
	}
	args = append(args, preset.Main)
	args = append(args, preset.Args...)
	cmd := exec.Command(filepath.Join(*sparkHomePath, "/bin/spark-submit"), args...)
	zap.L().Info("spark-submit", zap.Strings("args", args))
	go func() {
		if *debugSubmit {
			writer := &zapio.Writer{Log: zap.L(), Level: zap.DebugLevel}
			cmd.Stderr = writer
			cmd.Stdout = writer
			defer writer.Close()
		}

		if err := cmd.Run(); err != nil {
			zap.L().Error("spark-submit failed", zap.Error(err))
		}
	}()

	return nil
}
