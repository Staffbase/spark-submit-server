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

package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/Staffbase/spark-submit/pkg/handlers"
	"github.com/Staffbase/spark-submit/pkg/spark"
	"github.com/alecthomas/kong"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mainCmd struct {
	SparkHome    string `required:"" default:"/opt/spark" help:"spark home directory" env:"SPARK_HOME"`
	SparkConfDir string `required:"" help:"directory with spark configuration presets" env:"SPARK_CONF_DIR"`
	Master       string `required:"" help:"spark master address" env:"SPARK_MASTER"`
	DebugSubmit  bool   `help:"write spark-submit output to logger" env:"DEBUG_SPARK_SUBMIT"`
	DevMode      bool   `help:"sets the logger output to development config"`
	Debug        bool   `help:"enables debug logs" env:"DEBUG"`
}

var CLI struct {
	Main mainCmd `cmd:"" default:"withargs" help:"start the web-server"`
}

func main() {
	kong.Parse(&CLI)
	CLI.Main.Run()
}

func (cmd *mainCmd) Run() {
	cmd.setupLogger()
	s, err := spark.New(cmd.SparkHome, cmd.SparkConfDir, cmd.Master, cmd.DebugSubmit)
	if err != nil {
		zap.L().Fatal("couldn't initialize spark dependency", zap.Error(err))
	}
	r := chi.NewRouter()
	r.Get("/health", handlers.HandleHealth)
	r.Post("/", handlers.HandleSubmit(s))
	r.Get("/", handlers.HandleStatus(s))
	r.Delete("/", handlers.HandleKill(s))
	zap.L().Info("start http server on port 7070")
	if err := http.ListenAndServe(":7070", r); err != nil {
		zap.L().Fatal("couldn't start webserver", zap.Error(err))
	}
}

func (cmd mainCmd) setupLogger() {
	config := zap.NewProductionConfig()
	if cmd.DevMode {
		config = zap.NewDevelopmentConfig()
	}

	if cmd.Debug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	if logger, err := config.Build(); err != nil {
		fmt.Printf("unable to setup zap logger %s\n", err)
		os.Exit(1)
	} else {
		zap.ReplaceGlobals(logger)
	}
}
