package main

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

type mainCmd struct {
	SparkHome    string `required:"" help:"spark home directory" env:"SPARK_HOME"`
	SparkConfDir string `required:"" help:"directory with spark configuration presets" env:"SPARK_CONF_DIR"`
	Master       string `required:"" help:"spark master address" env:"SPARK_MASTER"`
	DebugSubmit  bool   `help:"write spark-submit output to logger" env:"DEBUG_SPARK_SUBMIT"`
	DevMode      bool   `help:"sets the logger output to development config"`
	Debug        bool   `help:"enables debug logs"`
}

var CLI struct {
	Main mainCmd `cmd:"" default:"withargs" help:"start the web-server"`
}

func main() {
	kong.Parse(&CLI)
	CLI.Main.Run()
}

type configurationPreset struct {
	Main      string            `yaml:"main"`
	Args      []string          `yaml:"args"`
	SparkConf map[string]string `yaml:"sparkConf"`
}

var presets map[string]configurationPreset = make(map[string]configurationPreset)
var sparkSubmitPath string

func (cmd *mainCmd) Run() {
	cmd.setupLogger()
	if _, err := os.Stat(cmd.SparkHome); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark home found", zap.String("sparkHomePath", cmd.SparkHome))
	}
	sparkSubmitPath = filepath.Join(cmd.SparkHome, "/bin/spark-submit")

	if _, err := os.Stat(cmd.SparkConfDir); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark configuration presets not found", zap.String("sparkConfigDir", cmd.SparkConfDir))
	}

	files, err := os.ReadDir(cmd.SparkConfDir)
	if err != nil {
		zap.L().Fatal("error reading preset directory", zap.String("sparkConfigDir", cmd.SparkConfDir))
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fn := file.Name()
		if !strings.HasSuffix(fn, ".yaml") {
			continue
		}

		confPath := path.Join(cmd.SparkConfDir, fn)
		rawConf, err := os.ReadFile(confPath)
		if err != nil {
			zap.L().Error("error reading config", zap.Error(err), zap.String("configPath", confPath))
			continue
		}

		var preset configurationPreset
		err = yaml.Unmarshal(rawConf, &preset)
		if err != nil {
			zap.L().Debug("couldn't parse preset", zap.Error(err), zap.String("rawConf", string(rawConf)))
			continue
		}

		presetName := strings.TrimSuffix(fn, ".yaml")
		presets[presetName] = preset
		zap.L().Debug("loaded preset", zap.String("presetName", presetName))
	}

	if len(presets) == 0 {
		zap.L().Fatal("no presets found, please add some presets to the spark configuration preset directory", zap.String("sparkConfigDir", cmd.SparkConfDir))
	}
	zap.L().Info("presets initialized", zap.Int("presetCount", len(presets)))

	r := chi.NewRouter()
	r.Post("/", handleSubmit(cmd.Master, cmd.DebugSubmit))
	zap.L().Info("start http server on port 3000")
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
