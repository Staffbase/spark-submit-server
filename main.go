package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

var (
	sparkHomePath   = flag.String("spark-home", "", "spark home directory")
	sparkConfigDir  = flag.String("spark-conf-dir", "", "directory with spark configuration presets")
	master          = flag.String("master", "", "spark master address")
	debugSubmit     = flag.Bool("debug-submit", false, "write spark-submit output to logger")
	devMode         = flag.Bool("dev-mode", false, "sets the logger output to development config")
	enableDebugLogs = flag.Bool("debug", false, "enables debug logs")
)

func main() {
	flag.Parse()
	setup()
}

var presets map[string]configurationPreset = make(map[string]configurationPreset)

type configurationPreset struct {
	Main      string            `yaml:"main"`
	Args      []string          `yaml:"args"`
	SparkConf map[string]string `yaml:"sparkConf"`
}

func setup() {
	setupLogger()

	if *master == "" {
		zap.L().Fatal("spark master is not specified, please use --master")
	}

	if *sparkHomePath == "" {
		zap.L().Fatal("spark home is not specified, please use --spark-home")
	}

	if _, err := os.Stat(*sparkHomePath); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark home found", zap.String("sparkHomePath", *sparkHomePath))
	}

	if *sparkConfigDir == "" {
		zap.L().Fatal("spark config directory is not specified, please use --spark-conf-dir")
	}

	if _, err := os.Stat(*sparkConfigDir); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark configuration presets not found", zap.String("sparkConfigDir", *sparkHomePath))
	}

	files, err := ioutil.ReadDir(*sparkConfigDir)
	if err != nil {
		zap.L().Fatal("error reading preset directory", zap.String("sparkConfigDir", *sparkHomePath))
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fn := file.Name()
		if !strings.HasSuffix(fn, ".yaml") {
			continue
		}

		confPath := path.Join(*sparkConfigDir, fn)
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
		zap.L().Fatal("no presets found, please add some presets to the spark configuration preset directory", zap.String("sparkConfigDir", *sparkConfigDir))
	}
	zap.L().Info("presets initialized", zap.Int("presetCount", len(presets)))

	r := chi.NewRouter()
	r.Post("/", handleSubmit)
	zap.L().Info("start http server on port 3000")
	if err := http.ListenAndServe(":7070", r); err != nil {
		zap.L().Fatal("couldn't start webserver", zap.Error(err))
	}
}

func setupLogger() {
	config := zap.NewProductionConfig()
	if *devMode {
		config = zap.NewDevelopmentConfig()
	}

	if *enableDebugLogs {
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
