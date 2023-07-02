package spark

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
	"gopkg.in/yaml.v2"
)

type Spark struct {
	presets    map[string]configurationPreset
	binaryPath string
	master     string
	debug      bool
}

type configurationPreset struct {
	Main      string            `yaml:"main"`
	Args      []string          `yaml:"args"`
	SparkConf map[string]string `yaml:"sparkConf"`
}

func New(sparkHome, sparkConfDir, master string, debug bool) *Spark {
	spark := Spark{
		presets: make(map[string]configurationPreset),
		master:  master,
		debug:   debug,
	}

	if _, err := os.Stat(sparkHome); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark home found", zap.String("path", sparkHome))
	}
	spark.binaryPath = filepath.Join(sparkHome, "/bin/spark-submit")

	if _, err := os.Stat(sparkConfDir); os.IsNotExist(err) {
		zap.L().Fatal("directory for spark configuration presets not found", zap.String("path", sparkConfDir))
	}

	files, err := os.ReadDir(sparkConfDir)
	if err != nil {
		zap.L().Fatal("error reading preset directory", zap.String("path", sparkConfDir))
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fn := file.Name()
		if !strings.HasSuffix(fn, ".yaml") {
			continue
		}

		confPath := path.Join(sparkConfDir, fn)
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
		spark.presets[presetName] = preset
		zap.L().Debug("loaded preset", zap.String("presetName", presetName))
	}

	if len(spark.presets) == 0 {
		zap.L().Fatal("no presets found, please add some presets to the spark configuration preset directory", zap.String("path", sparkConfDir))
	}
	zap.L().Info("presets initialized", zap.Int("presetCount", len(spark.presets)))

	return &spark
}

var PresetNotFoundError error = fmt.Errorf("preset not found")

func (s *Spark) Submit(presetName string) error {
	preset, ok := s.presets[presetName]
	if !ok {
		return PresetNotFoundError
	}

	zap.L().Info("submit with args", zap.Any("args", preset))
	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--master=%s", s.master))
	args = append(args, "--deploy-mode=cluster")
	args = append(args, fmt.Sprintf("--name=%s", presetName))
	for key, value := range preset.SparkConf {
		args = append(args, fmt.Sprintf("--conf=%s=%s", key, value))
	}
	args = append(args, preset.Main)
	args = append(args, preset.Args...)
	go s.exec(args)
	return nil
}

func (s *Spark) Kill(namespace, name string) {
	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--master=%s", s.master))
	args = append(args, fmt.Sprintf("--kill=%s:%s", namespace, name))
	go s.exec(args)
}

func (s *Spark) Status(namespace, name string) string {
	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--master=%s", s.master))
	args = append(args, fmt.Sprintf("--status=%s:%s", namespace, name))
	cmd := exec.Command(s.binaryPath, args...)
	zap.L().Info("spark-submit", zap.Strings("args", args))
	var buffer bytes.Buffer
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	if err := cmd.Run(); err != nil {
		zap.L().Error("spark-submit failed", zap.Error(err))
	}
	return buffer.String()
}

func (s *Spark) exec(args []string) {
	cmd := exec.Command(s.binaryPath, args...)
	zap.L().Info("spark-submit", zap.Strings("args", args))
	if s.debug {
		writer := &zapio.Writer{Log: zap.L(), Level: zap.DebugLevel}
		cmd.Stderr = writer
		cmd.Stdout = writer
		defer writer.Close()
	}
	if err := cmd.Run(); err != nil {
		zap.L().Error("spark-submit failed", zap.Error(err))
	}
}
