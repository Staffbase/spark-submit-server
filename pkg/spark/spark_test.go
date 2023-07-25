package spark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpark(t *testing.T) {
	t.Run("should be able to read spark templates", func(t *testing.T) {
		s, err := New(".", "../../example/sparkConf", "", false)
		require.NoError(t, err)
		require.Len(t, s.presets, 1)
		require.Equal(t, configurationPreset{
			Main: "local:////opt/spark/examples/src/main/python/pi.py",
			Args: []string{"10000000"},
			SparkConf: map[string]string{
				"spark.kubernetes.namespace":                              "spark",
				"spark.kubernetes.container.image":                        "apache/spark:3.4.0-python3",
				"spark.driver.cores":                                      "1",
				"spark.driver.memory":                                     "512m",
				"spark.executor.instances":                                "2",
				"spark.executor.cores":                                    "1",
				"spark.executor.memory":                                   "512m",
				"spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
				"spark.kubernetes.driver.podTemplateFile":                 "example/podTemplate/pi-pod.yaml",
			},
		}, s.presets["pi"])
	})

	t.Run("submitArgs bulds the correct arguments", func(t *testing.T) {
		s := Spark{
			presets: map[string]configurationPreset{
				"mypreset": {
					Main: "/app/example.py",
					Args: []string{"--verbose=true"},
					SparkConf: map[string]string{
						"spark.kubernetes.namespace": "spark",
					},
				},
			},
			master: "k8s://http://localhost:8000",
		}

		args, err := s.submitArgs("mypreset")
		require.NoError(t, err)
		require.Equal(t, []string{
			"--master=k8s://http://localhost:8000",
			"--deploy-mode=cluster",
			"--name=mypreset",
			"--conf=spark.kubernetes.namespace=spark",
			"/app/example.py",
			"--verbose=true",
		}, args)
	})

	t.Run("buildArgs bulds the correct arguments", func(t *testing.T) {
		s := Spark{master: "k8s://http://localhost:8000"}
		args := s.buildArgs("status", "namespace", "name")
		require.Equal(t, []string{
			"--master=k8s://http://localhost:8000",
			"--status=namespace:name",
		}, args)
	})
}
