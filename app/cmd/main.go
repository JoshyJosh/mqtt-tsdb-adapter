package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"taos-adapter/models"
	"taos-adapter/mqtt"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var serverPort string

const (
	envServerPort   = "SERVER_PORT"
	envTDDBPort     = "TDENGINE_PORT"
	envTDDBHost     = "TDENGINE_HOST"
	envTDDBUser     = "TDENGINE_USER"
	envTDDBPass     = "TDENGINE_PASS"
	envTDDBName     = "TDENGINE_DBNAME"
	envMQTTPort     = "MQTT_PORT"
	envMQTTHost     = "MQTT_HOST"
	envMQTTSubTopic = "MQTT_SUB_TOPIC"
	envMQTTSubQos   = "MQTT_SUB_QOS"
	envMQTTClientID = "MQTT_CLIENT_ID"
	envMQTTUser     = "MQTT_USER"
	envMQTTPass     = "MQTT_PASS"
)

// @todo consider initializing all env stuff here to get clear error messages down the line.
func init() {
	envFileFlag := flag.String("env-file", "", "env file to read")
	flag.Parse()

	if envFileFlag != nil && *envFileFlag != "" {
		if err := godotenv.Load(*envFileFlag); err != nil {
			panic(errors.Wrapf(err, "failed to read env file: %s", envFileFlag))
		}
	}

	missingParams := []string{}
	if serverPort = os.Getenv(envServerPort); serverPort == "" {
		missingParams = append(missingParams, envServerPort)
	}

	// check TDEngine env vars

	tddbPortStr := os.Getenv(envTDDBPort)
	var tddbPort int64 = 6030
	if tddbPortStr != "" {
		var err error
		tddbPort, err = strconv.ParseInt(tddbPortStr, 10, 64)

		if err != nil {
			panic(errors.Wrapf(err, "failed to read %s variable", envTDDBPort))
		}
	}

	fmt.Println(tddbPort)

	tddbHost := os.Getenv(envTDDBHost)
	if tddbHost == "" {
		missingParams = append(missingParams, envTDDBHost)
	}

	tddbUser := os.Getenv(envTDDBUser)
	if tddbUser == "" {
		missingParams = append(missingParams, envTDDBUser)
	}

	tddbPass := os.Getenv(envTDDBPass)
	if tddbPass == "" {
		missingParams = append(missingParams, envTDDBPass)
	}

	tddbName := os.Getenv(envTDDBName)
	if tddbName == "" {
		missingParams = append(missingParams, envTDDBName)
	}

	// check MQTT env vars
	mqttPortStr := os.Getenv(envMQTTPort)
	var mqttPort int64 = 1883
	if mqttPortStr != "" {
		var err error
		mqttPort, err = strconv.ParseInt(mqttPortStr, 10, 64)

		if err != nil {
			panic(errors.Wrapf(err, "failed to read %s variable", envMQTTPort))
		}
	}

	mqttHost := os.Getenv(envMQTTHost)
	if mqttHost == "" {
		missingParams = append(missingParams, envMQTTHost)
	}

	mqttUser := os.Getenv(envMQTTUser)
	if mqttUser == "" {
		missingParams = append(missingParams, envMQTTUser)
	}

	mqttPass := os.Getenv(envMQTTPass)
	if mqttPass == "" {
		missingParams = append(missingParams, envMQTTPass)
	}

	mqttClientID := os.Getenv(envMQTTClientID)
	if mqttClientID == "" {
		missingParams = append(missingParams, envMQTTClientID)
	}

	mqttSubTopic := os.Getenv(envMQTTSubTopic)
	if mqttSubTopic == "" {
		missingParams = append(missingParams, envMQTTSubTopic)
	}

	var mqttSubQos int64 = 1
	mqttSubQosStr := os.Getenv(envMQTTSubQos)
	if mqttSubQosStr == "" {
		missingParams = append(missingParams, envMQTTSubQos)
	} else {
		var err error
		mqttSubQos, err = strconv.ParseInt(mqttSubQosStr, 10, 64)
		if err != nil {
			panic(err)
		}
	}

	if len(missingParams) > 0 {
		panic(fmt.Sprintf("missing required env variables: %s", strings.Join(missingParams, ", ")))
	}

	// db.SetDBVars(int(tddbPort), tddbHost, tddbUser, tddbPass, tddbName)

	mqtt.SetMQTTVars(int(mqttPort), int(mqttSubQos), mqttHost, mqttUser, mqttPass, mqttClientID, mqttSubTopic)
}

var connLive bool

func main() {
	// @todo add signals
	r := gin.Default()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := logrus.New()

	tbMetrics := make(chan models.TimeBasedMetrics, 10)

	// fmt.Println("testing123")
	// msgChan, err := mqtt.ConnectAndSub(ctx, logrus.NewEntry(log).WithField("stage", "mqtt-init"))
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("testing456")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		fmt.Println("starting mqtt")
		defer wg.Done()
		logEntry := logrus.NewEntry(log).WithField("stage", "mqtt")
		// logEntry := logrus.NewEntry(log)
		err := mqtt.Sub(ctx, logEntry, tbMetrics)
		if err != nil {
			log.Error(errors.Wrap(err, "exiting mqtt coroutine"))
		}
		log.Info("exiting mqtt")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.GET("/status", k8sProbeHandler)
		err := r.Run(fmt.Sprintf(":%s", serverPort))
		if err != nil {
			log.Error(errors.Wrap(err, "exiting server coroutine"))
		}
	}()

	wg.Wait()
}

func k8sProbeHandler(ctx *gin.Context) {
	const liveQuery = "live"

	if connLive {
		ctx.String(http.StatusAccepted, "")
		return
	}

	ctx.String(http.StatusNotFound, "")
}
