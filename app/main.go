package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/taosdata/driver-go/v3/af"
)

func initDatabase() {
	fmt.Printf("Connecting to %s:%d %s//%s\n", host, int(port), user, pass)
	conn, err := af.Open(host, user, pass, "", int(port))
	if err != nil {
		fmt.Println("failed to init connect, err:", err)
	}
	defer conn.Close()

	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;"))
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("USE test")
	if err != nil {
		panic(err)
	}
}

var host, user, pass, dbName, serverPort string
var port int64 = 6030

const (
	envPort       = "TDENGINE_PORT"
	envHost       = "TDENGINE_HOST"
	envUser       = "TDENGINE_USER"
	envPass       = "TDENGINE_PASS"
	envDBName     = "TDENGINE_DBNAME"
	envServerPort = "SERVER_PORT"
)

func init() {
	portStr := os.Getenv(envPort)
	if portStr != "" {
		var err error
		port, err = strconv.ParseInt(portStr, 10, 64)
		if err != nil {
			panic(errors.Wrapf(err, "failed to read %s variable", envDBName))
		}
	}

	missingParams := []string{}

	if host = os.Getenv(envHost); host == "" {
		missingParams = append(missingParams, envHost)
	}

	if user = os.Getenv(envUser); user == "" {
		missingParams = append(missingParams, envUser)
	}

	if pass = os.Getenv(envPass); pass == "" {
		missingParams = append(missingParams, envPass)
	}

	if dbName = os.Getenv(envDBName); dbName == "" {
		missingParams = append(missingParams, envDBName)
	}

	if serverPort = os.Getenv(envServerPort); serverPort == "" {
		missingParams = append(missingParams, envServerPort)
	}

	if len(missingParams) > 0 {
		panic(fmt.Sprintf("missing required env variables: %s", strings.Join(missingParams, ", ")))
	}
}

var connLive bool

func main() {
	r := gin.Default()

	r.GET("/status", k8sProbeHandler)

	go r.Run(fmt.Sprintf(":%s", serverPort))

	initDatabase()

	connLive = true
	// var lines = []string{
	// 	"meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
	// 	"meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
	// 	"meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
	// 	"meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
	// }

	// err = conn.InfluxDBInsertLines(lines, "ms")
	// if err != nil {
	// 	log.Fatalln("insert error:", err)
	// }

	time.Sleep(30 * time.Minute)
}

func k8sProbeHandler(ctx *gin.Context) {
	const liveQuery = "live"

	if connLive {
		ctx.String(http.StatusAccepted, "")
		return
	}

	ctx.String(http.StatusNotFound, "")
}
