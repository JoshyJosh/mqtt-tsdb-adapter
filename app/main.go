package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE IF EXISTS test")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("USE test")
	if err != nil {
		panic(err)
	}
}

var host, user, pass, dbName string
var port int64 = 6030

var (
	envPort   = "TDENGINE_PORT"
	envHost   = "TDENGINE_HOST"
	envUser   = "TDENGINE_USER"
	envPass   = "TDENGINE_PASS"
	envDBName = "TDENGINE_DBNAME"
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

	if len(missingParams) > 0 {
		panic(fmt.Sprintf("missing required env variables: %s", strings.Join(missingParams, ", ")))
	}
}

var conn *af.Connector

func main() {
	http.HandleFunc("/", statusHandler)

	go http.ListenAndServe(":8000", nil)

	fmt.Printf("Connecting to %s:%d %s//%s\n", host, int(port), user, pass)
	var err error
	conn, err = af.Open(host, user, pass, dbName, int(port))
	if err != nil {
		fmt.Println("failed to connect, err:", err)
	}
	defer conn.Close()

	prepareDatabase(conn)
	var lines = []string{
		"meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
		"meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
		"meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
		"meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
	}

	err = conn.InfluxDBInsertLines(lines, "ms")
	if err != nil {
		log.Fatalln("insert error:", err)
	}
}

func statusHandler(res http.ResponseWriter, req *http.Request) {
	if conn == nil {
		fmt.Fprintf(res, "live")
	}

	fmt.Fprintf(res, "ready")
}
