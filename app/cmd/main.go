package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"taos-adapter/db"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

var serverPort string

const (
	envServerPort = "SERVER_PORT"
	envPort       = "TDENGINE_PORT"
	envHost       = "TDENGINE_HOST"
	envUser       = "TDENGINE_USER"
	envPass       = "TDENGINE_PASS"
	envDBName     = "TDENGINE_DBNAME"
)

// @todo consider initializing all env stuff here to get clear error messages down the line.
func init() {
	missingParams := []string{}
	if serverPort = os.Getenv(envServerPort); serverPort == "" {
		missingParams = append(missingParams, envServerPort)
	}

	dbPortStr := os.Getenv(envPort)
	var dbPort int64 = 6030
	if dbPortStr != "" {
		var err error
		dbPort, err = strconv.ParseInt(dbPortStr, 10, 64)

		if err != nil {
			panic(errors.Wrapf(err, "failed to read %s variable", envPort))
		}
	}

	host := os.Getenv(envHost)
	if host == "" {
		missingParams = append(missingParams, envHost)
	}

	user := os.Getenv(envUser)
	if user == "" {
		missingParams = append(missingParams, envUser)
	}

	pass := os.Getenv(envPass)
	if pass == "" {
		missingParams = append(missingParams, envPass)
	}

	if len(missingParams) > 0 {
		panic(fmt.Sprintf("missing required env variables: %s", strings.Join(missingParams, ", ")))
	}

	db.SetDBVars(host, int(dbPort), user, pass)
}

var connLive bool

func main() {
	r := gin.Default()
	ctx := context.Background()

	db.InitDatabase(ctx)

	r.GET("/status", k8sProbeHandler)

	r.Run(fmt.Sprintf(":%s", serverPort))
}

func k8sProbeHandler(ctx *gin.Context) {
	const liveQuery = "live"

	if connLive {
		ctx.String(http.StatusAccepted, "")
		return
	}

	ctx.String(http.StatusNotFound, "")
}
