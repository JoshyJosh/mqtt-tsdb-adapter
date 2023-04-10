package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"taos-adapter/models"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/af"
)

var databaseMap map[string]struct{}
var host, user, pass, dbName, serverPort string
var port int = 6030

func SetDBVars(portVar int, hostVar, userVar, passVar, dbNameVar string) {
	port = portVar
	host = hostVar
	user = userVar
	pass = passVar
	dbName = dbNameVar
}

/* PingDatabase is used to check if the database is reachable for connections and get current table list */
func InitDatabase(ctx context.Context) {
	logrus.Infof("Connecting to %s:%d %s//%s\n", host, int(port), user, pass)
	conn, err := af.Open(host, user, pass, "", int(port))
	if err != nil {
		fmt.Println("failed to init connect, err:", err)
	}
	defer conn.Close()

	rows, err := conn.Query("SHOW DATABASES;")
	if err != nil {
		logrus.Printf("failed to create database %s", dbName)
		panic(err)
	}
	defer rows.Close()

	databases := make([]driver.Value, len(rows.Columns()))
	databaseMap := map[string]struct{}{}
	i := 0

dbLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			defer func() {
				i++
			}()

			if err := rows.Next(databases); err != nil {
				if errors.Is(err, io.EOF) {
					logrus.Infof("retrieved database list:\n %v", databases)
					break dbLoop
				} else {
					logrus.Error(errors.Wrap(err, "failed to read databases"))
					panic(err)
				}
			}

			databaseMap[databases[i].(string)] = struct{}{}
		}
	}

}

func CreateDatabase(ctx context.Context, dbName string) error {
	logrus.Infof("Creating database: %s", dbName)

	conn, err := getConn(dbName)
	if err != nil {
		return errors.Wrapf(err, "failed to create database: %s", dbName)
	}
	defer conn.Close()

	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbName))
	if err != nil {
		errMsg := fmt.Sprintf("failed to create database %s", dbName)
		logrus.Error(errMsg)
		return errors.Wrapf(err, errMsg)
	}

	return nil
}

/* GetConn gets tdengine connection. Remember to close it */
func getConn(dbName string) (*af.Connector, error) {
	logrus.Printf("Connecting to %s:%d %s//%s\n", host, int(port), user, pass)
	conn, err := af.Open(host, user, pass, "", int(port))
	if err != nil {
		logrus.Println("failed to init connect, err: ", err)
		return nil, errors.Wrap(err, "failed to connect to tdengine")
	}

	return conn, nil
}

func InsertDatad(ctx context.Context, tbMetrics chan models.TimeBasedMetrics) error {
	for tbMetric := range tbMetrics {
		conn, err := getConn("")
		if err != nil {
			logrus.Error(err)
			errMsg := fmt.Sprintf("failed to initial connect to database")
			logrus.Error(errMsg)
			return err
		}
		defer conn.Close()

		if _, ok := databaseMap[tbMetric.DB]; !ok {
			if _, err := conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", dbName)); err != nil {
				errMsg := fmt.Sprintf("failed to create database %s", dbName)
				logrus.Error(errMsg)
				return errors.Wrapf(err, errMsg)
			}

			databaseMap[tbMetric.DB] = struct{}{}
		}

		if _, err := conn.Execf("USE %s", tbMetric.DB); err != nil {
			logrus.Error(err)
			continue
		}

		var tagStr []string
		for key, val := range tbMetric.Tags {
			tagStr = append(tagStr, fmt.Sprintf("%s=%g", key, val))
		}

		var metricStr []string
		for key, val := range tbMetric.Metrics {
			tagStr = append(metricStr, fmt.Sprintf("%s=%s", key, val))
		}

		tdenginePayload := fmt.Sprintf("%s,%s %s %d", tbMetric.Table, strings.Join(tagStr, ","), strings.Join(metricStr, ","), tbMetric.Timestamp.Unix())

		if err := conn.InfluxDBInsertLines([]string{tdenginePayload}, "ms"); err != nil {
			logrus.Error(err)
		}
	}

	return nil
}
