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

var databaseMap = map[string]struct{}{}
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
	conn, err := getConn("")
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

	for {
		select {
		case <-ctx.Done():
			return
		default:
			defer func() {
				i++
			}()

			err := rows.Next(databases)
			if err != nil {
				if errors.Is(err, io.EOF) {
					logrus.Infof("retrieved database list:\n %v", databases)
					break
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
	logrus.Info("Connected to tdengine")

	return conn, nil
}

func InsertDatad(ctx context.Context, tbMetrics chan models.TimeBasedMetrics) error {
	conn, err := getConn("")
	if err != nil {
		logrus.Error(errors.Wrap(err, "failed to initial connect to database"))
		return err
	}
	defer conn.Close()

	for tbMetric := range tbMetrics {
		logrus.Info(tbMetric.DB)
		if _, ok := databaseMap[tbMetric.DB]; !ok {
			logrus.Infof("creating database %s", tbMetric.DB)

			if _, err := conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", tbMetric.DB)); err != nil {
				errMsg := fmt.Sprintf("failed to create database %s", tbMetric.DB)
				logrus.Error(errMsg)
				return errors.Wrapf(err, errMsg)
			}

			databaseMap[tbMetric.DB] = struct{}{}
		}

		if _, err := conn.Exec(fmt.Sprintf("USE %s", tbMetric.DB)); err != nil {
			logrus.Error(err)
			continue
		}

		tagStr, metricStr := compileTDEngineMetricsAndTags(tbMetric)

		tdenginePayload := []string{fmt.Sprintf("%s,%s %s %d", tbMetric.Table, strings.Join(tagStr, ","), strings.Join(metricStr, ","), tbMetric.Timestamp.Unix())}
		logrus.Info(tdenginePayload[0])

		if err := conn.InfluxDBInsertLines(tdenginePayload, "s"); err != nil {
			logrus.Error(err)
		}
	}

	return nil
}

func compileTDEngineMetricsAndTags(tbMetric models.TimeBasedMetrics) (tagStr, metricStr []string) {
	for key, val := range tbMetric.Tags {
		tagStr = append(tagStr, fmt.Sprintf("%s=%s", key, val))
	}

	if len(tagStr) == 0 {
		tagStr = []string{"nullTag=null"}
	}

	for key, val := range tbMetric.Metrics {
		metricStr = append(metricStr, fmt.Sprintf("%s=%g", key, val))
	}

	if len(metricStr) == 0 {
		metricStr = []string{"nullVal=0"}
	}

	return
}
