package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/af"
)

var databaseList map[string]struct{}
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
	for {
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

func InsertData(insertData map[string]any) error {

}
