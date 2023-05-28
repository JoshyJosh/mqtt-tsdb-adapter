package mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"taos-adapter/models"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var host, user, pass, clientID, subTopic string
var port int = 1883
var subQos int = 1

const TIMESTAMP_FIELD string = "timestamp"

func SetMQTTVars(portVar, subQosVar int, hostVar, userVar, passVar, clientIDVar, subTopicVar string) {
	port = portVar
	host = hostVar
	user = userVar
	pass = passVar
	clientID = clientIDVar
	subTopic = subTopicVar
	subQos = subQosVar
}

func Sub(ctx context.Context, log *logrus.Entry, tbMetrics chan models.TimeBasedMetrics) error {
	server := fmt.Sprintf("%s:%d", host, port)

	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", server, err)
		return err
	}

	msgChan := make(chan *paho.Publish)

	c := paho.NewClient(paho.ClientConfig{
		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
			msgChan <- m
		}),
		Conn: conn,
	})
	c.SetErrorLogger(log)

	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   clientID,
		CleanStart: true,
		Username:   user,
		Password:   []byte(pass),
	}

	if user != "" {
		cp.UsernameFlag = true
	}
	if pass != "" {
		cp.PasswordFlag = true
	}

	ca, err := c.Connect(ctx, cp)
	if err != nil {
		log.Fatalln(err)
		return err
	}

	defer func() {
		err = c.Disconnect(&paho.Disconnect{})
		if err != nil {
			log.Error(errors.Wrap(err, "failed to disconnect from mqtt"))
		}
	}()

	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", server, ca.ReasonCode, ca.Properties.ReasonString)
		return err
	}

	log.Infof("Connected to %s\n", server)

	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			subTopic: {QoS: byte(subQos)},
		},
	})

	if err != nil {
		log.Fatalln(err)
		return err
	}

	if sa.Reasons[0] != byte(subQos) {
		log.Fatalf("Failed to subscribe to %s : %d", subTopic, sa.Reasons[0])
	}

	log.Infof("Subscribed to topic: %s", subTopic)

	for {
		select {
		case <-ctx.Done():
			log.Info("context done")
			return nil
		case m := <-msgChan:
			log.Info("looping in msgChan")

			log.Info(m.Topic)

			// topic should be structured as db_name/table
			topicSlice := strings.Split(m.Topic, "/")
			dbName := topicSlice[0]
			table := topicSlice[1]

			var timestamp time.Time
			var err error

			var metrics map[string]float64
			var tags map[string]string
			if bytes.Contains(m.Payload, []byte("{")) {
				metrics, tags, timestamp, err = parseJSON(m.Payload, log)
			} else {
				metrics, tags, timestamp, err = parseCSV(m.Payload, log)
			}

			if err != nil {
				log.Error(err)
				continue
			}

			var emptyTime time.Time

			if timestamp == emptyTime {
				for _, prop := range m.Properties.User {
					if prop.Key == TIMESTAMP_FIELD {
						timestamp, err = time.Parse(time.RFC3339, prop.Value)
						if err != nil {
							log.Error(err)
							continue
						}
					}
				}

				// @todo set to brokers time if no timestamp has been found.
				// set to adapters time if no timestamp has been found.
				if timestamp == emptyTime {
					timestamp = time.Now()
				}
			}

			tbMetrics <- models.TimeBasedMetrics{
				Metrics:   metrics,
				Tags:      tags,
				Timestamp: timestamp,
				DB:        dbName,
				Table:     table, // @todo determine way to set table and db name
			}
		}
	}

	return nil
}

func parseCSV(body []byte, log *logrus.Entry) (map[string]float64, map[string]string, time.Time, error) {
	rows := bytes.Split(body, []byte("\n"))

	// split headers for measurement names
	headers := bytes.Split(rows[0], []byte(";"))
	fieldNames := make([]string, len(headers))
	timestampIdx := -1

	for i := range headers {
		fieldNames[i] = string(headers[i])
		if fieldNames[i] == TIMESTAMP_FIELD {
			timestampIdx = i
		}
	}

	// @todo make batch inputs
	floatMap := map[string]float64{}
	tagMap := map[string]string{}
	var timestamp time.Time

	// determine datatypes of values
	for i := 1; i < len(rows); i++ {
		// @todo cause error if a space is in a row
		row := bytes.Split(rows[i], []byte(";"))

		// check if csv value row length does not match the header length
		if len(row) != len(fieldNames) {
			return nil, nil, timestamp, fmt.Errorf("row value length does not match header length. Header count: %d Row count: %d", len(fieldNames), len(row))
		}

		for j := range row {
			if j == timestampIdx {
				var err error
				timestamp, err = bytesToUnixTimestamp(row[j])
				if err != nil {
					return nil, nil, timestamp, errors.Wrap(err, "failed to parse CSV timestamp")
				}
				continue
			}

			rowStr := string(row[j])
			valFloat := parseValue(rowStr)
			if valFloat != nil {
				floatMap[fieldNames[j]] = *valFloat
			} else {
				tagMap[fieldNames[j]] = string(rowStr)
			}
		}
	}

	return floatMap, tagMap, timestamp, nil
}

func parseValue(rowData string) *float64 {
	valFloat, err := strconv.ParseFloat(rowData, 64)
	if err != nil {
		return nil
	}

	return &valFloat
}

func parseJSON(body []byte, log *logrus.Entry) (map[string]float64, map[string]string, time.Time, error) {
	var jsonMap map[string]interface{}
	var timestamp time.Time

	err := json.Unmarshal(body, &jsonMap)
	if err != nil {
		return nil, nil, timestamp, errors.Wrap(err, "failed to parse mqtt payload")
	}

	metricsMap := map[string]float64{}
	tagMap := map[string]string{}

	for key, value := range jsonMap {
		if key == TIMESTAMP_FIELD {
			timestamp, err = bytesToUnixTimestamp(value)
			if err != nil {
				return nil, nil, timestamp, errors.Wrap(err, "failed to parse JSON timestamp")
			}

			continue
		}

		switch t := value.(type) {
		case float64:
			val, ok := value.(float64)
			if !ok {
				return nil, nil, timestamp, errors.New("failed to cast json value to float")
			}

			metricsMap[key] = val

		case string:
			val, ok := value.(string)
			if !ok {
				return nil, nil, timestamp, errors.New("failed to cast json value to string")
			}

			tagMap[key] = val
		default:
			return nil, nil, timestamp, fmt.Errorf("failed to cast unexpected json type: %T", t)
		}
	}

	return metricsMap, tagMap, timestamp, nil
}

func bytesToUnixTimestamp(rawTime interface{}) (time.Time, error) {
	var timestamp time.Time
	var err error

	switch rawTime.(type) {
	case int:
		v := rawTime.(int)
		timestamp = time.Unix(int64(v), 0)
	case float64:
		v := rawTime.(float64)
		timestamp = time.Unix(int64(v), 0)
	case string:
		var timeInt int
		timeInt, err = strconv.Atoi(rawTime.(string))
		if err != nil {
			err = errors.Wrapf(err, "failed to cast timestamp string %s to int", rawTime)
			break
		}

		timestamp = time.Unix(int64(timeInt), 0)
	case []byte:
		var timeInt int
		timeInt, err = strconv.Atoi(string(rawTime.([]byte)[:]))
		if err != nil {
			err = errors.Wrapf(err, "failed to cast timestamp byte type cast string %s to int", fmt.Sprint(rawTime))
			break
		}

		timestamp = time.Unix(int64(timeInt), 0)
	default:
		var timeInt int
		timeInt, err = strconv.Atoi(fmt.Sprint(rawTime))
		if err != nil {
			err = errors.Wrapf(err, "failed to cast timestamp default type cast string %s to int", fmt.Sprint(rawTime))
			break
		}

		timestamp = time.Unix(int64(timeInt), 0)
	}

	// assume measurements are in UNIX timestamps
	return timestamp, err
}
