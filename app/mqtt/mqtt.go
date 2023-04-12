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

	"github.com/eclipse/paho.golang/paho"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var host, user, pass, clientID, subTopic string
var port int = 1883
var subQos int = 1

func SetMQTTVars(portVar, subQosVar int, hostVar, userVar, passVar, clientIDVar, subTopicVar string) {
	port = portVar
	host = hostVar
	user = userVar
	pass = passVar
	clientID = clientIDVar
	subTopic = subTopicVar
	subQos = subQosVar
}

// func ConnectAndSub(ctx context.Context, log *logrus.Entry) (chan *paho.Publish, error) {
// 	server := fmt.Sprintf("%s:%d", host, port)

// 	conn, err := net.Dial("tcp", server)
// 	if err != nil {
// 		log.Fatalf("Failed to connect to %s: %s", server, err)
// 		return nil, err
// 	}

// 	msgChan := make(chan *paho.Publish)

// 	c := paho.NewClient(paho.ClientConfig{
// 		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
// 			msgChan <- m
// 		}),
// 		Conn: conn,
// 	})
// 	c.SetErrorLogger(log)

// 	cp := &paho.Connect{
// 		KeepAlive:  30,
// 		ClientID:   clientID,
// 		CleanStart: true,
// 		Username:   user,
// 		Password:   []byte(pass),
// 	}

// 	if user != "" {
// 		cp.UsernameFlag = true
// 	}
// 	if pass != "" {
// 		cp.PasswordFlag = true
// 	}

// 	ca, err := c.Connect(ctx, cp)
// 	if err != nil {
// 		log.Fatalln(err)
// 		return nil, err
// 	}

// 	defer func() {
// 		err = c.Disconnect(&paho.Disconnect{})
// 		log.Error(errors.Wrap(err, "failed to disconnect from mqtt"))
// 	}()

// 	if ca.ReasonCode != 0 {
// 		log.Fatalf("Failed to connect to %s : %d - %s", server, ca.ReasonCode, ca.Properties.ReasonString)
// 		return nil, err
// 	}

// 	log.Infof("Connected to %s\n", server)

// 	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
// 		Subscriptions: map[string]paho.SubscribeOptions{
// 			"distest/#": {QoS: byte(subQos)},
// 		},
// 	})

// 	if err != nil {
// 		log.Fatalln(err)
// 		return nil, err
// 	}

// 	if sa.Reasons[0] != byte(subQos) {
// 		log.Fatalf("Failed to subscribe to %s : %d", *topic, sa.Reasons[0])
// 	}

// 	// if sa.Reasons[0] != []byte(subQos)[0] {
// 	// 	log.Fatalf("Failed to subscribe to %s : %d", subTopic, sa.Reasons[0])
// 	// 	return nil, err
// 	// }

// 	log.Infof("Subscribed to topic: %s", subTopic)

// 	return msgChan, nil
// }

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

			topicSlice := strings.Split(m.Topic, "/")
			dbName := topicSlice[1]
			// table := topicSlice[2]

			log.Info("Received message: ", string(m.Payload))
			log.Infof("%#v\n", m.Properties.User)

			// var timestamp time.Time
			// var err error

			// for _, prop := range m.Properties.User {
			// 	if prop.Key == "timestamp" {
			// 		timestamp, err = time.Parse(time.RFC3339, prop.Value)
			// 		if err != nil {
			// 			log.Error(err)
			// 			continue
			// 		}
			// 	}
			// }

			// var metrics map[string]float64
			// var tags map[string]string
			// if bytes.Contains(m.Payload, []byte("{")) {
			// 	metrics, tags, err = parseJSON(m.Payload)
			// 	if err != nil {
			// 		log.Error(err)
			// 		continue
			// 	}
			// } else {
			// 	metrics, tags = parseCSV(m.Payload)
			// }

			// tbMetrics <- models.TimeBasedMetrics{
			// 	// Metrics:   metrics,
			// 	// Tags:      tags,
			// 	// Timestamp: timestamp,
			// 	DB:    dbName,
			// 	Table: table, // @todo determine way to set table and db name
			// }

			// tdenginePayload := fmt.Sprintf("%s,%s %d", dbName, payloadMetrics, timestamp)

			// log.Info("Timestamp: ", timestamp)
			log.Info("dbName: ", dbName)
		}
	}

	return nil
}

func parseCSV(body []byte) (map[string]float64, map[string]string) {

	rows := bytes.Split(body, []byte("\n"))

	// split headers for measurement names
	headers := bytes.Split(rows[0], []byte(";"))
	fieldNames := make([]string, len(headers))

	for i := range headers {
		fieldNames[i] = string(headers[i])
	}

	// @todo make batch inputs
	floatMap := map[string]float64{}
	tagMap := map[string]string{}

	// determine datatypes of values
	for i := 1; i < len(rows); i++ {
		// @todo cause error if a space is in a row
		row := bytes.Split(rows[i], []byte(";"))

		for j := range row {
			rowStr := string(row[j])
			valFloat := parseValue(rowStr)
			if valFloat != nil {
				floatMap[fieldNames[j]] = *valFloat
			} else {
				tagMap[fieldNames[j]] = string(rowStr)
			}
		}
	}

	return floatMap, tagMap
}

func parseValue(rowData string) *float64 {
	valFloat, err := strconv.ParseFloat(rowData, 64)
	if err != nil {
		return nil
	}

	return &valFloat
}

func parseJSON(body []byte) (map[string]float64, map[string]string, error) {
	var jsonMap map[string]interface{}

	err := json.Unmarshal(body, &jsonMap)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to parse mqtt payload")
	}

	metricsMap := map[string]float64{}
	tagMap := map[string]string{}

	for key, value := range jsonMap {
		switch t := value.(type) {
		case float64:
			val, ok := value.(float64)
			if !ok {
				return nil, nil, errors.New("failed to cast json value to float")
			}

			metricsMap[key] = val

		case string:
			val, ok := value.(string)
			if !ok {
				return nil, nil, errors.New("failed to cast json value to string")
			}

			tagMap[key] = val
		default:
			return nil, nil, fmt.Errorf("failed to case unexpected json type: %T", t)
		}
	}

	return metricsMap, tagMap, nil
}
