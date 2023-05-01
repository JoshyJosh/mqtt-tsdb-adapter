package mqtt

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func TestParseJSON(t *testing.T) {
	cases := []struct {
		name              string
		body              []byte
		expectedMetrics   map[string]float64
		expectedTags      map[string]string
		expectedTimestamp time.Time
		expectedError     bool
	}{
		{
			name: "Success: All fields present",
			body: []byte(`{"timestamp":1257894000,"name":"test_name","tag":"test_tag","temp":12.34,"preassure":10.23,"count":9}`),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
	}

	var log = logrus.New()

	for _, c := range cases {
		t.Logf("starting test case: %s", c.name)

		metrics, tags, timestamp, err := parseJSON(c.body, logrus.NewEntry(log))
		if err != nil {
			if !c.expectedError {
				t.Fatal(errors.Wrap(err, "unexpected error"))
			} else {
				continue
			}
		}

		if len(c.expectedMetrics) != len(metrics) {
			t.Fatalf("expected no. of metrics %d, got %d", len(c.expectedMetrics), len(metrics))
		}

		for metricKey, metricVal := range metrics {
			expectedVal, ok := c.expectedMetrics[metricKey]
			if !ok {
				t.Fatalf("unexpected metric: %s", metricKey)
			}

			if expectedVal != metricVal {
				t.Fatalf("unexpected value for key: %s, expected metric: %g, got: %g", metricKey, expectedVal, metricVal)
			}
		}

		if len(c.expectedTags) != len(tags) {
			t.Fatalf("expected no. of tags %d, got %d", len(c.expectedTags), len(tags))
		}

		for tagKey, tagVal := range tags {
			expectedVal, ok := c.expectedTags[tagKey]
			if !ok {
				t.Fatalf("unexpected tag: %s", tagKey)
			}

			if expectedVal != tagVal {
				t.Fatalf("unexpected value for key: %s, expected tag: %s, got: %s", tagKey, expectedVal, tagVal)
			}
		}

		if c.expectedTimestamp != timestamp {
			t.Fatalf("expected timestamp: %d got: %d", c.expectedTimestamp.Unix(), timestamp.Unix())
		}
	}
}

func TestParseCSV(t *testing.T) {
	cases := []struct {
		name              string
		body              []byte
		expectedMetrics   map[string]float64
		expectedTags      map[string]string
		expectedTimestamp time.Time
		expectedError     bool
	}{
		{
			name: "Success: All fields present",
			body: []byte(
				"timestamp;name;tag;temp;preassure;count\n1257894000;test_name;test_tag;12.34;10.23;9",
			),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
	}

	var log = logrus.New()

	for _, c := range cases {
		t.Logf("starting test case: %s", c.name)

		metrics, tags, timestamp, err := parseCSV(c.body, logrus.NewEntry(log))
		if err != nil {
			if !c.expectedError {
				t.Fatal(errors.Wrap(err, "unexpected error"))
			} else {
				continue
			}
		}

		if len(c.expectedMetrics) != len(metrics) {
			t.Fatalf("expected no. of metrics %d, got %d", len(c.expectedMetrics), len(metrics))
		}

		for metricKey, metricVal := range metrics {
			expectedVal, ok := c.expectedMetrics[metricKey]
			if !ok {
				t.Fatalf("unexpected metric: %s", metricKey)
			}

			if expectedVal != metricVal {
				t.Fatalf("unexpected value for key: %s, expected metric: %g, got: %g", metricKey, expectedVal, metricVal)
			}
		}

		if len(c.expectedTags) != len(tags) {
			t.Fatalf("expected no. of tags %d, got %d", len(c.expectedTags), len(tags))
		}

		for tagKey, tagVal := range tags {
			expectedVal, ok := c.expectedTags[tagKey]
			if !ok {
				t.Fatalf("unexpected tag: %s", tagKey)
			}

			if expectedVal != tagVal {
				t.Fatalf("unexpected value for key: %s, expected tag: %s, got: %s", tagKey, expectedVal, tagVal)
			}
		}

		if c.expectedTimestamp != timestamp {
			t.Fatalf("expected timestamp: %d got: %d", c.expectedTimestamp.Unix(), timestamp.Unix())
		}
	}
}
