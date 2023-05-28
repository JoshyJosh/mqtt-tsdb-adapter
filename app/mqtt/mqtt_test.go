package mqtt

import (
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func TestParseJSON(t *testing.T) {
	t.Parallel()

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
		{
			name: "Success: Metrics and timestamp",
			body: []byte(`{"timestamp":1257894000,"temp":12.34,"preassure":10.23,"count":9}`),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
		{
			name: "Success: Tags and timestamp",
			body: []byte(`{"timestamp":1257894000,"name":"test_name","tag":"test_tag"}`),
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
		{
			name: "Success: Only metrics",
			body: []byte(`{"temp":12.34,"preassure":10.23,"count":9}`),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
		},
		{
			name: "Success: Only tags",
			body: []byte(`{"name":"test_name","tag":"test_tag"}`),
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
		},
		{
			name:          "Failure: Invalid JSON body",
			body:          []byte(`this is not JSON`),
			expectedError: true,
		},
		{
			name:          "Failure: Invalid JSON value",
			body:          []byte(`{"invalid_field":0x88}`),
			expectedError: true,
		},
	}

	var log = logrus.New()
	failedTests := []string{}

testCaseLoop:
	for _, c := range cases {
		t.Logf("starting test case: %s", c.name)

		metrics, tags, timestamp, err := parseJSON(c.body, logrus.NewEntry(log))
		if err != nil {
			if !c.expectedError {
				t.Error(errors.Wrap(err, "unexpected error"))
				failedTests = append(failedTests, c.name)
			}

			continue
		}

		if len(c.expectedMetrics) != len(metrics) {
			t.Errorf("expected no. of metrics %d, got %d", len(c.expectedMetrics), len(metrics))
			failedTests = append(failedTests, c.name)
			continue
		}

		for metricKey, metricVal := range metrics {
			expectedVal, ok := c.expectedMetrics[metricKey]
			if !ok {
				t.Errorf("unexpected metric: %s", metricKey)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}

			if expectedVal != metricVal {
				t.Errorf("unexpected value for key: %s, expected metric: %g, got: %g", metricKey, expectedVal, metricVal)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}
		}

		if len(c.expectedTags) != len(tags) {
			t.Errorf("expected no. of tags %d, got %d", len(c.expectedTags), len(tags))
			failedTests = append(failedTests, c.name)
			continue
		}

		for tagKey, tagVal := range tags {
			expectedVal, ok := c.expectedTags[tagKey]
			if !ok {
				t.Errorf("unexpected tag: %s", tagKey)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}

			if expectedVal != tagVal {
				t.Errorf("unexpected value for key: %s, expected tag: %s, got: %s", tagKey, expectedVal, tagVal)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}
		}

		if c.expectedTimestamp != timestamp {
			t.Fatalf("expected timestamp: %d got: %d", c.expectedTimestamp.Unix(), timestamp.Unix())
			failedTests = append(failedTests, c.name)
		}
	}

	if len(failedTests) > 0 {
		t.Errorf("failure in test cases: %s", strings.Join(failedTests, ","))
	}
}

func TestParseCSV(t *testing.T) {
	t.Parallel()

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
		{
			name: "Success: Metrics and timestamp",
			body: []byte(
				"timestamp;temp;preassure;count\n1257894000;12.34;10.23;9",
			),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
		{
			name: "Success: Tags and timestamp",
			body: []byte(
				"timestamp;name;tag\n1257894000;test_name;test_tag",
			),
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
			expectedTimestamp: time.Unix(1257894000, 0),
		},
		{
			name: "Success: Only metrics",
			body: []byte(
				"temp;preassure;count\n12.34;10.23;9",
			),
			expectedMetrics: map[string]float64{
				"temp":      12.34,
				"preassure": 10.23,
				"count":     9,
			},
		},
		{
			name: "Success: Only tags",
			body: []byte(
				"name;tag\ntest_name;test_tag",
			),
			expectedTags: map[string]string{
				"tag":  "test_tag",
				"name": "test_name",
			},
		},
		{
			name: "Success: HEX CSV value, dosen't work in JSON",
			body: []byte("invalid_field\n0x88"),
			expectedTags: map[string]string{
				"invalid_field": "0x88",
			},
		},
		{
			name: "Failure: Headers only",
			body: []byte(
				"timestamp;name;tag;temp;preassure;count",
			),
			expectedError: true,
		},
		{
			name:          "Failure: Invalid CSV body",
			body:          []byte(`this is not CSV`),
			expectedError: true,
		},
		{
			name: "Failure: Greater value column size",
			body: []byte(
				"timestamp;name;tag;temp;preassure;count\n1257894000;test_name;test_tag;12.34;10.23;9;666",
			),
			expectedError: true,
		},
		{
			name: "Failure: Lesser value column size",
			body: []byte(
				"timestamp;name;tag;temp;preassure;count\n1257894000;test_name;test_tag;12.34;10.23",
			),
			expectedError: true,
		},
	}

	var log = logrus.New()
	failedTests := []string{}

testCaseLoop:
	for _, c := range cases {
		t.Logf("starting test case: %s", c.name)

		metrics, tags, timestamp, err := parseCSV(c.body, logrus.NewEntry(log))
		if err != nil {
			if !c.expectedError {
				t.Fatal(errors.Wrap(err, "unexpected error"))
				failedTests = append(failedTests, c.name)
			}

			continue
		}

		if len(c.expectedMetrics) != len(metrics) {
			t.Fatalf("expected no. of metrics %d, got %d", len(c.expectedMetrics), len(metrics))
			failedTests = append(failedTests, c.name)
			continue
		}

		for metricKey, metricVal := range metrics {
			expectedVal, ok := c.expectedMetrics[metricKey]
			if !ok {
				t.Fatalf("unexpected metric: %s", metricKey)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}

			if expectedVal != metricVal {
				t.Fatalf("unexpected value for key: %s, expected metric: %g, got: %g", metricKey, expectedVal, metricVal)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}
		}

		if len(c.expectedTags) != len(tags) {
			t.Fatalf("expected no. of tags %d, got %d", len(c.expectedTags), len(tags))
			failedTests = append(failedTests, c.name)
			continue
		}

		for tagKey, tagVal := range tags {
			expectedVal, ok := c.expectedTags[tagKey]
			if !ok {
				t.Fatalf("unexpected tag: %s", tagKey)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}

			if expectedVal != tagVal {
				t.Fatalf("unexpected value for key: %s, expected tag: %s, got: %s", tagKey, expectedVal, tagVal)
				failedTests = append(failedTests, c.name)
				continue testCaseLoop
			}
		}

		if c.expectedTimestamp != timestamp {
			t.Fatalf("expected timestamp: %d got: %d", c.expectedTimestamp.Unix(), timestamp.Unix())
			failedTests = append(failedTests, c.name)
		}
	}

	if len(failedTests) > 0 {
		t.Errorf("failure in test cases: %s", strings.Join(failedTests, ","))
	}
}
