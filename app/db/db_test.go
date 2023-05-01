package db

import (
	"taos-adapter/models"
	"testing"
)

func TestCompileTDEngineMetricsAndTags(t *testing.T) {

	cases := []struct {
		name            string
		metrics         models.TimeBasedMetrics
		expectedMetrics map[string]struct{} // set to map due to unordered metric and tag parsing
		expectedTags    map[string]struct{} // set to map due to unordered metric and tag parsing
		expectedFail    bool
	}{
		{
			name: "Success: All values",
			metrics: models.TimeBasedMetrics{
				Metrics: map[string]float64{
					"temp":      22.11,
					"preassure": 14.695,
					"count":     2.0,
				},
				Tags: map[string]string{
					"name":     "test_metric",
					"location": "test_location",
				},
			},
			expectedMetrics: map[string]struct{}{
				"temp=22.11":       struct{}{},
				"preassure=14.695": struct{}{},
				"count=2":          struct{}{},
			},
			expectedTags: map[string]struct{}{
				"name=test_metric":       struct{}{},
				"location=test_location": struct{}{},
			},
		},
		{
			name: "Success: empty metrics",
			metrics: models.TimeBasedMetrics{
				Tags: map[string]string{
					"name":     "test_metric",
					"location": "test_location",
				},
			},
			expectedMetrics: map[string]struct{}{
				"nullVal=0": struct{}{},
			},
			expectedTags: map[string]struct{}{
				"name=test_metric":       struct{}{},
				"location=test_location": struct{}{},
			},
		},
		{
			name: "Success empty tags",
			metrics: models.TimeBasedMetrics{
				Metrics: map[string]float64{
					"temp":      22.11,
					"preassure": 14.695,
					"count":     2.0,
				},
			},
			expectedMetrics: map[string]struct{}{
				"temp=22.11":       struct{}{},
				"preassure=14.695": struct{}{},
				"count=2":          struct{}{},
			},
			expectedTags: map[string]struct{}{
				"nullTag=null": struct{}{},
			},
		},
	}

	for _, c := range cases {
		t.Logf("starting test case: %s", c.name)
		tags, metrics := compileTDEngineMetricsAndTags(c.metrics)

		if len(c.expectedTags) != len(tags) {
			t.Fatalf("expected no. of tags %d, got %d", len(c.expectedTags), len(tags))
		}

		for _, tag := range tags {
			if _, ok := c.expectedTags[tag]; !ok {
				t.Fatalf("unexpected tag returned %s", tag)
			}
		}

		if len(c.expectedMetrics) != len(metrics) {
			t.Fatalf("expected no of metrics %d, got %d", len(c.expectedMetrics), len(metrics))
		}

		for _, metric := range metrics {
			if _, ok := c.expectedMetrics[metric]; !ok {
				t.Fatalf("unexpected metric returned %s", metric)
			}
		}
	}
}
