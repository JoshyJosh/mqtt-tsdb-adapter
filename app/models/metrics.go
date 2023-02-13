package models

import "time"

type TimeBasedMetrics struct {
	Metrics   map[string]float64 // ints also fall here, however they're formatted with %g
	Tags      map[string]string
	Timestamp time.Time
	DB        string
	Table     string
}
