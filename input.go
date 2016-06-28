package main

type Input interface {
	FetchMetrics() ([]Metric, error)
	Teardown()
}
