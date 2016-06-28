package main

type Output interface {
	Send([]Metric) error
}
