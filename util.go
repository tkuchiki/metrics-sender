package main

import (
	"time"
)

func FixedTimezone(t time.Time, timezone string) (time.Time, error) {
	var err error
	var loc *time.Location
	zone, offset := time.Now().In(time.Local).Zone()

	if zone == "UTC" {
		return t, err
	}

	if timezone != "" {
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return t, err
		}

		return t.In(loc), err
	}

	loc = time.FixedZone(zone, offset)
	return t.In(loc), err
}
