package main

import (
	"testing"
	"time"
)

type timestampTest struct {
	Stamp   string
	Time    time.Time
	Invalid bool
}

var timestampTests = []timestampTest{
	{
		Stamp: "123",
		Time:  time.Unix(123, 0),
	},
	{
		Stamp: "1.123456789",
		Time:  time.Unix(1, 123456789),
	},
	{
		Stamp: "1.012345678",
		Time:  time.Unix(1, 12345678),
	},
	{
		Stamp: "1.5",
		Time:  time.Unix(1, int64(time.Second/2)),
	},
	{
		Stamp: "1.05",
		Time:  time.Unix(1, int64(time.Second/20)),
	},
	{
		Stamp: "1.00",
		Time:  time.Unix(1, 0),
	},
	{
		Stamp:   "0",
		Invalid: true,
	},
	{
		Stamp:   ".0",
		Invalid: true,
	},
	{
		Stamp:   "1.",
		Invalid: true,
	},
	{
		Stamp:   "1.00.",
		Invalid: true,
	},
}

func TestParseUnixTimestamp(t *testing.T) {
	for _, x := range timestampTests {
		ts, ok := parseUnixTimestamp([]byte(x.Stamp))
		if !ok {
			if !x.Invalid {
				t.Errorf("ParseUnixTimestamp (%s): want: invalid timestamp got: %s", x.Stamp, ts)
			}
			continue
		}
		if !x.Time.Equal(ts) {
			t.Errorf("ParseUnixTimestamp (%s): want: %s got: %s", x.Stamp, x.Time, ts)
		}
	}
}
