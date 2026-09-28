package nats

import "testing"

func TestRunawayBackstopDeliver(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, 100},   // unset -> default business budget, but backstop >= 100
		{-3, 100},  // invalid -> same
		{5, 100},   // typical mailbox budget: PG authority, backstop 100
		{99, 100},  // just under the backstop floor
		{100, 100}, // at the floor
		{500, 500}, // explicit larger backstop is honored
	}
	for _, c := range cases {
		if got := runawayBackstopDeliver(c.in); got != c.want {
			t.Errorf("runawayBackstopDeliver(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
