package nats

import "testing"

// NATS 2.15 changed MaxConsumers semantics: -1 and unset now mean the server
// default — a 1000-consumer ceiling — instead of unlimited. Every mailbox
// owns a durable consumer on the tenant TASKS stream, so a 1000-consumer
// default silently caps tenants at 1000 mailboxes (GA-11 runner incident:
// mailbox #1001+ failed with "maximum consumers limit reached", surfaced as
// a sanitized 400). Every stream config must carry an explicit positive
// ceiling above the 2.15 default.
func TestStreamConfigsExplicitMaxConsumersAboveDefault(t *testing.T) {
	configs := map[string]struct {
		got      int
		minExtra int
	}{
		"TASKS": {taskStreamConfig("tenant-a").MaxConsumers, 1000},
		"EVENTS": {eventStreamConfig("tenant-a").MaxConsumers, 1000},
		"RETRY":  {retryStreamConfig("tenant-a").MaxConsumers, 1000},
		"DLQ":    {dlqStreamConfig("tenant-a", "mb-1").MaxConsumers, 1000},
	}
	for name, c := range configs {
		if c.got <= c.minExtra {
			t.Errorf("%s stream MaxConsumers = %d, must be an explicit positive value above the NATS 2.15 default ceiling (%d); -1/unset now means \"server default\", not unlimited", name, c.got, c.minExtra)
		}
	}
}
