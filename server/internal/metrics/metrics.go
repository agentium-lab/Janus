package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TasksCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_tasks_created_total",
	}, []string{"tenant_id"})

	TasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_tasks_completed_total",
	}, []string{"tenant_id"})

	TasksFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_tasks_failed_total",
	}, []string{"tenant_id"})

	TasksDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_tasks_dead_lettered_total",
	}, []string{"tenant_id"})

	PullRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_pull_requests_total",
	}, []string{"tenant_id", "mailbox_id"})

	AckTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_ack_total",
	}, []string{"tenant_id"})

	NackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_nack_total",
	}, []string{"tenant_id"})

	BudgetThrottle = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_budget_throttle_total",
	}, []string{"tenant_id", "reason"})

	PolicyDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_policy_denied_total",
	}, []string{"tenant_id"})

	TaskLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "janus_task_latency_seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"tenant_id"})

	AgentOnline = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "janus_agent_online",
	}, []string{"tenant_id"})

	QueueBacklog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "janus_queue_backlog",
	}, []string{"tenant_id", "mailbox_id"})

	AgentHeartbeatLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "janus_agent_heartbeat_lag_seconds",
	}, []string{"tenant_id", "agent_id"})

	TasksExpired = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_tasks_expired_total",
	}, []string{"tenant_id"})

	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "janus_outbox_pending",
		Help: "Number of outbox entries observed pending in the last publisher batch",
	})
	OutboxPublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "janus_outbox_publish_total",
		Help: "Total number of outbox entries the publisher attempted to publish",
	})
	OutboxPublishFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "janus_outbox_publish_failed_total",
		Help: "Total number of outbox entries that failed to publish",
	})

	RoutingDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "janus_routing_decisions_total",
		Help: "Total routing decisions partitioned by outcome",
	}, []string{"outcome"})

	RetryAttempted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "janus_retry_attempted_total",
		Help: "Total number of retry attempts promoted to queued by the retry scheduler",
	})
)
