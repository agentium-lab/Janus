package auth

// grpcMethodScopes is the gRPC projection of the HTTP scopeRules in
// apikey.go: each entry names the scope the method's HTTP counterpart
// requires, so a principal gets the SAME verdict on both protocols. The
// cross-protocol parity tests assert this lockstep; a new unary method that
// is missing here fails TestGRPCMethodScopes_CoversEveryGeneratedMethod
// until it is mapped.
var grpcMethodScopes = map[string]string{
	// AgentService. HTTP: /v1/tenants/{t}/agents... (GET reads, POST/PATCH writes).
	"/janus.v1.AgentService/GetAgent":      ScopeTaskRead,
	"/janus.v1.AgentService/Heartbeat":     ScopeTaskWrite,
	"/janus.v1.AgentService/ListAgents":    ScopeTaskRead,
	"/janus.v1.AgentService/RegisterAgent": ScopeTaskWrite,
	"/janus.v1.AgentService/UpdateAgent":   ScopeTaskWrite,

	// AuditService. HTTP: traces paths are audit:read; task/tenant event
	// listings stay task:read (GET defaults).
	"/janus.v1.AuditService/GetTrace":       ScopeAuditRead,
	"/janus.v1.AuditService/ListEvents":     ScopeTaskRead,
	"/janus.v1.AuditService/ListTaskEvents": ScopeTaskRead,

	// DispatchService. HTTP: ack/nack/pull/start/heartbeat are POST writes.
	"/janus.v1.DispatchService/AckTask":       ScopeTaskWrite,
	"/janus.v1.DispatchService/NackTask":      ScopeTaskWrite,
	"/janus.v1.DispatchService/PullTask":      ScopeTaskWrite,
	"/janus.v1.DispatchService/StartTask":     ScopeTaskWrite,
	"/janus.v1.DispatchService/TaskHeartbeat": ScopeTaskWrite,

	// DLQService. HTTP: dlq /replay|/discard are admin (scopeRules);
	// the DLQ query is a GET read.
	"/janus.v1.DLQService/DiscardDLQ": ScopeAdmin,
	"/janus.v1.DLQService/QueryDLQ":   ScopeTaskRead,
	"/janus.v1.DLQService/ReplayDLQ":  ScopeAdmin,

	// MailboxService. HTTP: mailbox create/pause/resume/update are writes,
	// mailbox get is a read.
	"/janus.v1.MailboxService/CreateMailbox": ScopeTaskWrite,
	"/janus.v1.MailboxService/GetMailbox":    ScopeTaskRead,
	"/janus.v1.MailboxService/PauseMailbox":  ScopeTaskWrite,
	"/janus.v1.MailboxService/ResumeMailbox": ScopeTaskWrite,
	"/janus.v1.MailboxService/UpdateMailbox": ScopeTaskWrite,

	// TaskService. HTTP: tasks POST writes; note task /replay is a plain
	// task:write (only dlq and audit /replay are admin).
	"/janus.v1.TaskService/CancelTask": ScopeTaskWrite,
	"/janus.v1.TaskService/CreateTask": ScopeTaskWrite,
	"/janus.v1.TaskService/GetTask":    ScopeTaskRead,
	"/janus.v1.TaskService/ReplayTask": ScopeTaskWrite,
}

// RequiredScopeGRPC returns the scope a gRPC full method
// ("/janus.v1.Service/Method") requires, mirroring RequiredScope for the
// HTTP surface. Unknown methods fail closed to task:write: the strictest
// data-plane scope that still lets operators work, so an unmapped future
// mutation can never be exercised by a read-only key. (In practice the
// completeness test keeps the table exhaustive.)
func RequiredScopeGRPC(fullMethod string) string {
	if scope, ok := grpcMethodScopes[fullMethod]; ok {
		return scope
	}
	return ScopeTaskWrite
}
