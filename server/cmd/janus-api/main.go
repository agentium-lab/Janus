package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/bootstrap"
	"github.com/agentium-lab/Janus/server/internal/config"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/gateway/a2a"
	"github.com/agentium-lab/Janus/server/internal/gateway/acp"
	"github.com/agentium-lab/Janus/server/internal/gateway/mcp"
	grpcserver "github.com/agentium-lab/Janus/server/internal/grpc"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/heartbeat"
	"github.com/agentium-lab/Janus/server/internal/lease"
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/agentium-lab/Janus/server/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/agentium-lab/Janus/server/internal/expiry"
	"github.com/agentium-lab/Janus/server/internal/service"
)

func main() {
	configureLogging("json")
	cfg := config.Load()
	configureLogging(cfg.Log.Format)

	traceShutdown, err := configureTracing(context.Background(), cfg.Observability.Tracing)
	if err != nil {
		log.Fatalf("tracing config: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceShutdown(ctx); err != nil {
			log.Printf("tracing shutdown: %v", err)
		}
	}()

	if cfg.Migration.Auto {
		runMigration(cfg)
	}

	pool := mustOpenPool(cfg)
	defer pool.Close()

	pgDB, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("auth db open: %v", err)
	}
	defer pgDB.Close()

	natsDrv, err := natsdriver.NewDriver(natsdriver.Config{URL: cfg.NATS.URL})
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer natsDrv.Close()

	redisDrv, err := redisdriver.NewDriver(redisdriver.Config{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		HeartbeatTTL: mustParseDuration("heartbeat.ttl", cfg.Heartbeat.TTL),
	})
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisDrv.Close()

	tenantRepo := pgdriver.NewTenantRepository(pool)
	agentRepo := pgdriver.NewAgentRepository(pool)
	taskRepo := pgdriver.NewTaskRepository(pool)
	mailboxRepo := pgdriver.NewMailboxRepository(pool)
	attemptRepo := pgdriver.NewTaskAttemptRepository(pool)
	budgetRepo := pgdriver.NewBudgetRepository(pool)
	budgetUsageRepo := pgdriver.NewBudgetUsageRepo(pool)
	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)
	eventRepo := pgdriver.NewEventRepo(pool)
	outboxRepo := pgdriver.NewOutboxRepo(pool).WithMaxRetries(cfg.Outbox.MaxRetries)

	approvalRepo := pgdriver.NewApprovalRepo(pool)

	queueResources, err := bootstrap.EnsureQueueResources(context.Background(), tenantRepo, mailboxRepo, natsDrv)
	if err != nil {
		log.Fatalf("ensure queue resources: %v", err)
	}
	if queueResources.Tenants > 0 || queueResources.DeferredMailboxes > 0 {
		log.Printf("ensured queue tenant resources tenants=%d deferred_mailboxes=%d",
			queueResources.Tenants, queueResources.DeferredMailboxes)
	}

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetServiceWithUsage(budgetRepo, budgetUsageRepo).WithRateLimiter(redisDrv)
	taskSvc := service.NewTaskService(taskRepo, natsDrv, pool, outboxRepo).
		WithPolicy(policySvc).
		WithBudget(budgetSvc).
		WithRouting(agentRepo, mailboxRepo).
		WithTargetRouting(service.TargetRoutingConfig{
			GroupMailboxes: cfg.Routing.GroupMailboxes,
			HumanMailboxes: cfg.Routing.HumanMailboxes,
		})
	mailboxSvc := service.NewMailboxService(mailboxRepo, natsDrv)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, natsDrv, policySvc, budgetSvc).
		WithOutbox(outboxRepo).
		WithAgentRepo(agentRepo)
	eventSvc := service.NewEventService(eventRepo)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, natsDrv).WithOutbox(outboxRepo)
	taskSvc.WithApproval(approvalSvc)
	contextRefSvc := service.NewContextRefService(pgdriver.NewContextRefRepo(pool))
	artifactSvc := service.NewArtifactService(newArtifactStore(cfg.Artifacts)).WithContextRefs(contextRefSvc)
	apiKeySvc := auth.NewAPIKeyManager(pgDB)
	var apiKeyValidator *auth.APIKeyValidator
	if cfg.Auth.Enabled {
		apiKeyValidator = auth.NewAPIKeyValidator(pgDB)
	}
	if cfg.Auth.BootstrapKey != "" {
		key, created, err := apiKeySvc.EnsureRawKey(context.Background(), cfg.Auth.BootstrapTenantID, cfg.Auth.BootstrapKeyName, cfg.Auth.BootstrapKey)
		if err != nil {
			log.Fatalf("bootstrap api key: %v", err)
		}
		log.Printf("bootstrap api key ensured tenant=%s key_id=%s prefix=%s created=%t", key.TenantID, key.ID, key.Prefix, created)
	}

	tenantH := handler.NewTenantHandler(tenantSvc)
	agentH := handler.NewAgentHandler(agentSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	mailboxH := handler.NewMailboxHandler(mailboxSvc)
	dispatchH := handler.NewDispatchHandler(&dispatchAdapter{svc: dispatchSvc})
	auditH := handler.NewAuditHandler(&auditAdapter{svc: eventSvc})
	approvalH := handler.NewApprovalHandler(approvalSvc)
	contextRefH := handler.NewContextRefHandler(contextRefSvc)
	artifactH := handler.NewArtifactHandler(artifactSvc)
	policyRuleH := handler.NewPolicyRuleHandler(policyRuleRepo)
	budgetH := handler.NewBudgetHandler(budgetRepo)
	securityAudit := &securityAuditAdapter{svc: eventSvc}
	apiKeyH := handler.NewAPIKeyHandler(&auditedAPIKeyService{svc: apiKeySvc, audit: securityAudit})
	a2aGw := a2a.NewGateway(agentSvc, taskSvc)
	acpGw := acp.NewGateway(agentSvc, taskSvc)
	mcpGw := mcp.NewGateway(taskSvc, contextRefSvc)

	dlqSvc := handler.NewDLQServiceAdapter(taskRepo, natsDrv).WithOutbox(outboxRepo)
	dlqH := handler.NewDLQHandler(dlqSvc)

	rawEventCh := make(chan core.JanusEvent, 256)
	_, err = natsDrv.SubscribeEvents(context.Background(), rawEventCh)
	if err != nil {
		log.Fatalf("subscribe events: %v", err)
	}

	broadcastCh := make(chan core.JanusEvent, 256)
	go func() {
		for evt := range rawEventCh {
			select {
			case broadcastCh <- evt:
			default:
			}
		}
		close(broadcastCh)
	}()

	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	wsH := handler.NewWebSocketHandler(broadcaster)

	if cfg.Outbox.Enabled {
		outboxPub := outbox.NewPublisher(outboxRepo, natsDrv).WithOptions(outbox.PublisherOptions{
			PollInterval:   mustParseDuration("outbox.poll_interval", cfg.Outbox.PollInterval),
			IdleBackoffMax: mustParseDuration("outbox.idle_backoff_max", cfg.Outbox.IdleBackoffMax),
			BatchSize:      cfg.Outbox.BatchSize,
			LeaseDuration:  mustParseDuration("outbox.lease_duration", cfg.Outbox.LeaseDuration),
			ListenNotify:   cfg.Outbox.ListenNotify,
		})
		go outboxPub.Start(context.Background(), 0)
		defer outboxPub.Stop()
	} else {
		log.Printf("outbox worker disabled by configuration")
	}

	eventProjector := outbox.NewEventProjector(eventSvc)
	projectionCtx, stopProjection := context.WithCancel(context.Background())
	startDurableEventProjection(projectionCtx, tenantRepo, natsDrv, eventProjector)
	defer stopProjection()
	defer eventProjector.Stop()

	hbSweeper := heartbeat.NewSweeper(redisDrv, agentRepo, mustParseDuration("heartbeat.sweeper_interval", cfg.Heartbeat.SweeperInterval))
	go hbSweeper.Start(context.Background())
	defer hbSweeper.Stop()

	expiryScanner := expiry.NewScanner(taskRepo, 30*time.Second)
	go expiryScanner.Start(context.Background())
	defer expiryScanner.Stop()

	leaseScanner := lease.NewScanner(dispatchSvc, 30*time.Second, 100)
	go leaseScanner.Start(context.Background())
	defer leaseScanner.Stop()

	if cfg.Observability.Metrics.Enabled {
		metricsCtx, stopMetrics := context.WithCancel(context.Background())
		defer stopMetrics()
		go metrics.NewCollector(outboxRepo, mailboxRepo, agentRepo).Start(metricsCtx)
	}

	serverTLSConfig, err := buildServerTLSConfig(cfg.TLS)
	if err != nil {
		log.Fatalf("tls config: %v", err)
	}
	gatewayTLSConfig, err := buildGatewayTLSConfig(cfg.TLS)
	if err != nil {
		log.Fatalf("gateway tls config: %v", err)
	}

	grpcSrv := grpcserver.NewServerWithTLSAndObservability(
		cfg.GRPCPort,
		agentSvc,
		taskSvc,
		dispatchSvc,
		mailboxSvc,
		dlqSvc,
		eventSvc,
		apiKeySvc,
		serverTLSConfig,
		grpcserver.ObservabilityOptions{
			TracingEnabled: cfg.Observability.Tracing.Enabled,
			ServiceName:    cfg.Observability.Tracing.ServiceName,
		},
		grpcserver.SecurityOptions{
			AuthEnabled:     cfg.Auth.Enabled,
			APIKeyValidator: apiKeyValidator,
		},
	)
	go func() {
		if err := grpcSrv.Start(); err != nil {
			log.Fatalf("grpc: %v", err)
		}
	}()
	defer grpcSrv.Stop()

	grpcAddr := fmt.Sprintf("localhost:%d", cfg.GRPCPort)
	gwMux, err := grpcserver.RegisterGatewayWithTLS(context.Background(), grpcAddr, gatewayTLSConfig)
	if err != nil {
		log.Fatalf("grpc-gateway: %v", err)
	}

	mux := newRouterWithGateways(tenantH, agentH, taskH, mailboxH, dispatchH, auditH, approvalH, contextRefH, artifactH, policyRuleH, budgetH, apiKeyH, wsH, a2aGw, acpGw, mcpGw, dlqH)

	combined := http.NewServeMux()
	if cfg.Observability.Metrics.Enabled {
		metricsPath := cfg.Observability.Metrics.Path
		if metricsPath == "" {
			metricsPath = "/metrics"
		}
		combined.Handle(metricsPath, promhttp.Handler())
	}
	combined.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	combined.Handle("/readyz", readinessHandler(map[string]dependencyCheck{
		"postgres": pool.Ping,
		"nats":     natsDrv.Health,
		"redis":    redisDrv.Health,
	}))
	combined.Handle("/", mux)
	combined.Handle("/grpc/", http.StripPrefix("/grpc", gwMux))

	var handler http.Handler = combined
	if cfg.Auth.Enabled {
		handler = auth.MiddlewareWithAudit(apiKeyValidator, extractTenantFromPath, securityAudit)(
			auth.TenantGuardWithAudit(extractTenantFromPath, securityAudit)(combined),
		)
		log.Println("api key authentication enabled")
	} else {
		log.Println("WARNING: authentication disabled (JANUS_AUTH_ENABLED=false)")
	}
	handler = observabilityMiddleware(cfg.Observability, handler)

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	log.Printf("janus-api listening HTTP=%s gRPC=%s", addr, grpcAddr)

	srv := &http.Server{Addr: addr, Handler: handler, TLSConfig: serverTLSConfig}
	go func() {
		var err error
		if cfg.TLS.Enabled {
			err = srv.ListenAndServeTLS("", "")
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	srv.Close()
}

func runMigration(cfg *config.Config) {
	migrationsPath, _ := filepath.Abs(cfg.Migration.Path)
	m, err := migrate.New("file://"+migrationsPath, cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("migrate init: %v", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate up: %v", err)
	}
	m.Close()
	log.Println("migration completed")
}

func mustOpenPool(cfg *config.Config) *pgxpool.Pool {
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("pgx pool config: %v", err)
	}
	poolConfig.MaxConns = int32(cfg.Postgres.MaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("pgx pool open: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("pgx pool ping: %v", err)
	}
	return pool
}

func newArtifactStore(cfg config.ArtifactsConfig) core.ArtifactStore {
	switch strings.ToLower(strings.TrimSpace(cfg.Store)) {
	case "", "local":
		return storage.NewLocalArtifactStore(cfg.LocalDir)
	default:
		log.Fatalf("unsupported artifact store %q", cfg.Store)
		return nil
	}
}

func newRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, contextRefH *handler.ContextRefHandler, apiKeyH *handler.APIKeyHandler, wsH *handler.WebSocketHandler, a2aGw http.Handler, dlqH *handler.DLQHandler) http.Handler {
	return newRouterWithGateways(tenantH, agentH, taskH, mailboxH, dispatchH, auditH, approvalH, contextRefH, nil, nil, nil, apiKeyH, wsH, a2aGw, http.NotFoundHandler(), http.NotFoundHandler(), dlqH)
}

func newRouterWithGateways(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, contextRefH *handler.ContextRefHandler, artifactH *handler.ArtifactHandler, policyRuleH *handler.PolicyRuleHandler, budgetH *handler.BudgetHandler, apiKeyH *handler.APIKeyHandler, wsH *handler.WebSocketHandler, a2aGw http.Handler, acpGw http.Handler, mcpGw http.Handler, dlqH *handler.DLQHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", wsH.ServeHTTP)
	mux.Handle("/a2a/", a2aGw)
	mux.Handle("/acp/", acpGw)
	mux.Handle("/mcp/", mcpGw)

	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			tenantH.Create(w, r)
			return
		}
		http.NotFound(w, r)
	})

	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path

		switch {
		case hasSegment(p, "dlq") && hasSuffix(p, "/replay"):
			postOnly(w, r, dlqH.Replay)
		case hasSegment(p, "dlq") && hasSuffix(p, "/discard"):
			postOnly(w, r, dlqH.Discard)
		case hasSegment(p, "dlq") && r.Method == http.MethodGet:
			dlqH.Query(w, r)
		case hasSegment(p, "pull"):
			postOnly(w, r, dispatchH.Pull)
		case hasSegment(p, "traces"):
			getOnly(w, r, auditH.QueryByTrace)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/mailboxes"):
			postOnly(w, r, mailboxH.Create)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/pause"):
			postOnly(w, r, mailboxH.Pause)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/resume"):
			postOnly(w, r, mailboxH.Resume)
		case hasSegment(p, "mailboxes") && r.Method == http.MethodPatch:
			mailboxH.Update(w, r)
		case hasSegment(p, "mailboxes"):
			getOnly(w, r, mailboxH.Get)
		case hasSegment(p, "heartbeat") && hasSegment(p, "agents"):
			postOnly(w, r, agentH.Heartbeat)
		case hasSegment(p, "agents") && !hasLastSegment(p, "agents"):
			getOnly(w, r, agentH.Get)
		case hasSegment(p, "agents") && hasLastSegment(p, "agents"):
			if r.Method == http.MethodPost {
				agentH.Register(w, r)
			} else {
				agentH.List(w, r)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/start"):
			postOnly(w, r, dispatchH.Start)
		case hasSegment(p, "tasks") && hasSuffix(p, "/heartbeat"):
			postOnly(w, r, dispatchH.Heartbeat)
		case hasSegment(p, "tasks") && hasSuffix(p, "/ack"):
			postOnly(w, r, dispatchH.Ack)
		case hasSegment(p, "tasks") && hasSuffix(p, "/nack"):
			postOnly(w, r, dispatchH.Nack)
		case hasSegment(p, "tasks") && hasSuffix(p, "/cancel"):
			postOnly(w, r, taskH.Cancel)
		case hasSegment(p, "tasks") && hasSuffix(p, "/block"):
			postOnly(w, r, taskH.Block)
		case hasSegment(p, "tasks") && hasSuffix(p, "/unblock"):
			postOnly(w, r, taskH.Unblock)
		case hasSegment(p, "approvals") && hasSuffix(p, "/approve"):
			postOnly(w, r, approvalH.Approve)
		case hasSegment(p, "approvals") && hasSuffix(p, "/reject"):
			postOnly(w, r, approvalH.Reject)
		case hasSegment(p, "approvals") && !hasLastSegment(p, "approvals"):
			getOnly(w, r, approvalH.Get)
		case hasSegment(p, "approvals") && hasLastSegment(p, "approvals"):
			if r.Method == http.MethodPost {
				postOnly(w, r, approvalH.Request)
			} else {
				getOnly(w, r, approvalH.ListPending)
			}
		case hasSegment(p, "context-refs") && hasSuffix(p, "/attach"):
			postOnly(w, r, contextRefH.Attach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/detach"):
			postOnly(w, r, contextRefH.Detach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/list"):
			getOnly(w, r, contextRefH.ListByTask)
		case hasSegment(p, "context-refs") && !hasLastSegment(p, "context-refs"):
			getOnly(w, r, contextRefH.Get)
		case hasSegment(p, "tasks") && hasSuffix(p, "/replay"):
			postOnly(w, r, taskH.Replay)
		case hasSegment(p, "tasks") && hasSuffix(p, "/complete"):
			postOnly(w, r, taskH.Complete)
		case hasSegment(p, "tasks") && hasSuffix(p, "/fail"):
			postOnly(w, r, taskH.Fail)
		case hasSegment(p, "tasks") && hasSuffix(p, "/events"):
			getOnly(w, r, auditH.QueryByTask)
		case hasSegment(p, "tasks") && !hasLastSegment(p, "tasks"):
			getOnly(w, r, taskH.Get)
		case hasSegment(p, "tasks") && hasLastSegment(p, "tasks"):
			postOnly(w, r, taskH.Create)
		case hasSegment(p, "events"):
			getOnly(w, r, auditH.QueryByTenant)
		case hasSegment(p, "artifacts"):
			if artifactH == nil {
				http.NotFound(w, r)
			} else if r.Method == http.MethodPost {
				artifactH.Put(w, r)
			} else {
				getOnly(w, r, artifactH.Get)
			}
		case hasSegment(p, "policy-rules") && hasSuffix(p, "/templates"):
			if policyRuleH == nil {
				http.NotFound(w, r)
			} else {
				postOnly(w, r, policyRuleH.CreateFromTemplate)
			}
		case hasSegment(p, "policy-rules") && hasLastSegment(p, "policy-rules"):
			if policyRuleH == nil {
				http.NotFound(w, r)
			} else if r.Method == http.MethodPost {
				policyRuleH.Create(w, r)
			} else {
				getOnly(w, r, policyRuleH.List)
			}
		case hasSegment(p, "budgets") && hasLastSegment(p, "budgets"):
			if budgetH == nil {
				http.NotFound(w, r)
			} else if r.Method == http.MethodPost {
				budgetH.Upsert(w, r)
			} else {
				getOnly(w, r, budgetH.List)
			}
		case hasSegment(p, "budgets"):
			if budgetH == nil {
				http.NotFound(w, r)
			} else {
				getOnly(w, r, budgetH.Get)
			}
		case hasSegment(p, "api-keys") && hasSuffix(p, "/revoke"):
			postOnly(w, r, apiKeyH.Revoke)
		case hasSegment(p, "api-keys") && hasLastSegment(p, "api-keys"):
			if r.Method == http.MethodPost {
				apiKeyH.Create(w, r)
			} else {
				getOnly(w, r, apiKeyH.List)
			}
		default:
			if r.Method == http.MethodGet {
				tenantH.Get(w, r)
			} else {
				http.NotFound(w, r)
			}
		}
	})

	return mux
}

func postOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodPost {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func getOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodGet {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func hasSegment(path, seg string) bool {
	for _, s := range strings.Split(path, "/") {
		if s == seg {
			return true
		}
	}
	return false
}

func hasLastSegment(path, seg string) bool {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	return len(parts) > 0 && parts[len(parts)-1] == seg
}

func hasSuffix(path, suffix string) bool {
	return strings.HasSuffix(strings.TrimRight(path, "/"), suffix)
}

func mustParseDuration(name, value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		log.Fatalf("invalid %s %q", name, value)
	}
	return d
}

type dispatchAdapter struct {
	svc *service.DispatchService
}

func (a *dispatchAdapter) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*handler.ServicePullResult, error) {
	res, err := a.svc.PullTask(ctx, tenantID, mailboxID, agentID)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return &handler.ServicePullResult{
		Task:      res.Task,
		Attempt:   res.Attempt,
		LeaseID:   res.LeaseID,
		ExpiresAt: res.ExpiresAt,
	}, nil
}

func (a *dispatchAdapter) StartTask(ctx context.Context, tenantID, taskID string, attempt int, leaseID string) error {
	return a.svc.StartTask(ctx, tenantID, taskID, attempt, leaseID)
}

func (a *dispatchAdapter) TaskHeartbeat(ctx context.Context, tenantID, taskID string, attempt int, leaseID string) error {
	return a.svc.TaskHeartbeat(ctx, tenantID, taskID, attempt, leaseID)
}

func (a *dispatchAdapter) AckTask(ctx context.Context, tenantID, taskID string, attempt int, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, attempt, leaseID, resultRef, usage)
}

func (a *dispatchAdapter) NackTask(ctx context.Context, tenantID, taskID string, attempt int, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, attempt, leaseID, retriable, taskErr)
}

type auditAdapter struct {
	svc *service.EventService
}

func (a *auditAdapter) QueryByTask(ctx context.Context, tenantID, taskID string, limit int) (interface{}, error) {
	return a.svc.QueryByTask(ctx, tenantID, taskID, limit)
}

func (a *auditAdapter) QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) (interface{}, error) {
	return a.svc.QueryByTrace(ctx, tenantID, traceID, limit)
}

func (a *auditAdapter) QueryByTenant(ctx context.Context, tenantID string, limit int) (interface{}, error) {
	return a.svc.QueryByTenant(ctx, tenantID, limit)
}

type securityAuditAdapter struct {
	svc *service.EventService
}

func (a *securityAuditAdapter) RecordSecurityEvent(ctx context.Context, event auth.SecurityAuditEvent) {
	if a == nil || a.svc == nil || event.TenantID == "" || event.EventType == "" {
		return
	}
	payload := map[string]string{}
	for key, value := range event.Payload {
		payload[key] = value
	}
	if event.ActorType != "" {
		payload["actor_type"] = event.ActorType
	}
	if event.ActorID != "" {
		payload["actor_id"] = event.ActorID
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		payloadBytes = []byte(`{}`)
	}
	_ = a.svc.Record(ctx, core.JanusEvent{
		EventType:   core.EventType(event.EventType),
		TenantID:    event.TenantID,
		TraceID:     event.TraceID,
		SourceAgent: event.ActorID,
		ActorType:   event.ActorType,
		ActorID:     event.ActorID,
		Payload:     payloadBytes,
	})
}

type auditedAPIKeyService struct {
	svc   *auth.APIKeyManager
	audit auth.SecurityAuditRecorder
}

func (s *auditedAPIKeyService) Create(ctx context.Context, tenantID, name string) (*auth.CreatedAPIKey, error) {
	key, err := s.svc.Create(ctx, tenantID, name)
	if err == nil && s.audit != nil {
		s.audit.RecordSecurityEvent(ctx, auth.SecurityAuditEvent{
			TenantID:  tenantID,
			EventType: string(core.EventSecurityAPIKeyCreated),
			TraceID:   traceIDFromContext(ctx),
			ActorType: "api_key",
			ActorID:   auth.APIKeyPrefixFromContext(ctx),
			Payload: map[string]string{
				"key_id":     key.ID,
				"key_name":   key.Name,
				"key_prefix": key.Prefix + "...",
			},
		})
	}
	return key, err
}

func (s *auditedAPIKeyService) List(ctx context.Context, tenantID string) ([]auth.APIKey, error) {
	return s.svc.List(ctx, tenantID)
}

func (s *auditedAPIKeyService) Revoke(ctx context.Context, tenantID, keyID string) (*auth.APIKey, error) {
	key, err := s.svc.Revoke(ctx, tenantID, keyID)
	if err == nil && s.audit != nil {
		s.audit.RecordSecurityEvent(ctx, auth.SecurityAuditEvent{
			TenantID:  tenantID,
			EventType: string(core.EventSecurityAPIKeyRevoked),
			TraceID:   traceIDFromContext(ctx),
			ActorType: "api_key",
			ActorID:   auth.APIKeyPrefixFromContext(ctx),
			Payload: map[string]string{
				"key_id":     key.ID,
				"key_name":   key.Name,
				"key_prefix": key.Prefix + "...",
			},
		})
	}
	return key, err
}

func traceIDFromContext(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.TraceID().IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func extractTenantFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
