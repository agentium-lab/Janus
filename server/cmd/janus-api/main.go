package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/bootstrap"
	"github.com/agentium-lab/Janus/server/internal/config"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgqueue "github.com/agentium-lab/Janus/server/internal/driver/pgqueue"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/gateway/a2a"
	"github.com/agentium-lab/Janus/server/internal/gateway/acp"
	"github.com/agentium-lab/Janus/server/internal/gateway/mcp"
	grpcserver "github.com/agentium-lab/Janus/server/internal/grpc"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/heartbeat"
	"github.com/agentium-lab/Janus/server/internal/lease"
	"github.com/agentium-lab/Janus/server/internal/llm"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/agentium-lab/Janus/server/internal/observability"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/agentium-lab/Janus/server/internal/expiry"
	"github.com/agentium-lab/Janus/server/internal/retry"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/agentium-lab/Janus/server/internal/service/intent"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

type intentAgentLookup struct {
	repo *pgdriver.AgentRepository
}

func (l *intentAgentLookup) ListOnlineAgents(ctx context.Context, tenantID string) ([]core.Agent, error) {
	ptrs, err := l.repo.ListOnlineWithCapabilities(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]core.Agent, len(ptrs))
	for i, a := range ptrs {
		out[i] = *a
	}
	return out, nil
}

type intentAdapter struct {
	r *intent.IntentResolver
}

func (a *intentAdapter) Resolve(ctx context.Context, tenantID, intentValue string, payload core.Payload, contextRefs []core.ContextRef, policyHints []string) (*service.IntentResolveResult, error) {
	result, err := a.r.Resolve(ctx, tenantID, intentValue, payload, contextRefs, policyHints)
	if err != nil {
		return nil, err
	}
	return &service.IntentResolveResult{
		ResolvedCapability: result.ResolvedCapability,
		Confidence:         result.Confidence,
		Reason:             result.Reason,
	}, nil
}

func main() {
	cfg := config.Load()

	if cfg.Migration.Auto {
		runMigration(cfg)
	}

	pool := mustOpenPool(cfg)
	defer pool.Close()

	var natsDrv *natsdriver.Driver
	if cfg.Queue.Driver == "nats" {
		var derr error
		natsDrv, derr = natsdriver.NewDriver(natsdriver.Config{URL: cfg.NATS.URL})
		if derr != nil {
			log.Fatalf("nats: %v", derr)
		}
		defer natsDrv.Close()
	}

	var queueDrv core.QueueEventDriver
	switch cfg.Queue.Driver {
	case "pg":
		queueDrv = pgqueue.NewDriver(pool)
		log.Println("queue driver: postgres (single-dependency mode; NATS disabled)")
	default:
		queueDrv = natsDrv
	}
	subscribeEvents := func(ctx context.Context, ch chan<- core.JanusEvent) (*nats.Subscription, error) {
		if q, ok := queueDrv.(*pgqueue.Driver); ok {
			if _, serr := q.SubscribeEvents(ctx, ch); serr != nil {
				return nil, serr
			}
			return nil, nil
		}
		return natsDrv.SubscribeEvents(ctx, ch)
	}

	redisDrv, rerr := redisdriver.NewDriver(redisdriver.Config{
		Addr:      cfg.Redis.Addr,
		Password:  cfg.Redis.Password,
		DB:        cfg.Redis.DB,
		EnableTLS: cfg.Redis.EnableTLS,
	})
	if rerr != nil {
		if cfg.Queue.Driver == "pg" {
			log.Printf("WARNING: redis unavailable (%v); pg-only mode continues without heartbeat/rate-limiter", rerr)
			redisDrv = nil // nil-safe: AgentService/Sweeper/RateLimiter guard below
		} else {
			log.Fatalf("redis: %v", rerr)
		}
	} else {
		defer redisDrv.Close()
	}

	tenantRepo := pgdriver.NewTenantRepository(pool)
	bootstrap.Run(context.Background(), bootstrap.Options{
		TenantLister: tenantRepo,
		QueueEnsurer: queueDrv,
	})

	agentRepo := pgdriver.NewAgentRepository(pool)
	taskRepo := pgdriver.NewTaskRepository(pool)
	mailboxRepo := pgdriver.NewMailboxRepository(pool)
	attemptRepo := pgdriver.NewTaskAttemptRepository(pool)
	budgetRepo := pgdriver.NewBudgetRepository(pool)
	budgetUsageRepo := pgdriver.NewBudgetUsageRepo(pool)
	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)
	eventRepo := pgdriver.NewEventRepo(pool)
	outboxRepo := pgdriver.NewOutboxRepo(pool)
	lookupRepo := pgdriver.NewAgentLookupRepo(pool)

	approvalRepo := pgdriver.NewApprovalRepo(pool)
	apiKeyRepo := pgdriver.NewAPIKeyRepo(pool)

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, queueDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetServiceWithUsage(budgetRepo, budgetUsageRepo).WithRateLimiter(redisDrv)

	var llmClient *llm.Client
	if cfg.LLM.Enabled && cfg.LLM.APIKey != "" {
		llmClient = llm.NewClient(cfg.LLM.BaseURL, cfg.LLM.APIKey, cfg.LLM.Model, cfg.LLM.MaxTokens, cfg.LLM.TimeoutSeconds)
		log.Println("LLM intent resolution enabled")
	} else if cfg.LLM.Enabled {
		log.Println("warning: LLM enabled but API key not set; keyword fallback only")
	}

	intentResolver := intent.NewResolver(&intentAgentLookup{repo: agentRepo})
	if llmClient != nil {
		intentResolver = intentResolver.WithLLM(llmClient)
	}

	contextRefSvc := service.NewContextRefService(pgdriver.NewContextRefRepo(pool))
	router := routing.NewRouter(lookupRepo, policyCheckerAdapter{svc: policySvc}, budgetCheckerAdapter{svc: budgetSvc})
	taskSvc := service.NewTaskService(taskRepo, queueDrv, pool, outboxRepo).WithPolicy(policySvc).WithRouter(router).WithIntentResolver(&intentAdapter{r: intentResolver}).WithAgentExistence(agentExistenceAdapter{agentRepo}).WithContextRefService(contextRefSvc).WithAttemptRepo(attemptRepo)
	mailboxSvc := service.NewMailboxService(mailboxRepo, queueDrv)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queueDrv, policySvc, budgetSvc)
	lifecycleSvc := service.NewLifecycleService(pool)
	taskSvc = taskSvc.WithLifecycle(lifecycleSvc)
	dispatchSvc = dispatchSvc.WithLifecycle(lifecycleSvc, outboxRepo, budgetUsageRepo)
	eventSvc := service.NewEventService(eventRepo)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queueDrv)
	approvalSvc.WithOutboxRepo(outboxRepo, pool)
	taskSvc.WithApproval(approvalSvc)

	tenantH := handler.NewTenantHandler(tenantSvc)
	agentH := handler.NewAgentHandler(agentSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	mailboxH := handler.NewMailboxHandler(mailboxSvc)
	dispatchH := handler.NewDispatchHandler(&dispatchAdapter{svc: dispatchSvc})
	auditH := handler.NewAuditHandler(&auditAdapter{svc: eventSvc})
	approvalH := handler.NewApprovalHandler(approvalSvc)
	apiKeyH := handler.NewAPIKeyHandler(service.NewAPIKeyService(apiKeyRepo))
	policyH := handler.NewPolicyRuleHandler(service.NewPolicyRuleService(policyRuleRepo))
	budgetH := handler.NewBudgetHandler(service.NewBudgetSpecService(budgetRepo))
	contextRefH := handler.NewContextRefHandler(contextRefSvc)
	acpGw := acp.NewGateway(agentSvc, taskSvc, taskSvc)
	mcpGw := mcp.NewGateway(taskSvc, taskSvc, contextRefSvc).WithEventPublisher(queueDrv)

	dlqSvc := handler.NewDLQServiceAdapter(taskRepo, queueDrv).WithOutbox(outboxRepo, pool)
	dlqH := handler.NewDLQHandler(dlqSvc)
	catalogH := handler.NewCatalogHandler(agentRepo)

	rawEventCh := make(chan core.JanusEvent, 256)
	eventSub, serr := subscribeEvents(context.Background(), rawEventCh)
	if serr != nil {
		log.Fatalf("subscribe events: %v", serr)
	}
	defer func() {
		// Drain the NATS subscription then close rawEventCh so the fan-out
		// goroutine below exits and closes its downstream channels. Without
		// this, the subscription + goroutine outlive HTTP shutdown.
		if eventSub != nil {
			_ = eventSub.Unsubscribe()
		}
		close(rawEventCh)
	}()

	broadcastCh := make(chan core.JanusEvent, 256)
	projectorCh := make(chan core.JanusEvent, 256)
	go func() {
		for evt := range rawEventCh {
			select {
			case broadcastCh <- evt:
			default:
			}
			select {
			case projectorCh <- evt:
			default:
			}
		}
		close(broadcastCh)
		close(projectorCh)
	}()

	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	wsH := handler.NewWebSocketHandler(broadcaster)
	sseH := handler.NewSSEHandler(broadcaster).WithStatusChecker(taskSvc)
	progressH := handler.NewProgressHandler(taskSvc, broadcaster) // FanoutBroadcaster implements EventPublisher via its inbound channel
	a2aGw := a2a.NewGatewayWithStatus(agentSvc, taskSvc, taskSvc).WithTaskStreamer(sseH).WithEventSubscriber(broadcaster)

	outboxPub := outbox.NewPublisher(outboxRepo, queueDrv)
	host, _ := os.Hostname()
	outboxRepo.SetWorker(fmt.Sprintf("%s-%d", host, os.Getpid()), 60*time.Second)
	go outboxPub.Start(context.Background(), 500*time.Millisecond)
	defer outboxPub.Stop()

	eventProjector := outbox.NewEventProjector(eventSvc)
	go func() {
		for evt := range projectorCh {
			eventProjector.Record(context.Background(), evt)
		}
	}()
	go eventProjector.Start(context.Background())
	defer eventProjector.Stop()

	retrySched := retry.NewScheduler(pool, queueDrv).WithOutbox()
	go retrySched.Start(context.Background(), 1*time.Second)
	defer retrySched.Stop()

	scannerInterval := 30 * time.Second
	if v := os.Getenv("JANUS_SCANNER_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			scannerInterval = d
		} else {
			log.Printf("invalid JANUS_SCANNER_INTERVAL %q, using 30s", v)
		}
	}

	hbSweeper := heartbeat.NewSweeper(redisDrv, agentRepo, scannerInterval)
	go hbSweeper.Start(context.Background())
	defer hbSweeper.Stop()

	expiryScanner := expiry.NewScanner(taskRepo, scannerInterval)
	go expiryScanner.Start(context.Background())
	defer expiryScanner.Stop()

	leaseScanner := lease.NewScanner(pool, scannerInterval)
	go leaseScanner.Start(context.Background())
	defer leaseScanner.Stop()

	pgDB, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("auth db open: %v", err)
	}
	defer pgDB.Close()
	validator := auth.NewAPIKeyValidator(pgDB)

	var sharedTLSCfg *tls.Config
	if cfg.TLS.Enabled && cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		var err error
		sharedTLSCfg, err = buildTLSConfig(cfg.TLS)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
	}
	grpcOpts := []grpcserver.Option{}
	if sharedTLSCfg != nil {
		grpcOpts = append(grpcOpts, grpcserver.WithTLS(sharedTLSCfg))
		log.Printf("gRPC TLS enabled (mTLS=%t)", cfg.TLS.ClientCAFile != "")
	} else {
		log.Println("WARNING: gRPC serving plaintext; set JANUS_TLS_ENABLED=true with cert/key to enable TLS")
	}
	grpcSrv := grpcserver.NewServer(cfg.GRPCPort, validator, agentSvc, taskSvc, dispatchSvc, eventSvc, mailboxSvc, dlqSvc, grpcOpts...)
	go func() {
		if err := grpcSrv.Start(); err != nil {
			log.Fatalf("grpc: %v", err)
		}
	}()
	defer grpcSrv.Stop()

	grpcAddr := fmt.Sprintf("localhost:%d", cfg.GRPCPort)
	gwMux, err := grpcserver.RegisterGateway(context.Background(), grpcAddr, sharedTLSCfg)
	if err != nil {
		log.Fatalf("grpc-gateway: %v", err)
	}

	mux := newRouter(tenantH, agentH, taskH, mailboxH, dispatchH, auditH, approvalH, contextRefH, wsH, sseH, progressH, a2aGw, acpGw, mcpGw, dlqH, catalogH, apiKeyH, policyH, budgetH)

	// Orchestration probes and the Prometheus scrape stay unauthenticated:
	// health checkers and scrapers cannot present API keys, and blocking them
	// makes every authenticated deployment fail its own readiness gate.
	public := http.NewServeMux()
	public.Handle("/metrics", promhttp.Handler())
	public.Handle("/.well-known/agent.json", a2a.AgentCardHandler())
	public.Handle("/.well-known/agent-card.json", a2a.AgentCardV1Handler())
	public.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	readyChecker := observability.NewReadyChecker()
	readyChecker.Add("postgres", func(ctx context.Context) error { return pool.Ping(ctx) })
	if natsDrv != nil {
		readyChecker.Add("nats", func(ctx context.Context) error {
			done := make(chan error, 1)
			go func() { done <- natsDrv.Conn().FlushTimeout(2 * time.Second) }()
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	if redisDrv != nil {
		readyChecker.Add("redis", redisDrv.Ready)
	}
	public.Handle("/readyz", readyChecker.Handler())

	protected := http.NewServeMux()
	protected.Handle("/", mux)
	protected.Handle("/grpc/", http.StripPrefix("/grpc", gwMux))

	addr := fmt.Sprintf("%s:%d", cfg.HTTPHost, cfg.HTTPPort)
	var core http.Handler = protected
	if cfg.Auth.Enabled {
		core = auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extractTenantFromPath)(core)))
		log.Println("api key authentication enabled")
	} else if !isLoopbackAddr(addr) {
		log.Fatalf("authentication disabled but binding non-loopback %s — refusing to start; set JANUS_AUTH_ENABLED=true, or bind a loopback address via JANUS_HTTP_HOST=localhost for local development", addr)
	} else {
		log.Println("WARNING: authentication disabled — dev mode (loopback only)")
	}
	public.Handle("/", core)

	handler := observability.CORSMiddleware(cfg.CORS.AllowedOrigins)(public)
	if cfg.TLS.Enabled && cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		handler = observability.HSTSMiddleware(handler)
	}

	log.Printf("janus-api listening HTTP=%s gRPC=%s", addr, grpcAddr)

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if sharedTLSCfg != nil {
			srv.TLSConfig = sharedTLSCfg
			log.Printf("janus-api starting with TLS (mTLS=%t)", cfg.TLS.ClientCAFile != "")
			if err := srv.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				log.Fatalf("https: %v", err)
			}
		} else {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("http: %v", err)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	log.Println("shutdown complete")
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

func isLoopbackAddr(addr string) bool {
	host := addr
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
	}
	switch host {
	case "localhost", "127.0.0.1", "[::1]", "::1":
		return true
	}
	return false
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

func newRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, contextRefH *handler.ContextRefHandler, wsH *handler.WebSocketHandler, sseH *handler.SSEHandler, progressH *handler.ProgressHandler, a2aGw http.Handler, acpGw http.Handler, mcpGw http.Handler, dlqH *handler.DLQHandler, catalogH *handler.CatalogHandler, apiKeyH *handler.APIKeyHandler, policyH *handler.PolicyRuleHandler, budgetH *handler.BudgetHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", wsH.ServeHTTP)
	mux.Handle("/a2a/", a2aGw)
	// ACP is deprecated in favor of A2A (protocol merged upstream). The shell
	// keeps existing consumers working while advertising removal via headers.
	mux.Handle("/acp/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Deprecation", "true")
		w.Header().Set("Sunset", "Thu, 31 Dec 2026 23:59:59 GMT")
		w.Header().Set("Link", "</a2a/>; rel=\"deprecation\"")
		acpGw.ServeHTTP(w, r)
	}))
	mux.Handle("/mcp", mcpGw)
	mux.Handle("/mcp/", mcpGw)

	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			tenantH.Create(w, r)
		case http.MethodGet:
			tenantH.List(w, r)
		default:
			http.NotFound(w, r)
		}
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
		case hasSegment(p, "tasks") && hasSuffix(p, "/progress"):
			postOnly(w, r, progressH.Report)
		case hasSegment(p, "tasks") && hasSuffix(p, "/stream"):
			getOnly(w, r, sseH.ServeHTTP)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/pause"):
			postOnly(w, r, mailboxH.Pause)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/resume"):
			postOnly(w, r, mailboxH.Resume)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/mailboxes"):
			postOnly(w, r, mailboxH.Create)
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
		case hasSuffix(p, "/catalog"):
			getOnly(w, r, catalogH.List)
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
		case hasSegment(p, "policy-rules") && hasSuffix(p, "/templates"):
			postOnly(w, r, policyH.CreateFromTemplate)
		case hasSegment(p, "policy-rules") && r.Method == http.MethodGet:
			getOnly(w, r, policyH.List)
		case hasSegment(p, "policy-rules"):
			postOnly(w, r, policyH.Create)
		case hasSegment(p, "budgets") && hasSuffix(p, "/budgets") && r.Method == http.MethodGet:
			getOnly(w, r, budgetH.List)
		case hasSegment(p, "budgets") && hasSuffix(p, "/budgets"):
			postOnly(w, r, budgetH.Upsert)
		case hasSegment(p, "budgets") && r.Method == http.MethodGet:
			getOnly(w, r, budgetH.Get)
		case hasSegment(p, "api-keys") && hasSuffix(p, "/revoke"):
			postOnly(w, r, apiKeyH.Revoke)
		case hasSegment(p, "api-keys") && r.Method == http.MethodGet:
			getOnly(w, r, apiKeyH.List)
		case hasSegment(p, "api-keys"):
			postOnly(w, r, apiKeyH.Create)
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
		case hasSegment(p, "context-refs") && hasSuffix(p, "/attach"):
			postOnly(w, r, contextRefH.Attach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/detach"):
			postOnly(w, r, contextRefH.Detach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/list"):
			getOnly(w, r, contextRefH.ListByTask)
		case hasSegment(p, "context-refs") && !hasLastSegment(p, "context-refs"):
			getOnly(w, r, contextRefH.Get)
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

type agentExistenceAdapter struct {
	repo *pgdriver.AgentRepository
}

func (a agentExistenceAdapter) AgentExists(ctx context.Context, tenantID, agentID string) (bool, error) {
	_, err := a.repo.Get(ctx, tenantID, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type policyCheckerAdapter struct {
	svc *service.PolicyService
}

func (a policyCheckerAdapter) CheckRoute(ctx context.Context, tenantID, agentID, dataClass string) (bool, error) {
	decision, err := a.svc.Evaluate(ctx, core.PolicyInput{
		TenantID: tenantID,
		Actor:    core.PolicyActor{Type: "agent", ID: agentID},
		Action:   "execute",
		Resource: core.PolicyResource{Type: "agent", Value: agentID},
		Context:  core.PolicyContextData{DataClassification: dataClass, TargetAgentID: agentID},
	})
	if err != nil {
		return false, err
	}
	return decision.Decision == core.PolicyDecisionAllow, nil
}

type budgetCheckerAdapter struct {
	svc *service.BudgetService
}

// CheckCapacity filters routing candidates at agent level only; tenant-level
// concurrency and rate limits stay enforced at PullTask where real counters
// are available.
func (b budgetCheckerAdapter) CheckCapacity(ctx context.Context, tenantID, agentID string, running int, _ int) (bool, error) {
	return b.svc.CheckConcurrency(ctx, tenantID, agentID, running, 0) == nil, nil
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
		LeaseID:   res.LeaseID,
		ExpiresAt: res.ExpiresAt,
	}, nil
}

func (a *dispatchAdapter) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.StartTask(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.TaskHeartbeat(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, leaseID, resultRef, usage)
}

func (a *dispatchAdapter) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, leaseID, retriable, taskErr)
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

func extractTenantFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// buildTLSConfig constructs a *tls.Config from the TLSConfig. When ClientCAFile
// is set, client certificates are required and verified (mTLS). MinVersion is
// TLS 1.2 and only strong AEAD cipher suites are enabled.
func buildTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}
	if tlsCfg.ClientCAFile != "" {
		caCert, err := os.ReadFile(tlsCfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse client CA certificate")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
