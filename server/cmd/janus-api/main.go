package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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
	// A nil *redisdriver.Driver stored in an interface is NOT nil (Go typed
	// nil); downstream `if driver == nil` guards would never fire. Inject
	// explicit nil interfaces so PG-only mode truly runs without Redis.
	var agentHb service.HeartbeatDriver
	var budgetRL service.RateLimiter
	if redisDrv != nil {
		agentHb = redisDrv
		budgetRL = redisDrv
	}
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, agentHb, queueDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetServiceWithUsage(budgetRepo, budgetUsageRepo).WithRateLimiter(budgetRL)

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
	pgLifecycle := service.NewPGLifecycle(pool)
	taskSvc = taskSvc.WithLifecycle(pgLifecycle)
	dispatchSvc = dispatchSvc.WithTxPath(pgLifecycle, outboxRepo, service.PGBudgetLedger(budgetUsageRepo))
	eventSvc := service.NewEventService(eventRepo)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queueDrv)
	approvalSvc.WithTxPath(pgLifecycle, outboxRepo)
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
	mcpGw := mcp.NewGateway(taskSvc, taskSvc, contextRefSvc).WithEventPublisher(outbox.NewOutboxEventRecorder(outboxRepo))

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
	// Event routing with delivery classes (ninth review: no silent loss):
	//   terminal events    -> blocking hand-off to broadcast (bounded wait);
	//                          a lost terminal hangs SSE clients
	//   audit projection   -> blocking hand-off (backpressure to NATS);
	//                          audit records must not be dropped
	//   non-terminal (progress etc.) -> droppable under backpressure
	go func() {
		for evt := range rawEventCh {
			terminal := isTerminalJanusEvent(evt)
			if terminal {
				select {
				case broadcastCh <- evt:
				case <-time.After(5 * time.Second):
					log.Printf("event router: broadcast hand-off timed out for terminal event %s %s", evt.EventID, evt.EventType)
				}
			} else {
				select {
				case broadcastCh <- evt:
				default:
				}
			}
		}
		close(broadcastCh)
	}()

	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	wsH := handler.NewWebSocketHandler(broadcaster)
	sseH := handler.NewSSEHandler(broadcaster).WithStatusChecker(taskSvc)
	progressH := handler.NewProgressHandler(taskSvc, broadcaster) // FanoutBroadcaster implements EventPublisher via its inbound channel
	a2aGw := a2a.NewGatewayWithStatus(agentSvc, taskSvc, taskSvc).WithTaskStreamer(sseH).
		WithEventSubscriber(broadcaster).WithTaskLister(pgTaskLister{repo: taskRepo})

	outboxInterval, err := time.ParseDuration(cfg.Outbox.WorkerInterval)
	if err != nil || outboxInterval <= 0 {
		outboxInterval = 500 * time.Millisecond
	}
	leaseDuration, err := time.ParseDuration(cfg.Outbox.LeaseDuration)
	if err != nil || leaseDuration <= 0 {
		leaseDuration = 60 * time.Second
	}
	outboxPub := outbox.NewPublisher(outboxRepo, queueDrv).WithBatchSize(cfg.Outbox.BatchSize)
	host, _ := os.Hostname()
	outboxRepo.SetWorker(fmt.Sprintf("%s-%d", host, os.Getpid()), leaseDuration)
	outboxRepo.SetMaxRetries(cfg.Outbox.MaxAttempts)
	go outboxPub.Start(context.Background(), outboxInterval)
	defer outboxPub.Stop()

	// ADR-0006: audit projection reads from the outbox table (persistent
	// source) — crashes resume without loss; the memory channel path is gone.
	auditProjector := outbox.NewAuditProjector(outboxRepo, eventSvc)
	auditH.WithReplayer(auditProjector).WithOutboxRetryReviver(outboxRepo)
	go auditProjector.WithInterval(outboxInterval).WithBatchSize(cfg.Outbox.BatchSize).Start(context.Background())
	defer auditProjector.Stop()

	// Dead/quarantined entries are NOT auto-revived: a poison message would
	// retry forever at a fixed cadence. Recovery is manual via
	// POST /v1/tenants/{tenant}/outbox/retry-dead (e.g. after a rolling upgrade adds the
	// handler for a quarantined kind); visibility via janus_outbox_dead_total
	// and janus_outbox_quarantined_total.

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

	var hbScan heartbeat.HeartbeatScanner
	if redisDrv != nil {
		hbScan = redisDrv
	}
	hbSweeper := heartbeat.NewSweeper(hbScan, agentRepo, scannerInterval)
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
		guarded := auth.AgentIdentityMiddleware(core)
		core = auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extractTenantFromPath)(guarded)))
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
