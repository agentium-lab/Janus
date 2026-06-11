package main

import (
	"context"
	"database/sql"
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
	"github.com/agentium-lab/Janus/server/internal/config"
	grpcserver "github.com/agentium-lab/Janus/server/internal/grpc"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/gateway/a2a"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/heartbeat"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/agentium-lab/Janus/server/internal/expiry"
	"github.com/agentium-lab/Janus/server/internal/retry"
	"github.com/agentium-lab/Janus/server/internal/service"
)

func main() {
	cfg := config.Load()

	if cfg.Migration.Auto {
		runMigration(cfg)
	}

	pool := mustOpenPool(cfg)
	defer pool.Close()

	natsDrv, err := natsdriver.NewDriver(natsdriver.Config{URL: cfg.NATS.URL})
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer natsDrv.Close()

	redisDrv, err := redisdriver.NewDriver(redisdriver.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
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
	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)
	eventRepo := pgdriver.NewEventRepo(pool)
	outboxRepo := pgdriver.NewOutboxRepo(pool)

	approvalRepo := pgdriver.NewApprovalRepo(pool)

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetService(budgetRepo).WithRateLimiter(redisDrv)
	taskSvc := service.NewTaskService(taskRepo, natsDrv, pool, outboxRepo).WithPolicy(policySvc)
	mailboxSvc := service.NewMailboxService(mailboxRepo, natsDrv)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, natsDrv, policySvc, budgetSvc)
	eventSvc := service.NewEventService(eventRepo)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc)

	tenantH := handler.NewTenantHandler(tenantSvc)
	agentH := handler.NewAgentHandler(agentSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	mailboxH := handler.NewMailboxHandler(mailboxSvc)
	dispatchH := handler.NewDispatchHandler(&dispatchAdapter{svc: dispatchSvc})
	auditH := handler.NewAuditHandler(&auditAdapter{svc: eventSvc})
	approvalH := handler.NewApprovalHandler(approvalSvc)
	a2aGw := a2a.NewGateway(agentSvc, taskSvc)

	dlqSvc := handler.NewDLQServiceAdapter(taskRepo, natsDrv)
	dlqH := handler.NewDLQHandler(dlqSvc)

	eventCh := make(chan core.JanusEvent, 256)
	_, err = natsDrv.SubscribeEvents(context.Background(), eventCh)
	if err != nil {
		log.Fatalf("subscribe events: %v", err)
	}
	broadcaster := handler.NewFanoutBroadcaster(eventCh)
	wsH := handler.NewWebSocketHandler(broadcaster)

	outboxPub := outbox.NewPublisher(outboxRepo, natsDrv)
	go outboxPub.Start(context.Background(), 500*time.Millisecond)
	defer outboxPub.Stop()

	retrySched := retry.NewScheduler(pool)
	go retrySched.Start(context.Background(), 1*time.Second)
	defer retrySched.Stop()

	hbSweeper := heartbeat.NewSweeper(redisDrv, agentRepo, 30*time.Second)
	go hbSweeper.Start(context.Background())
	defer hbSweeper.Stop()

	expiryScanner := expiry.NewScanner(taskRepo, 30*time.Second)
	go expiryScanner.Start(context.Background())
	defer expiryScanner.Stop()

	grpcSrv := grpcserver.NewServer(cfg.GRPCPort, agentSvc, taskSvc, dispatchSvc, eventSvc)
	go func() {
		if err := grpcSrv.Start(); err != nil {
			log.Fatalf("grpc: %v", err)
		}
	}()
	defer grpcSrv.Stop()

	grpcAddr := fmt.Sprintf("localhost:%d", cfg.GRPCPort)
	gwMux, err := grpcserver.RegisterGateway(context.Background(), grpcAddr)
	if err != nil {
		log.Fatalf("grpc-gateway: %v", err)
	}

	mux := newRouter(tenantH, agentH, taskH, mailboxH, dispatchH, auditH, approvalH, wsH, a2aGw, dlqH)

	combined := http.NewServeMux()
	combined.Handle("/metrics", promhttp.Handler())
	combined.Handle("/v1/", gwMux)
	combined.Handle("/", mux)

	var handler http.Handler = combined
	if cfg.Auth.Enabled {
		pgDB, err := sql.Open("pgx", cfg.Postgres.DSN())
		if err != nil {
			log.Fatalf("auth db open: %v", err)
		}
		defer pgDB.Close()
		validator := auth.NewAPIKeyValidator(pgDB)
		handler = auth.Middleware(validator)(mux)
		log.Println("api key authentication enabled")
	} else {
		log.Println("WARNING: authentication disabled (JANUS_AUTH_ENABLED=false)")
	}

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	log.Printf("janus-api listening on %s", addr)

	srv := &http.Server{Addr: addr, Handler: handler}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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

func newRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, wsH *handler.WebSocketHandler, a2aGw http.Handler, dlqH *handler.DLQHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", wsH.ServeHTTP)
	mux.Handle("/a2a/", a2aGw)

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
	return []struct{}{}, nil
}
