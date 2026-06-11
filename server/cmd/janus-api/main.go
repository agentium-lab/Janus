package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"

	"github.com/agentium-lab/Janus/server/internal/config"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
)

func main() {
	cfg := config.Load()

	if cfg.Migration.Auto {
		runMigration(cfg)
	}

	db := mustOpenDB(cfg)
	defer db.Close()

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

	tenantRepo := pgdriver.NewTenantRepository(db)
	agentRepo := pgdriver.NewAgentRepository(db)
	taskRepo := pgdriver.NewTaskRepository(db)
	mailboxRepo := pgdriver.NewMailboxRepository(db)

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	taskSvc := service.NewTaskService(taskRepo, natsDrv)
	mailboxSvc := service.NewMailboxService(mailboxRepo, natsDrv)

	tenantH := handler.NewTenantHandler(tenantSvc)
	agentH := handler.NewAgentHandler(agentSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	mailboxH := handler.NewMailboxHandler(mailboxSvc)

	mux := newRouter(tenantH, agentH, taskH, mailboxH)

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	log.Printf("janus-api listening on %s", addr)

	srv := &http.Server{Addr: addr, Handler: mux}

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

func mustOpenDB(cfg *config.Config) *sql.DB {
	db, err := sql.Open("postgres", cfg.Postgres.ConnStr())
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	db.SetMaxOpenConns(cfg.Postgres.MaxConns)
	if err := db.Ping(); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	return db
}

func newRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler) http.Handler {
	mux := http.NewServeMux()

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
		case hasSegment(p, "mailboxes"):
			dispatchMailbox(w, r, mailboxH)
		case hasSegment(p, "agents") && hasSegment(p, "heartbeat"):
			if r.Method == http.MethodPost {
				agentH.Heartbeat(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "agents") && !hasLastSegment(p, "agents"):
			if r.Method == http.MethodGet {
				agentH.Get(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "agents") && hasLastSegment(p, "agents"):
			if r.Method == http.MethodPost {
				agentH.Register(w, r)
			} else {
				agentH.List(w, r)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/start"):
			if r.Method == http.MethodPost {
				taskH.Start(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/complete"):
			if r.Method == http.MethodPost {
				taskH.Complete(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/fail"):
			if r.Method == http.MethodPost {
				taskH.Fail(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/cancel"):
			if r.Method == http.MethodPost {
				taskH.Cancel(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "tasks") && !hasLastSegment(p, "tasks"):
			if r.Method == http.MethodGet {
				taskH.Get(w, r)
			} else {
				http.NotFound(w, r)
			}
		case hasSegment(p, "tasks"):
			if r.Method == http.MethodPost {
				taskH.Create(w, r)
			} else {
				http.NotFound(w, r)
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

func dispatchMailbox(w http.ResponseWriter, r *http.Request, h *handler.MailboxHandler) {
	p := r.URL.Path
	if hasLastSegment(p, "mailboxes") {
		if r.Method == http.MethodPost {
			h.Create(w, r)
		} else {
			http.NotFound(w, r)
		}
		return
	}
	if r.Method == http.MethodGet {
		h.Get(w, r)
	} else {
		http.NotFound(w, r)
	}
}
