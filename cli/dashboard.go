package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

//go:embed dashboard
var dashboardFS embed.FS

func dashboardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Start local dashboard",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, _ := cmd.Flags().GetString("port")
			if port == "" {
				port = "8090"
			}

			apiURL, _ := url.Parse(serverURL)
			staticContent, _ := fs.Sub(dashboardFS, "dashboard")
			fileServer := http.FileServer(http.FS(staticContent))
			proxy := httputil.NewSingleHostReverseProxy(apiURL)
			proxy.Transport = &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ResponseHeaderTimeout: 30 * time.Second,
				IdleConnTimeout:       90 * time.Second,
			}

			mux := http.NewServeMux()
			mux.Handle("/ws", proxy)
			mux.Handle("/", fileServer)

			addr := ":" + port
			fmt.Fprintf(cmd.OutOrStdout(), "Dashboard: http://localhost%s\n", addr)
			fmt.Fprintf(cmd.OutOrStdout(), "API server: %s\n", serverURL)
			srv := &http.Server{
				Addr:              addr,
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
				ReadTimeout:       30 * time.Second,
				WriteTimeout:      30 * time.Second,
				IdleTimeout:       120 * time.Second,
			}
			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
			}()
			return srv.ListenAndServe()
		},
	}
	cmd.Flags().String("port", "8090", "Dashboard listen port")
	return cmd
}
