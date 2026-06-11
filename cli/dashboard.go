package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"

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

			mux := http.NewServeMux()
			mux.Handle("/ws", proxy)
			mux.Handle("/", fileServer)

			addr := ":" + port
			fmt.Fprintf(cmd.OutOrStdout(), "Dashboard: http://localhost%s\n", addr)
			fmt.Fprintf(cmd.OutOrStdout(), "API server: %s\n", serverURL)
			return http.ListenAndServe(addr, mux)
		},
	}
	cmd.Flags().String("port", "8090", "Dashboard listen port")
	return cmd
}
