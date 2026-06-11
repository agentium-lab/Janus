package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverURL string
	tenantID  string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "janus",
		Short: "Janus — A2A-native Durable Agent Broker CLI",
	}

	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:8080", "Janus API server URL")
	rootCmd.PersistentFlags().StringVar(&tenantID, "tenant", "default", "Tenant ID")

	rootCmd.AddCommand(agentCmd())
	rootCmd.AddCommand(taskCmd())
	rootCmd.AddCommand(mailboxCmd())
	rootCmd.AddCommand(dashboardCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
