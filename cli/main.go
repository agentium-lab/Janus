package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverURL string
	tenantID  string
	apiKey	  string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "janus",
		Short: "Janus — A2A-native Durable Agent Broker CLI",
	}

	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:8080", "Janus API server URL")
	rootCmd.PersistentFlags().StringVar(&tenantID, "tenant", "default", "Tenant ID")
	rootCmd.PersistentFlags().StringVar(&apiKey, "api-key", os.Getenv("JANUS_API_KEY"), "Janus API Key")
	rootCmd.PersistentFlags().StringVar(&projectFile, "file", "", "Janus project config file")

	rootCmd.AddCommand(agentCmd())
	rootCmd.AddCommand(taskCmd())
	rootCmd.AddCommand(mailboxCmd())
	rootCmd.AddCommand(dlqCmd())
	rootCmd.AddCommand(apiKeyCmd())
	rootCmd.AddCommand(policyCmd())
	rootCmd.AddCommand(projectCmd())
	rootCmd.AddCommand(tenantCmd())
	rootCmd.AddCommand(projectValidateCmd())
	rootCmd.AddCommand(projectDiffCmd())
	rootCmd.AddCommand(projectApplyCmd())
	rootCmd.AddCommand(dashboardCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
