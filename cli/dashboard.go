package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func dashboardCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dashboard",
		Short: "Start local dashboard (not yet implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "Dashboard: not yet implemented. Use 'janus task status' and 'janus task events' for now.")
			return nil
		},
	}
}
