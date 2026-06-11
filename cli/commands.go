package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
)

func agentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent", Short: "Agent management"}

	registerCmd := &cobra.Command{
		Use:   "register",
		Short: "Register a new agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			name, _ := cmd.Flags().GetString("name")
			protocol, _ := cmd.Flags().GetString("protocol")
			if id == "" || name == "" {
				return fmt.Errorf("--id and --name are required")
			}
			c := client()
			if err := c.RegisterAgent(cmd.Context(), janus.RegisterAgentRequest{
				ID: id, DisplayName: name, Protocol: protocol,
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Agent %s registered\n", id)
			return nil
		},
	}
	registerCmd.Flags().String("id", "", "Agent ID")
	registerCmd.Flags().String("name", "", "Display name")
	registerCmd.Flags().String("protocol", "a2a", "Protocol (a2a, acp, custom-sdk)")

	statusCmd := &cobra.Command{
		Use:   "status [agent-id]",
		Short: "Get agent status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintf(cmd.OutOrStdout(), "Agent status for %s: not yet implemented\n", args[0])
			return nil
		},
	}

	heartbeatCmd := &cobra.Command{
		Use:   "heartbeat [agent-id]",
		Short: "Send agent heartbeat",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := serverURL + "/v1/tenants/" + tenantID + "/agents/" + args[0] + "/heartbeat"
			resp, err := http.Post(url, "application/json", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			fmt.Fprintf(cmd.OutOrStdout(), "Heartbeat sent for %s (status: %d)\n", args[0], resp.StatusCode)
			return nil
		},
	}

	cmd.AddCommand(registerCmd, statusCmd, heartbeatCmd)
	return cmd
}

func taskCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "task", Short: "Task operations"}

	publishCmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish a new task",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			source, _ := cmd.Flags().GetString("source")
			targetType, _ := cmd.Flags().GetString("target-type")
			targetValue, _ := cmd.Flags().GetString("target-value")
			mailbox, _ := cmd.Flags().GetString("mailbox")
			priority, _ := cmd.Flags().GetString("priority")
			payloadFile, _ := cmd.Flags().GetString("payload-file")

			if id == "" || source == "" || targetType == "" || targetValue == "" {
				return fmt.Errorf("--id, --source, --target-type, --target-value are required")
			}

			var content string
			if payloadFile != "" {
				data, err := os.ReadFile(payloadFile)
				if err != nil {
					return fmt.Errorf("read payload: %w", err)
				}
				content = string(data)
			}

			c := client()
			resp, err := c.PublishTask(cmd.Context(), janus.PublishTaskRequest{
				ID: id, SourceAgent: source, TargetType: targetType, TargetValue: targetValue,
				MailboxID: mailbox, Priority: priority,
				Envelope: envelope(id, source, targetType, targetValue, content),
			})
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), resp)
			return nil
		},
	}
	publishCmd.Flags().String("id", "", "Task ID")
	publishCmd.Flags().String("source", "", "Source agent ID")
	publishCmd.Flags().String("target-type", "agent", "Target type")
	publishCmd.Flags().String("target-value", "", "Target value")
	publishCmd.Flags().String("mailbox", "", "Mailbox ID")
	publishCmd.Flags().String("priority", "normal", "Priority (low, normal, high, critical)")
	publishCmd.Flags().String("payload-file", "", "Path to payload JSON file")

	statusCmd := &cobra.Command{
		Use:   "status [task-id]",
		Short: "Get task status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client()
			task, err := c.GetTask(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), task)
			return nil
		},
	}

	cancelCmd := &cobra.Command{
		Use:   "cancel [task-id]",
		Short: "Cancel a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client()
			if err := c.CancelTask(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Task %s cancelled\n", args[0])
			return nil
		},
	}

	eventsCmd := &cobra.Command{
		Use:   "events [task-id]",
		Short: "Get task audit events",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client()
			events, err := c.GetTaskEvents(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), events)
			return nil
		},
	}

	cmd.AddCommand(publishCmd, statusCmd, cancelCmd, eventsCmd)
	return cmd
}

func mailboxCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "mailbox", Short: "Mailbox operations"}

	pullCmd := &cobra.Command{
		Use:   "pull [mailbox-id]",
		Short: "Pull a task from mailbox",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agentID, _ := cmd.Flags().GetString("agent")
			c := client()
			result, err := c.PullTask(cmd.Context(), args[0], agentID)
			if err != nil {
				return err
			}
			if result == nil {
				fmt.Fprintln(cmd.OutOrStdout(), "No messages available")
				return nil
			}
			printJSON(cmd.OutOrStdout(), result)
			return nil
		},
	}
	pullCmd.Flags().String("agent", "default", "Agent ID")

	ackCmd := &cobra.Command{
		Use:   "ack [task-id]",
		Short: "Acknowledge task completion",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, _ := cmd.Flags().GetString("lease")
			resultRef, _ := cmd.Flags().GetString("result-ref")
			c := client()
			return c.AckTask(cmd.Context(), args[0], janus.AckRequest{
				LeaseID:   lease,
				ResultRef: resultRef,
			})
		},
	}
	ackCmd.Flags().String("lease", "", "Lease ID")
	ackCmd.Flags().String("result-ref", "", "Result reference URI")

	nackCmd := &cobra.Command{
		Use:   "nack [task-id]",
		Short: "Negatively acknowledge task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, _ := cmd.Flags().GetString("lease")
			retriable, _ := cmd.Flags().GetBool("retriable")
			code, _ := cmd.Flags().GetString("error-code")
			msg, _ := cmd.Flags().GetString("error-message")
			c := client()
			req := janus.NackRequest{LeaseID: lease, Retriable: retriable}
			if code != "" {
				req.Error = &core.TaskError{Code: code, Message: msg}
			}
			return c.NackTask(cmd.Context(), args[0], req)
		},
	}
	nackCmd.Flags().String("lease", "", "Lease ID")
	nackCmd.Flags().Bool("retriable", false, "Whether task can be retried")
	nackCmd.Flags().String("error-code", "", "Error code")
	nackCmd.Flags().String("error-message", "", "Error message")

	cmd.AddCommand(pullCmd, ackCmd, nackCmd)
	return cmd
}

func client() *janus.Client {
	return janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: tenantID})
}

func envelope(taskID, source, targetType, targetValue, payload string) core.TaskEnvelope {
	return core.TaskEnvelope{
		JanusVersion: "0.1",
		TaskID:       taskID,
		TenantID:     tenantID,
		SourceAgent:  source,
		Target:       core.Target{Type: core.TargetType(targetType), Value: targetValue},
		Payload:      core.Payload{Type: "json", Content: payload},
		Trace:        core.TraceContext{TraceID: "cli-" + taskID},
	}
}

func printJSON(w io.Writer, v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Fprintln(w, string(data))
}
