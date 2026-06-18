package main

import (
	"encoding/json"
	"fmt"
	"io"
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
			team, _ := cmd.Flags().GetString("team")
			protocol, _ := cmd.Flags().GetString("protocol")
			if id == "" || name == "" {
				return fmt.Errorf("--id and --name are required")
			}
			c := client()
			if err := c.RegisterAgent(cmd.Context(), janus.RegisterAgentRequest{
				ID: id, DisplayName: name, TeamID: team, Protocol: protocol,
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Agent %s registered\n", id)
			return nil
		},
	}
	registerCmd.Flags().String("id", "", "Agent ID")
	registerCmd.Flags().String("name", "", "Display name")
	registerCmd.Flags().String("team", "", "Team ID")
	registerCmd.Flags().String("protocol", "a2a", "Protocol (a2a, acp, custom-sdk)")

	statusCmd := &cobra.Command{
		Use:   "status [agent-id]",
		Short: "Get agent status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agent, err := client().GetAgent(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), agent)
			return nil
		},
	}

	heartbeatCmd := &cobra.Command{
		Use:   "heartbeat [agent-id]",
		Short: "Send agent heartbeat",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := client().HeartbeatAgent(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Heartbeat sent for %s\n", args[0])
			return nil
		},
	}

	cmd.AddCommand(registerCmd, agentAddCmd(), statusCmd, heartbeatCmd)
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

	replayCmd := &cobra.Command{
		Use:   "replay [task-id]",
		Short: "Replay a terminal task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			task, err := client().ReplayTask(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), task)
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

	cmd.AddCommand(publishCmd, statusCmd, cancelCmd, replayCmd, eventsCmd)
	return cmd
}

func mailboxCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "mailbox", Short: "Mailbox operations"}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a mailbox",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, _ := cmd.Flags().GetString("id")
			agentID, _ := cmd.Flags().GetString("agent")
			maxConcurrency, _ := cmd.Flags().GetInt("max-concurrency")
			ackWaitSeconds, _ := cmd.Flags().GetInt("ack-wait-seconds")
			maxDeliver, _ := cmd.Flags().GetInt("max-deliver")
			retentionSeconds, _ := cmd.Flags().GetInt("retention-seconds")
			if id == "" || agentID == "" {
				return fmt.Errorf("--id and --agent are required")
			}
			resp, err := client().CreateMailboxWithConfig(cmd.Context(), janus.CreateMailboxRequest{
				ID:               id,
				AgentID:          agentID,
				MaxConcurrency:   maxConcurrency,
				ACKWaitSeconds:   ackWaitSeconds,
				MaxDeliver:       maxDeliver,
				RetentionSeconds: retentionSeconds,
			})
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), resp)
			return nil
		},
	}
	createCmd.Flags().String("id", "", "Mailbox ID")
	createCmd.Flags().String("agent", "", "Owning agent ID")
	createCmd.Flags().Int("max-concurrency", 0, "Maximum in-flight tasks")
	createCmd.Flags().Int("ack-wait-seconds", 0, "ACK wait timeout in seconds")
	createCmd.Flags().Int("max-deliver", 0, "Maximum delivery attempts")
	createCmd.Flags().Int("retention-seconds", 0, "Message retention in seconds")

	statusCmd := &cobra.Command{
		Use:   "status [mailbox-id]",
		Short: "Get mailbox status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mailbox, err := client().GetMailbox(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), mailbox)
			return nil
		},
	}

	pauseCmd := &cobra.Command{
		Use:   "pause [mailbox-id]",
		Short: "Pause mailbox delivery",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().PauseMailbox(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), resp)
			return nil
		},
	}

	resumeCmd := &cobra.Command{
		Use:   "resume [mailbox-id]",
		Short: "Resume mailbox delivery",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().ResumeMailbox(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), resp)
			return nil
		},
	}

	pullCmd := &cobra.Command{
		Use:   "pull [mailbox-id]",
		Short: "Pull a task from mailbox",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agentID, _ := cmd.Flags().GetString("agent")
			if agentID == "" {
				return fmt.Errorf("--agent is required")
			}
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
	pullCmd.Flags().String("agent", "", "Agent ID")

	ackCmd := &cobra.Command{
		Use:   "ack [task-id]",
		Short: "Acknowledge task completion",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, _ := cmd.Flags().GetString("lease")
			attempt, _ := cmd.Flags().GetInt("attempt")
			resultRef, _ := cmd.Flags().GetString("result-ref")
			c := client()
			return c.AckTask(cmd.Context(), args[0], janus.AckRequest{
				LeaseID:   lease,
				Attempt:   attempt,
				ResultRef: resultRef,
			})
		},
	}
	ackCmd.Flags().String("lease", "", "Lease ID")
	ackCmd.Flags().Int("attempt", 0, "Task attempt")
	ackCmd.Flags().String("result-ref", "", "Result reference URI")

	nackCmd := &cobra.Command{
		Use:   "nack [task-id]",
		Short: "Negatively acknowledge task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, _ := cmd.Flags().GetString("lease")
			attempt, _ := cmd.Flags().GetInt("attempt")
			retriable, _ := cmd.Flags().GetBool("retriable")
			code, _ := cmd.Flags().GetString("error-code")
			msg, _ := cmd.Flags().GetString("error-message")
			c := client()
			req := janus.NackRequest{LeaseID: lease, Attempt: attempt, Retriable: retriable}
			if code != "" {
				req.Error = &core.TaskError{Code: code, Message: msg}
			}
			return c.NackTask(cmd.Context(), args[0], req)
		},
	}
	nackCmd.Flags().String("lease", "", "Lease ID")
	nackCmd.Flags().Int("attempt", 0, "Task attempt")
	nackCmd.Flags().Bool("retriable", false, "Whether task can be retried")
	nackCmd.Flags().String("error-code", "", "Error code")
	nackCmd.Flags().String("error-message", "", "Error message")

	cmd.AddCommand(createCmd, statusCmd, pauseCmd, resumeCmd, pullCmd, ackCmd, nackCmd)
	return cmd
}

func apiKeyCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "api-key", Short: "API key management"}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create an API key",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			key, err := client().CreateAPIKey(cmd.Context(), janus.CreateAPIKeyRequest{Name: name})
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), key)
			return nil
		},
	}
	createCmd.Flags().String("name", "", "API key name")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List API keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			keys, err := client().ListAPIKeys(cmd.Context())
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), keys)
			return nil
		},
	}

	revokeCmd := &cobra.Command{
		Use:   "revoke [key-id]",
		Short: "Revoke an API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := client().RevokeAPIKey(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), key)
			return nil
		},
	}

	cmd.AddCommand(createCmd, listCmd, revokeCmd)
	return cmd
}

func policyCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Short: "Policy rule template management"}

	cmd.AddCommand(policyCapabilityCmd("allow-agent", "Allow an agent to publish tasks to a capability", core.PolicyTemplateAllowAgentCapability, true))
	cmd.AddCommand(policyCapabilityCmd("deny-agent", "Deny an agent from publishing tasks to a capability", core.PolicyTemplateDenyAgentCapability, true))
	cmd.AddCommand(policyCapabilityCmd("allow-team", "Allow a team to publish tasks to a capability", core.PolicyTemplateAllowTeamCapability, false))
	cmd.AddCommand(policyCapabilityCmd("deny-team", "Deny a team from publishing tasks to a capability", core.PolicyTemplateDenyTeamCapability, false))
	cmd.AddCommand(policyRequireApprovalCmd())
	cmd.AddCommand(policyClassificationCmd("allow-classification", "Allow an agent or team to receive a data classification", true))
	cmd.AddCommand(policyClassificationCmd("deny-classification", "Deny an agent or team from receiving a data classification", false))
	cmd.AddCommand(policyToolCmd("allow-tool", "Allow an agent or team to invoke a tool", true))
	cmd.AddCommand(policyToolCmd("deny-tool", "Deny an agent or team from invoking a tool", false))

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List active policy rules",
		RunE: func(cmd *cobra.Command, args []string) error {
			rules, err := client().ListPolicyRules(cmd.Context())
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), rules)
			return nil
		},
	}
	cmd.AddCommand(listCmd)
	return cmd
}

func policyCapabilityCmd(use, short, template string, agent bool) *cobra.Command {
	req := core.PolicyRuleTemplateRequest{Template: template}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			req.Priority, _ = cmd.Flags().GetInt("priority")
			req.Name, _ = cmd.Flags().GetString("name")
			req.Capability, _ = cmd.Flags().GetString("capability")
			if agent {
				req.AgentID, _ = cmd.Flags().GetString("agent")
				if req.AgentID == "" || req.Capability == "" {
					return fmt.Errorf("--agent and --capability are required")
				}
			} else {
				req.TeamID, _ = cmd.Flags().GetString("team")
				if req.TeamID == "" || req.Capability == "" {
					return fmt.Errorf("--team and --capability are required")
				}
			}
			return createPolicyRuleFromTemplate(cmd, req)
		},
	}
	if agent {
		cmd.Flags().String("agent", "", "Agent ID")
	} else {
		cmd.Flags().String("team", "", "Team ID")
	}
	cmd.Flags().String("capability", "", "Capability name")
	addPolicyTemplateCommonFlags(cmd)
	return cmd
}

func policyRequireApprovalCmd() *cobra.Command {
	req := core.PolicyRuleTemplateRequest{}
	cmd := &cobra.Command{
		Use:   "require-approval",
		Short: "Require approval for a capability or tool",
		RunE: func(cmd *cobra.Command, args []string) error {
			req.Priority, _ = cmd.Flags().GetInt("priority")
			req.Name, _ = cmd.Flags().GetString("name")
			req.Capability, _ = cmd.Flags().GetString("capability")
			req.Tool, _ = cmd.Flags().GetString("tool")
			if req.Capability == "" && req.Tool == "" {
				return fmt.Errorf("--capability or --tool is required")
			}
			if req.Capability != "" && req.Tool != "" {
				return fmt.Errorf("--capability and --tool are mutually exclusive")
			}
			if req.Tool != "" {
				req.Template = core.PolicyTemplateRequireApprovalTool
			} else {
				req.Template = core.PolicyTemplateRequireApprovalCapability
			}
			return createPolicyRuleFromTemplate(cmd, req)
		},
	}
	cmd.Flags().String("capability", "", "Capability name")
	cmd.Flags().String("tool", "", "Tool name")
	addPolicyTemplateCommonFlags(cmd)
	return cmd
}

func policyClassificationCmd(use, short string, allow bool) *cobra.Command {
	req := core.PolicyRuleTemplateRequest{}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			req.Priority, _ = cmd.Flags().GetInt("priority")
			req.Name, _ = cmd.Flags().GetString("name")
			req.AgentID, _ = cmd.Flags().GetString("agent")
			req.TeamID, _ = cmd.Flags().GetString("team")
			req.DataClassification, _ = cmd.Flags().GetString("classification")
			if err := validatePolicySubject(req.AgentID, req.TeamID); err != nil {
				return err
			}
			if req.DataClassification == "" {
				return fmt.Errorf("--classification is required")
			}
			switch {
			case allow && req.AgentID != "":
				req.Template = core.PolicyTemplateAllowAgentDataClassification
			case allow && req.TeamID != "":
				req.Template = core.PolicyTemplateAllowTeamDataClassification
			case !allow && req.AgentID != "":
				req.Template = core.PolicyTemplateDenyAgentDataClassification
			default:
				req.Template = core.PolicyTemplateDenyTeamDataClassification
			}
			return createPolicyRuleFromTemplate(cmd, req)
		},
	}
	cmd.Flags().String("agent", "", "Agent ID")
	cmd.Flags().String("team", "", "Team ID")
	cmd.Flags().String("classification", "", "Data classification")
	addPolicyTemplateCommonFlags(cmd)
	return cmd
}

func policyToolCmd(use, short string, allow bool) *cobra.Command {
	req := core.PolicyRuleTemplateRequest{}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			req.Priority, _ = cmd.Flags().GetInt("priority")
			req.Name, _ = cmd.Flags().GetString("name")
			req.AgentID, _ = cmd.Flags().GetString("agent")
			req.TeamID, _ = cmd.Flags().GetString("team")
			req.Tool, _ = cmd.Flags().GetString("tool")
			if err := validatePolicySubject(req.AgentID, req.TeamID); err != nil {
				return err
			}
			if req.Tool == "" {
				return fmt.Errorf("--tool is required")
			}
			switch {
			case allow && req.AgentID != "":
				req.Template = core.PolicyTemplateAllowAgentTool
			case allow && req.TeamID != "":
				req.Template = core.PolicyTemplateAllowTeamTool
			case !allow && req.AgentID != "":
				req.Template = core.PolicyTemplateDenyAgentTool
			default:
				req.Template = core.PolicyTemplateDenyTeamTool
			}
			return createPolicyRuleFromTemplate(cmd, req)
		},
	}
	cmd.Flags().String("agent", "", "Agent ID")
	cmd.Flags().String("team", "", "Team ID")
	cmd.Flags().String("tool", "", "Tool name")
	addPolicyTemplateCommonFlags(cmd)
	return cmd
}

func addPolicyTemplateCommonFlags(cmd *cobra.Command) {
	cmd.Flags().String("name", "", "Override generated policy rule name")
	cmd.Flags().Int("priority", 0, "Policy priority; lower number wins")
}

func validatePolicySubject(agentID, teamID string) error {
	if agentID == "" && teamID == "" {
		return fmt.Errorf("--agent or --team is required")
	}
	if agentID != "" && teamID != "" {
		return fmt.Errorf("--agent and --team are mutually exclusive")
	}
	return nil
}

func createPolicyRuleFromTemplate(cmd *cobra.Command, req core.PolicyRuleTemplateRequest) error {
	rule, err := client().CreatePolicyRuleFromTemplate(cmd.Context(), req)
	if err != nil {
		return err
	}
	printJSON(cmd.OutOrStdout(), rule)
	return nil
}

func dlqCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "dlq", Short: "Dead letter queue operations"}

	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "Query dead-lettered tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			mailboxID, _ := cmd.Flags().GetString("mailbox")
			limit, _ := cmd.Flags().GetInt("limit")
			tasks, err := client().QueryDLQ(cmd.Context(), janus.DLQQueryOptions{
				MailboxID: mailboxID,
				Limit:     limit,
			})
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), tasks)
			return nil
		},
	}
	queryCmd.Flags().String("mailbox", "", "Filter by mailbox ID")
	queryCmd.Flags().Int("limit", 50, "Maximum tasks to return")

	replayCmd := &cobra.Command{
		Use:   "replay [task-id]",
		Short: "Replay a dead-lettered task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			task, err := client().ReplayDLQ(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), task)
			return nil
		},
	}

	discardCmd := &cobra.Command{
		Use:   "discard [task-id]",
		Short: "Discard a dead-lettered task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := client().DiscardDLQ(cmd.Context(), args[0]); err != nil {
				return err
			}
			printJSON(cmd.OutOrStdout(), map[string]string{"id": args[0], "status": "discarded"})
			return nil
		},
	}

	cmd.AddCommand(queryCmd, replayCmd, discardCmd)
	return cmd
}

func client() *janus.Client {
	return janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: tenantID, APIKey: apiKey})
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
