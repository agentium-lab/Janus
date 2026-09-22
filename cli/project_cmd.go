package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func projectCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "project", Short: "Project configuration operations"}
	cmd.AddCommand(projectInitCmd(), projectValidateCmd(), projectDiffCmd(), projectApplyCmd(), projectSyncCmd())
	return cmd
}

func projectInitCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := resolveProjectPath(true)
			if err != nil {
				return err
			}
			if _, err := os.Stat(path); err == nil && !force {
				return fmt.Errorf("%s already exists; use --force to overwrite", path)
			}
			cfg := emptyProjectConfig()
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Created %s\n", path)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing project file")
	return cmd
}

func projectValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s is valid\n", path)
			return nil
		},
	}
}

func projectDiffCmd() *cobra.Command {
	var allTenants bool
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Show project changes against Janus API",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			for _, tenant := range tenants {
				if err := diffProjectTenant(cmd, cfg, tenant); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Diff every tenant in the project file")
	return cmd
}

func projectApplyCmd() *cobra.Command {
	var allTenants bool
	var continueOnError bool
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply janus.project.yaml to Janus API",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			var firstErr error
			for _, tenant := range tenants {
				if err := applyProjectTenant(cmd, cfg, tenant); err != nil {
					if !continueOnError {
						return err
					}
					fmt.Fprintf(cmd.ErrOrStderr(), "tenant %s failed: %v\n", tenant, err)
					if firstErr == nil {
						firstErr = err
					}
				}
			}
			return firstErr
		},
	}
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Apply every tenant in the project file")
	cmd.Flags().BoolVar(&continueOnError, "continue-on-error", false, "Continue applying remaining tenants after an error")
	return cmd
}

func projectSyncCmd() *cobra.Command {
	var overwrite bool
	var allTenants bool
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Merge current Janus API resources into janus.project.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, allTenants)
			if err != nil {
				return err
			}
			for _, tenant := range tenants {
				if err := syncProjectTenant(cmd.Context(), cfg, tenant, overwrite); err != nil {
					return err
				}
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Synced %s\n", path)
			return nil
		},
	}
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing local tenant resource entries")
	cmd.Flags().BoolVar(&allTenants, "all-tenants", false, "Sync every tenant in the project file")
	return cmd
}

func tenantCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tenant", Short: "Tenant project operations"}
	cmd.AddCommand(tenantAddCmd())
	return cmd
}

func tenantAddCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "add [tenant-id]",
		Short: "Create a tenant and persist it to janus.project.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := strings.TrimSpace(args[0])
			if id == "" {
				return fmt.Errorf("tenant id is required")
			}
			if name == "" {
				name = id
			}
			cfg, path, err := loadProjectConfig(true)
			if err != nil {
				return err
			}
			cfg.normalize()
			if _, ok := cfg.Tenants[id]; ok {
				return fmt.Errorf("tenant %s already exists in %s", id, path)
			}
			if err := client().CreateTenant(cmd.Context(), id, name); err != nil {
				return err
			}
			cfg.Tenants[id] = &ProjectTenant{Name: name}
			if cfg.DefaultTenant == "" {
				cfg.DefaultTenant = id
			}
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Tenant %s added and saved to %s\n", id, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Tenant display name")
	return cmd
}

func agentAddCmd() *cobra.Command {
	var name, team, protocol, endpoint, description, mailboxID string
	var capabilities []string
	var classifications []string
	var concurrency, rpm, tpm int
	cmd := &cobra.Command{
		Use:   "add [agent-id]",
		Short: "Register an agent and persist it to janus.project.yaml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agentID := strings.TrimSpace(args[0])
			if agentID == "" {
				return fmt.Errorf("agent id is required")
			}
			if len(capabilities) == 0 {
				return fmt.Errorf("--capability is required")
			}
			cfg, path, err := loadProjectConfig(false)
			if err != nil {
				return err
			}
			if err := validateProjectConfig(cfg); err != nil {
				return err
			}
			tenants, err := selectedProjectTenants(cmd, cfg, false)
			if err != nil {
				return err
			}
			tenant := tenants[0]
			pt := cfg.Tenants[tenant]
			if pt.Agents == nil {
				pt.Agents = make(map[string]*ProjectAgent)
			}
			if _, ok := pt.Agents[agentID]; ok {
				return fmt.Errorf("agent %s already exists in tenant %s", agentID, tenant)
			}
			caps := make([]ProjectCapability, 0, len(capabilities))
			for _, cap := range capabilities {
				caps = append(caps, ProjectCapability{
					ID:                  strings.TrimSpace(cap),
					DataClassifications: cleanStringSlice(classifications),
				})
			}
			agent := &ProjectAgent{
				Name:         name,
				Team:         team,
				Protocol:     protocol,
				Endpoint:     endpoint,
				Description:  description,
				Capabilities: caps,
				Concurrency:  concurrency,
				RPM:          rpm,
				TPM:          tpm,
			}
			if mailboxID != "" {
				agent.Mailbox = &ProjectMailbox{ID: mailboxID}
			}
			c := projectClient(tenant)
			req, err := registerAgentRequest(agentID, *agent, cfg.Defaults)
			if err != nil {
				return err
			}
			if err := c.RegisterAgent(cmd.Context(), req); err != nil {
				return err
			}
			if _, err := c.CreateMailboxWithConfig(cmd.Context(), mailboxRequest(agentID, *agent, cfg.Defaults)); err != nil {
				return err
			}
			pt.Agents[agentID] = agent
			if err := saveProjectConfig(path, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Agent %s added to tenant %s and saved to %s\n", agentID, tenant, path)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Agent display name")
	cmd.Flags().StringVar(&team, "team", "", "Team ID")
	cmd.Flags().StringVar(&protocol, "protocol", "", "Protocol (defaults to project defaults or custom-sdk)")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "Agent endpoint")
	cmd.Flags().StringVar(&description, "description", "", "Agent description")
	cmd.Flags().StringArrayVar(&capabilities, "capability", nil, "Capability name; repeat for multiple capabilities")
	cmd.Flags().StringArrayVar(&classifications, "classification", nil, "Allowed data classification for each capability; repeat for multiple values")
	cmd.Flags().IntVar(&concurrency, "concurrency", 0, "Agent and default mailbox max concurrency")
	cmd.Flags().IntVar(&rpm, "rpm", 0, "Agent RPM metadata")
	cmd.Flags().IntVar(&tpm, "tpm", 0, "Agent TPM metadata")
	cmd.Flags().StringVar(&mailboxID, "mailbox", "", "Mailbox ID; defaults to <agent-id>.default")
	return cmd
}

func resolveProjectPath(create bool) (string, error) {
	if projectFile != "" {
		return filepath.Abs(projectFile)
	}
	if env := strings.TrimSpace(os.Getenv("JANUS_PROJECT_FILE")); env != "" {
		return filepath.Abs(env)
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if found, ok := findProjectFile(wd); ok {
		return found, nil
	}
	if create {
		return filepath.Join(wd, defaultProjectFileName), nil
	}
	return "", fmt.Errorf("%s not found; run `janus project init` or `janus tenant add` first", defaultProjectFileName)
}

func findProjectFile(start string) (string, bool) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, defaultProjectFileName)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func loadProjectConfig(create bool) (*ProjectConfig, string, error) {
	path, err := resolveProjectPath(create)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && create {
			return emptyProjectConfig(), path, nil
		}
		return nil, "", err
	}
	cfg := emptyProjectConfig()
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, "", fmt.Errorf("parse %s: %w", path, err)
		}
	}
	cfg.normalize()
	return cfg, path, nil
}

func saveProjectConfig(path string, cfg *ProjectConfig) error {
	cfg.normalize()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal project config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func selectedProjectTenants(cmd *cobra.Command, cfg *ProjectConfig, all bool) ([]string, error) {
	cfg.normalize()
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("project has no tenants")
	}
	if all {
		names := sortedTenantIDs(cfg)
		return names, nil
	}
	if tenantFlagChanged(cmd) {
		if _, ok := cfg.Tenants[tenantID]; !ok {
			return nil, fmt.Errorf("tenant %s is not declared in project", tenantID)
		}
		return []string{tenantID}, nil
	}
	if cfg.DefaultTenant != "" {
		return []string{cfg.DefaultTenant}, nil
	}
	if len(cfg.Tenants) == 1 {
		for tenant := range cfg.Tenants {
			return []string{tenant}, nil
		}
	}
	if tenantID != "" && tenantID != "default" {
		if _, ok := cfg.Tenants[tenantID]; ok {
			return []string{tenantID}, nil
		}
	}
	return nil, fmt.Errorf("multiple tenants declared; pass --tenant or set default_tenant")
}

func isAPIStatus(err error, status int) bool {
	if err == nil {
		return false
	}
	var apiErr *janus.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == status {
		return true
	}
	return false
}

var projectFile string

func tenantFlagChanged(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	if f := cmd.Flag("tenant"); f != nil && f.Changed {
		return true
	}
	return false
}

func sortedTenantIDs(cfg *ProjectConfig) []string {
	names := make([]string, 0, len(cfg.Tenants))
	for tenant := range cfg.Tenants {
		names = append(names, tenant)
	}
	sort.Strings(names)
	return names
}

func projectClient(projectTenant string) *janus.Client {
	return janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: projectTenant, APIKey: apiKey})
}

func registerAgentRequest(agentID string, agent ProjectAgent, defaults ProjectDefaults) (janus.RegisterAgentRequest, error) {
	displayName := strings.TrimSpace(agent.Name)
	if displayName == "" {
		displayName = agentID
	}
	protocol := strings.TrimSpace(agent.Protocol)
	if protocol == "" {
		protocol = strings.TrimSpace(defaults.Protocol)
	}
	if protocol == "" {
		protocol = "custom-sdk"
	}
	req := janus.RegisterAgentRequest{
		ID:             agentID,
		DisplayName:    displayName,
		TeamID:         strings.TrimSpace(agent.Team),
		Protocol:       protocol,
		Endpoint:       strings.TrimSpace(agent.Endpoint),
		Description:    strings.TrimSpace(agent.Description),
		MaxConcurrency: agentConcurrency(agent, defaults),
		RPM:            agent.RPM,
		TPM:            agent.TPM,
	}
	for _, capability := range agent.Capabilities {
		schema, err := capabilitySchemaJSON(capability)
		if err != nil {
			return janus.RegisterAgentRequest{}, err
		}
		req.Capabilities = append(req.Capabilities, janus.RegisterAgentCapability{
			Capability:  strings.TrimSpace(capability.ID),
			Description: strings.TrimSpace(capability.Description),
			Schema:      schema,
		})
	}
	return req, nil
}

func capabilitySchemaJSON(capability ProjectCapability) (string, error) {
	if len(capability.DataClassifications) == 0 {
		return "", nil
	}
	schema := map[string][]string{
		"allowed_data_classifications": cleanStringSlice(capability.DataClassifications),
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
